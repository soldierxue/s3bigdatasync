## Read large number of Azure Blob Storage Inventory CSV files and Construct Format Message to SQS

import csv
import codecs
import boto3
import json
import logging
import time

import pandas as pd
import numpy as np
from dask import dataframe as df1

import glob
import os
from pathlib import PurePosixPath, Path
import json
from datetime import datetime, timezone

logger = logging.getLogger()

is_local_debug = False # 是否是本地调试
is_limit_debug = True # 是否通过MAX_INVENTORY_NUM，MAX_OBJ_TOTAL_NUM 或 MAX_OBJ_TOTAL_SIZE 三者来控制提交消息的数量
region = "us-east-2"
sqs_queue = "https://sqs.us-east-2.amazonaws.com/188869792837/recode_msg_test"

# global variables
rootFolder="/home/ec2-user/environment/s3/sampleInventory" # which root folder has the inventory csv files
MAX_INVENTORY_NUM = 1 # the max number of inventory files will be processed
MAX_OBJ_TOTAL_NUM = 51 # the max number of objects will be processed
MAX_OBJ_TOTAL_SIZE = 1024 # GB, the max accumulated obj size will be processed
CHUNK_ROWS_NUM = 25 # 处理清单时，批量处理的对象行数

# Initial sqs client 
if is_local_debug:  # 运行在本地电脑，读取~/.aws 中的本地密钥
    src_session = boto3.session.Session(profile_name='ww')
    sqs = src_session.client('sqs', region_name=region)
else:  # 运行在EC2上，直接获取服务器带的 IAM Role
    sqs = boto3.client('sqs', region)

# Configure logging
def set_log(LoggingLevel, this_file_name):
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    # File logging
    log_path = Path(__file__).parent / 'amazon-s3-migration-log'
    if not Path.exists(log_path):
        Path.mkdir(log_path)
    start_time = datetime.now().isoformat().replace(':', '-')[:19]
    _log_file_name = str(log_path / f'{this_file_name}-{start_time}.log')
    print('Log file:', _log_file_name)
    fileHandler = logging.FileHandler(filename=_log_file_name)
    fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(fileHandler)
    return logger, _log_file_name

# 在一个目录递归查找所有子目录
# 获取所有想要的后缀文件比如 "/*-manifest.json" 或"/*.csv"
def retriveFiles(rootFolder,manifestFiles,pattern):
    folders = os.listdir(rootFolder)
    manifests = _findFiles(rootFolder,pattern)
    if len(manifests) > 0:
        manifestFiles.append(manifests)
        
    for d in os.listdir(rootFolder):
        fullDir = os.path.join(rootFolder,d)
        if os.path.isdir(fullDir):
            retriveFiles(fullDir,manifestFiles,pattern)

def _findFiles(root,pattern):
     manifests = glob.glob(root+pattern)
    #  print(root)
     manifestFiles = []
     if len(manifests) > 0:
        for mf in manifests:
            fullMf= os.path.join(rootFolder,mf)
            manifestFiles.append(fullMf)
            # print(fullMf) 
     return manifestFiles

def pdDaskReadCSV(filePath):
    # time taken to read data
    s_time = time.time()
    # readerType = "Pandas Chunk Reader"
    readerType = "Dask Reader"
    
    # chunk = pd.read_csv(filePath, chunksize=1000)
    # df = pd.concat(chunk)
    # df.sample(10)
    dask_df = df1.read_csv(filePath, usecols=['Name', 'Content-Length'],dtype={'Content-MD5': 'object'})
    print(dask_df.info())
    totalCount=0
    # print("Total Obj:"+dask_df.Name.compute())
    for i, j in dask_df.iterrows():
        totalCount = totalCount + 1
#         "schemaFields" : 
# 		[
# 			"Name",
# 			"Creation-Time",
# 			"Last-Modified",
# 			"Etag",
# 			"Content-Length",
# 			"Content-MD5",
# 			"BlobType",
# 			"AccessTier",
# 			"ArchiveStatus"
# 		]
        _name = j['Name']
        _length = j['Content-Length']
        # if i > 5:
        #     break
    
    e_time = time.time() 
    print("Read using"+readerType+ ": ", (e_time-s_time), "seconds,with "+str(i+1)+" records") 
        
def pdChunkRead(filePath,azureEndpoint):
   # time taken to read data
    s_time = time.time()
    readerType = "Pandas Chunk Reader"
    # rowsEachTime = 10000
    pdReader = pd.read_csv(filePath, usecols=['Name', 'Content-Length'],iterator=True, chunksize=CHUNK_ROWS_NUM)
    
    totalCount=0
    totalSize=0
    batchJobs = [] # SQS support 10 in batch messages
    for i, chunk in enumerate(pdReader):
       print(f"Enter Chunk {i}")
       for ri, row in chunk.iterrows():
          
          _name = row['Name']
          _length = row['Content-Length']
          # begin to construct batch messages
          # SendMessage
          job = _constructMsg(_name,_length,filePath,azureEndpoint)
          batchJobs.append({
                "Id": str(totalCount),
                "MessageBody": job,
          })
          
          if len(batchJobs) == 10: # 刚好10个，或者虽然不足10但已经到了该Chunk最后一个
            try:
                # print(f"Begin Send SQS Messages {batchJobs}")
                sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
                batchJobs = []
            except Exception as e:
                print(f'Fail to send sqs message: {str(batchJobs)}, {str(e)}')              
          # end 
          totalCount = totalCount + 1
          totalSize = totalSize + _length
          ## 严格控制处理的总数据量
          if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
              break
       
       # 处理 上一个 Chunk 尾部不足10个的消息
       # SendMessage
       if len(batchJobs) > 0: 
            try:
                # print(f"Begin Send SQS Messages {batchJobs}")
                sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
                batchJobs = []
            except Exception as e:
                print(f'Fail to send sqs message: {str(batchJobs)}, {str(e)}')           
       ## 严格控制处理的总数据量
       if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
           break
    
    e_time = time.time() 
    print(f"Read using {readerType} : {(e_time-s_time)} seconds,with {str(totalCount)} records and {totalSize} Bytes ")
    return {"TotalObjNum":totalCount,"TotalObjSize":totalSize}

# 构建同步工具需要的消息格式
def _constructMsg(objfile,size,inventoryCSVFile,endpoint):
    # job = {"file":objfile,"size":size}
    # get current datetime
    today = datetime.now(timezone.utc)
    # Get current ISO 8601 datetime in string format with UTC timezone
    iso_date = today.isoformat().replace("+00:00", "Z")
    msgBody = '''
            {
              "topic": "/aws/sqs'''+inventoryCSVFile+'''"
              "subject": "/blobServices/default/containers/'''+objfile+'''",
              "eventType": "Microsoft.Storage.BlobCreated",
              "id": "N/A",
              "data": {
                "api": "PutBlob",
                "clientRequestId": "N/A",
                "requestId": "N/A",
                "eTag": "N/A",
                "contentType": "N/A",
                "contentLength":'''+str(size)+''',
                "blobType": "BlockBlob",
                "url": "'''+endpoint+'''/'''+objfile+'''",
                "sequencer": "N/A",
                "storageDiagnostics": { "batchId": "N/A" }
              },
              "dataVersion": "",
              "metadataVersion": "1",
              "eventTime": "'''+iso_date+'''"
            }    
    '''
    return msgBody

def parseManifest(manifestJsonPath,inventoryInfo):
    try:
        with open(manifestJsonPath,"r") as dataFile:
            mf = json.load(dataFile)
            files = mf['files']
            total = len(files)
            endpoint = mf['endpoint']
            inventoryInfo['manifest'] = manifestJsonPath
            inventoryInfo['azure_sa_endpoint'] = endpoint
            inventoryInfo['inventory_files_num'] = total

    except FileNotFoundError:
        print("File Not Found!")

    
# cep1prod1abcdw_2.csv 13G
# cep1prod1abcdw_283.csv 2G, 
## Read usingDask Reader:  359.83536529541016 seconds,with 157826 records
## Read usingPandas Chunk Reader:  355.2787973880768 seconds,with 3650843009 records
# cep1prod1abcdw_4.csv 4.9G
# cep1prod1abcdw_6.csv 39G


# get all maifest json files
manifestFiles = []
retriveFiles(rootFolder,manifestFiles,"/*-manifest.json")
for f in manifestFiles:
    fobj = f[0]
    inventoryInfo = {'inventory_files_num':0}
    
    # From Manifest to get Azure Blob Storage Endpoint
    parseManifest(fobj,inventoryInfo)
    
    numberOfInventoryFiles = inventoryInfo['inventory_files_num']
    endpoint = inventoryInfo['azure_sa_endpoint']
    manifestFilePath = inventoryInfo['manifest']
    if numberOfInventoryFiles > 0:
        pathOfMainiffest = manifestFilePath[:manifestFilePath.rindex("/")]
        print(f"Begin to process its Inventory files of manifest {manifestFilePath}")
        inventoryCSVFiles = []
        retriveFiles(pathOfMainiffest,inventoryCSVFiles,"/*.csv")

        i = 0
        for f in inventoryCSVFiles:
            i = i + 1        
            print(f"Begin to processing file '{f[0]}'")
            pdChunkRead(f[0],endpoint)
            
            if is_limit_debug and i >= MAX_INVENTORY_NUM:
                break  
        print(f"End to process its Inventory files of manifest {manifestFilePath}")
    if is_limit_debug and i >= MAX_INVENTORY_NUM:
                break        



