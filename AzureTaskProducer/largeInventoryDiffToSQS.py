## Read large number of Azure Blob Storage Inventory differece CSV files and Construct Format Message to SQS

import boto3
import json
import time
from pathlib import PurePosixPath, Path
# import os
import pandas as pd
import json
import sys
from datetime import datetime, timezone
import concurrent.futures
import threading
from inventory_lib import setLog, constructSQSMsg, AzureBlobEventType,retriveFiles,setAWSServices, \
     checkMsgIdsFromDDB,ddbBatchSaveMsgLogs

is_local_debug = False # 是否是本地调试
is_limit_debug = False # 是否通过MAX_INVENTORY_NUM，MAX_OBJ_TOTAL_NUM 或 MAX_OBJ_TOTAL_SIZE 三者来控制提交消息的数量
region = "us-east-2"
sqs_queue = "https://sqs.us-east-2.amazonaws.com/188869792837/recode_msg_test"
# ddb_table = "inventoryMsgTable"

# global variables
inventoryDiffFile="/home/ec2-user/environment/s3/diffInventory/cep1prod1abcdw-cep1prod1abcdw-20230718-20230724-add.csv" # which root folder has the inventory diff csv files
MAX_INVENTORY_NUM = 1 # the max number of inventory files will be processed
MAX_OBJ_TOTAL_NUM = 8 # the max number of objects will be processed
MAX_OBJ_TOTAL_SIZE = 1024 # GB, the max accumulated obj size will be processed
CHUNK_ROWS_NUM = 10000 # 处理清单时，批量处理的对象行数

# #         "schemaFields" : 
# # 		[
# #.          "Storage-Account",
# # 			"Name",
# # 			"Creation-Time",
# # 			"Last-Modified",
# # 			"Etag",
# # 			"Content-Length",
# # 			"Variance" New | Update | Delete
# # 		]        

## 将大文件按照 Chunksize（行数） 拆成小文件
def pdChunkSplit(filePath):
    pf = Path(filePath)
    pathOfLargeFile = pf.parent
    nameOfLargeFile = pf.name
    # pathOfLargeFile, nameOfLargeFile = os.path.split(filePath)
    # diffInventory_path = os.path.join(pathOfLargeFile, nameOfLargeFile+'-splitted-' +str(CHUNK_ROWS_NUM))
    # diffInventoryArchive_path = os.path.join(pathOfLargeFile, nameOfLargeFile+ 'splitted-SQSProcessed-' +str(CHUNK_ROWS_NUM))
    diffInventory_path = Path(pathOfLargeFile).joinpath( nameOfLargeFile+'-splitted-' +str(CHUNK_ROWS_NUM))
    diffInventoryArchive_path = Path(pathOfLargeFile).joinpath(nameOfLargeFile+ 'splitted-SQSProcessed-' +str(CHUNK_ROWS_NUM))
    
    toBeProcessingFiles = list(Path(diffInventory_path).glob("*.csv"))
    processedFiles = list(Path(diffInventoryArchive_path).glob("*.csv"))
    
    if len(toBeProcessingFiles) != 0 or len(processedFiles) != 0:
        logger.info(f"Warning: the folder is not empty - NumOfToBeProcessing#{len(toBeProcessingFiles)}, NumOfProcessed#{len(processedFiles)},if you want to force to split the file {nameOfLargeFile} and run again, please manually delete the splitted folders {diffInventory_path} and {diffInventoryArchive_path}")
        return diffInventory_path,diffInventoryArchive_path
    
    Path(diffInventoryArchive_path).mkdir(parents=True, exist_ok=True)
    count = 0;
    try:
        if not Path(diffInventory_path).exists():
            Path(diffInventory_path).mkdir(parents=True, exist_ok=True)  
            pdReader = pd.read_csv(filePath, usecols=lambda x: x.upper() in ['STORAGE-ACCOUNT','NAME', 'CONTENT-LENGTH','VARIANCE'], chunksize=CHUNK_ROWS_NUM)
            
            for i, chunk in enumerate(pdReader):
                count = count + 1
                name = Path(diffInventory_path).joinpath(nameOfLargeFile+"-"+str(i)+".csv")
                logger.info(name)
                chunk.to_csv(name, sep=",")
    except Exception as e:
        Path(diffInventory_path).rmdir()
        Path(diffInventoryArchive_path).rmdir()
        logger.error(f"Error in Splitting Large file {filePath}, size {Path(filePath).stat().st_size} with exception {e}")
        sys.exit(0)
    if count == 0: # no files splitted, remove the folders
        if Path(diffInventory_path).exists(): Path(diffInventory_path).rmdir()
        if Path(diffInventoryArchive_path).exists(): Path(diffInventoryArchive_path).rmdir()
    logger.info(f"Successfully splitting the file {filePath} into {count} csv fils with {CHUNK_ROWS_NUM} each!")    
    return diffInventory_path,diffInventoryArchive_path

def pdChunkReadJob(is_local_debug,region,filePath,archivePath,logger):
    sqs, ddb = setAWSServices(is_local_debug,region)
    if type(filePath) is not str:
        filePath = str(filePath.resolve())
    return pdChunkReadNew(filePath,sqs,archivePath,logger)

def pdChunkReadNew(filePath,sqs,archivePath,logger):
    # time taken to read data
    s_time = time.time()
    readerType = "Pandas Chunk Reader"

    pdReader = pd.read_csv(filePath, usecols=lambda x: x.upper() in ['STORAGE-ACCOUNT','NAME', 'CONTENT-LENGTH','VARIANCE'],iterator=True, chunksize=CHUNK_ROWS_NUM)
    
    totalCount=0
    totalSize=0
    batchJobs = [] # SQS support 10 in batch messages
    # msgIds = [] # save the ids of the batch messages 
    # batchSeq = 1
    hasException = False
    for i, chunk in enumerate(pdReader):
      logger.info(f"Enter Chunk {i}")
      chunk.columns = chunk.columns.str.upper()
      for ri, row in chunk.iterrows():
          _endpoint = f"https://{row['STORAGE-ACCOUNT']}.blob.core.windows.net"
          _name = row['NAME']
          _length = row['CONTENT-LENGTH']
          _action = row['VARIANCE']
          
          # check whether or not the object length is right or not
          sizeByteInt = 0
          try:
              sizeByteInt = int(_length)
          except Exception as e:
              logger.error(f"Error:Something wrong for the content length of the object {filePath}")
              sys.exit(0)
          
          # begin to construct batch messages
          # SendMessage
          if _action.upper() == AzureBlobEventType.DELETE.name:
              job = constructSQSMsg(_name,_length,filePath,_endpoint,AzureBlobEventType.DELETE)
          else:
              job = constructSQSMsg(_name,_length,filePath,_endpoint,AzureBlobEventType.CREATE)
          
          batchJobs.append(job)
          
          if len(batchJobs) == 10: # 刚好10个
            hasException,batchJobs = sendBatchSQS(sqs,batchJobs,logger)
             
          # end 
          totalCount = totalCount + 1
          totalSize = totalSize + sizeByteInt
          
          ## 严格控制处理的总数据量
          if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
              break
       
      # 处理 上一个 Chunk 尾部不足10个的消息
      # SendMessage
      if len(batchJobs) > 0: 
          hasException,batchJobs = sendBatchSQS(sqs,batchJobs,logger)
         
      ## 严格控制处理的总数据量
      if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
          break
    
    e_time = time.time() 
    # archieve the processed inventory file
    pf = Path(filePath)
    pathOfLargeFile = pf.parent
    nameOfLargeFile = pf.name

    if not hasException:
        Path(filePath).replace(Path(archivePath).joinpath(nameOfLargeFile))
    
    logger.info(f"Read {nameOfLargeFile} using {readerType} : {(e_time-s_time)} seconds,with {str(totalCount)} records and {totalSize/1024/1024/1024} GBytes ")
    
    return {"File#":nameOfLargeFile,"TotalObjNum#":totalCount,"TotalObjSize#":totalSize}

def sendBatchSQS(sqs,batchJobs,logger):
    hasException = False
    try:
        # logger.infor(f"Begin Send SQS Messages {batchJobs}")
        sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
        batchJobs = []
    except Exception as e:
        hasException = True
        logger.error(f'Error:Fail to send sqs message: {str(batchJobs)}, {str(e)}') 
    return hasException, batchJobs

# Process one job
def jobProcessor(is_local_debug,region, splittedFiles, archivePath,logger, MaxThread=10):
    def workerDoneCallBack(future):
        result = future.result()
        print(result)
        print('*' * 50)

    # 处理主流程，通过线程池并行处理清单差异切割出来的小文件
    try:
        # with concurrent.futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MaxThread) as pool:
            s_time = time.time()
            # 提交处理每个文件的线程
            allJobs = [pool.submit(pdChunkReadJob,is_local_debug,region,f,archivePath,logger).add_done_callback(workerDoneCallBack) for f in splittedFiles]
            # for f in diffInventoryCSVFiles:
            #     pool.submit(pdChunkReadNew,sqs,f,archivePath).add_done_callback(workerDoneCallBack)
            # concurrent.futures.wait(allJobs, return_when="ALL_COMPLETED")
            pool.shutdown(wait=True)
            e_time = time.time()
            logger.infor("Time to Tread Pool process all {len(splittedFiles)} files and Send to SQS using {(e_time-s_time)/60} minutes")

    except Exception as e:
        logger.error(f'Exception in job_processor: {str(e)}')
        return "ERR"
    return "SUCCESS"


LoggingLevel = 'INFO'

# Main
if __name__ == '__main__':
    logger, log_file_name = setLog(LoggingLevel, 'differInventorySQS')
    sqs, ddb = setAWSServices(is_local_debug,region)

    print(f"Begin to split the large file '{inventoryDiffFile}' using predefined chunk size {CHUNK_ROWS_NUM}")
    s_time = time.time()
    diffInventoryPath,archivePath = pdChunkSplit(inventoryDiffFile)
    e_time = time.time() 
    print(f"End of splitting Azure Inventory Difference File {inventoryDiffFile} using {(e_time-s_time)} seconds")    
    
    # get all splitted azure inventory difference csv files
    diffInventoryCSVFiles = list(Path(diffInventoryPath).glob("*.csv"))
    # retriveFiles(diffInventoryPath,diffInventoryCSVFiles,"/*.csv")
    i = 0
    s_time = time.time()
    print(f"Total Splitted Inventory Files# {len(diffInventoryCSVFiles)}")
    
    jobProcessor(is_local_debug,region, diffInventoryCSVFiles, archivePath,logger,38)
        
    e_time = time.time() 
    print(f"End to process the large file {inventoryDiffFile}, Total Number#{len(diffInventoryCSVFiles)} using {(e_time-s_time)} seconds")


