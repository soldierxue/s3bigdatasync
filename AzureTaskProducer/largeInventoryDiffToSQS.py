## Read large number of Azure Blob Storage Inventory differece CSV files and Construct Format Message to SQS

import boto3
import json
import time
from pathlib import PurePosixPath, Path
import os
import pandas as pd
import json
from datetime import datetime, timezone
from inventory_lib import setLog, constructSQSMsg, AzureBlobEventType,retriveFiles,setAWSServices, \
     checkMsgIdsFromDDB,ddbBatchSaveMsgLogs

is_local_debug = False # 是否是本地调试
is_limit_debug = False # 是否通过MAX_INVENTORY_NUM，MAX_OBJ_TOTAL_NUM 或 MAX_OBJ_TOTAL_SIZE 三者来控制提交消息的数量
region = "us-east-2"
sqs_queue = "https://sqs.us-east-2.amazonaws.com/188869792837/recode_msg_test"
# ddb_table = "inventoryMsgTable"

# global variables
inventoryDiffFile="/home/ec2-user/environment/s3/diffInventory/sample2.csv" # which root folder has the inventory diff csv files
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

def pdChunkSplit(filePath):
    pathOfLargeFile, nameOfLargeFile = os.path.split(filePath)
    diffInventory_path = os.path.join(pathOfLargeFile, 'splitted' +str(CHUNK_ROWS_NUM))
    diffInventoryArchive_path = os.path.join(pathOfLargeFile, 'splittedArchieve' +str(CHUNK_ROWS_NUM))
    Path(diffInventoryArchive_path).mkdir(parents=True, exist_ok=True) 
    if not os.path.exists(diffInventory_path):
        Path(diffInventory_path).mkdir(parents=True, exist_ok=True)  
        pdReader = pd.read_csv(filePath, usecols=['Storage-Account','Name', 'Content-Length','Variance'], chunksize=CHUNK_ROWS_NUM)
        for i, chunk in enumerate(pdReader):
            name = os.path.join(diffInventory_path, nameOfLargeFile+"-"+str(i)+".csv")
            logger.info(name)
            chunk.to_csv(name, sep=",")
    return diffInventory_path,diffInventoryArchive_path

def pdChunkReadNew(filePath,sqs,archivePath):
  # time taken to read data
    s_time = time.time()
    readerType = "Pandas Chunk Reader"

    pdReader = pd.read_csv(filePath, usecols=['Storage-Account','Name', 'Content-Length','Variance'],iterator=True, chunksize=CHUNK_ROWS_NUM)
    
    totalCount=0
    totalSize=0
    batchJobs = [] # SQS support 10 in batch messages
    # msgIds = [] # save the ids of the batch messages 
    # batchSeq = 1
    hasException = False
    for i, chunk in enumerate(pdReader):
      logger.info(f"Enter Chunk {i}")
      for ri, row in chunk.iterrows():
          _endpoint = f"https://{row['Storage-Account']}.blob.core.windows.net"
          _name = row['Name']
          _length = row['Content-Length']
          _action = row['Variance']
          
          # begin to construct batch messages
          # SendMessage
          if _action.upper() == AzureBlobEventType.DELETE.name:
              job = constructSQSMsg(_name,_length,filePath,_endpoint,AzureBlobEventType.DELETE)
          else:
              job = constructSQSMsg(_name,_length,filePath,_endpoint,AzureBlobEventType.CREATE)
          
        #   msgId=job['Id']

        #   msgIds.append(msgId)
          batchJobs.append(job)
          
          if len(batchJobs) == 10: # 刚好10个
            hasException,batchJobs = sendBatchSQS(sqs,batchJobs)
             
          # end 
          totalCount = totalCount + 1
          totalSize = totalSize + _length
          ## 严格控制处理的总数据量
          if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
              break
       
      # 处理 上一个 Chunk 尾部不足10个的消息
      # SendMessage
      if len(batchJobs) > 0: 
          hasException,batchJobs = sendBatchSQS(sqs,batchJobs)
         
      ## 严格控制处理的总数据量
      if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
          break
    
    e_time = time.time() 
    # archieve the processed inventory file
    pathOfLargeFile, nameOfLargeFile = os.path.split(filePath)
    Path(filePath).replace(archivePath+"/"+nameOfLargeFile)
    
    logger.info(f"Read using {readerType} : {(e_time-s_time)} seconds,with {str(totalCount)} records and {totalSize} Bytes ")
    
    return {"TotalObjNum":totalCount,"TotalObjSize":totalSize}

def sendBatchSQS(sqs,batchJobs):
    hasException = False
    try:
        # print(f"Begin Send SQS Messages {batchJobs}")
        sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
        batchJobs = []
    except Exception as e:
        hasException = True
        print(f'Fail to send sqs message: {str(batchJobs)}, {str(e)}') 
    return hasException, batchJobs

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
    diffInventoryCSVFiles = [] 
    retriveFiles(diffInventoryPath,diffInventoryCSVFiles,"/*.csv")
    i = 0
    s_time = time.time()
    print(f"Total Splitted Inventory Files# {len(diffInventoryCSVFiles)}")
    for f in diffInventoryCSVFiles:
        i = i + 1        
        print(f"Begin to processing file '{f}'")
        
        pdChunkReadNew(f,sqs,archivePath)
                
        if is_limit_debug and i >= MAX_INVENTORY_NUM:
            break  
        print(f"End to process Azure Inventory Difference File {f}")
        
    e_time = time.time() 
    print(f"End to process Azure Inventory Difference Files {(e_time-s_time)} seconds")


