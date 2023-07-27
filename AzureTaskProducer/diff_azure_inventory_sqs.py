## Read large number of Azure Blob Storage Inventory differece CSV files and Construct Format Message to SQS

import boto3
import json
import time

import pandas as pd
import json
from datetime import datetime, timezone
from inventory_lib import setLog, constructSQSMsg, AzureBlobEventType,retriveFiles,setAWSServices, \
     checkMsgIdsFromDDB,ddbBatchSaveMsgLogs

is_local_debug = False # 是否是本地调试
is_limit_debug = False # 是否通过MAX_INVENTORY_NUM，MAX_OBJ_TOTAL_NUM 或 MAX_OBJ_TOTAL_SIZE 三者来控制提交消息的数量
region = "us-east-2"
sqs_queue = "https://sqs.us-east-2.amazonaws.com/188869792837/recode_msg_test"
ddb_table = "inventoryMsgTable"

# global variables
rootFolder="/home/ec2-user/environment/s3/diffInventory" # which root folder has the inventory diff csv files
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


def pdChunkReadNew(filePath,sqs,ddbTable):
  # time taken to read data
    s_time = time.time()
    readerType = "Pandas Chunk Reader"

    pdReader = pd.read_csv(filePath, usecols=['Storage-Account','Name', 'Content-Length','Variance'],iterator=True, chunksize=CHUNK_ROWS_NUM)
    
    totalCount=0
    totalSize=0
    batchJobs = [] # SQS support 10 in batch messages
    msgIds = [] # save the ids of the batch messages 
    batchSeq = 1
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
          
          msgId=job['Id']
          if checkMsgIdsFromDDB(ddbTable,filePath, msgId) < 0: # 该对象还未被发送到 DDB
              msgIds.append(msgId)
              batchJobs.append(job)
          else:
              logger.info("Blob Has already been pushed to SQS {msgId}")
          
          if len(batchJobs) == 10: # 刚好10个，或者虽然不足10但已经到了该Chunk最后一个
            try:
                # print(f"Begin Send SQS Messages {batchJobs}")
                sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
                ddbBatchSaveMsgLogs(ddbTable, filePath,msgIds)
                batchJobs = []
                msgIds = []
                batchSeq = batchSeq +1
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
                ddbBatchSaveMsgLogs(ddbTable, filePath,msgIds)
                batchJobs = []
                msgIds = []
                batchSeq = batchSeq +1
            except Exception as e:
                print(f'Fail to send sqs message: {str(batchJobs)}, {str(e)}')           
      ## 严格控制处理的总数据量
      if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
          break
    
    e_time = time.time() 
    logger.info(f"Read using {readerType} : {(e_time-s_time)} seconds,with {str(totalCount)} records and {totalSize} Bytes ")
    return {"TotalObjNum":totalCount,"TotalObjSize":totalSize}


LoggingLevel = 'INFO'

# Main
if __name__ == '__main__':
    logger, log_file_name = setLog(LoggingLevel, 'differInventorySQS')
    sqs, ddb = setAWSServices(is_local_debug,region)
    ddbtable = ddb.Table(ddb_table)
    
    # get all azure inventory difference csv files
    diffInventoryCSVFiles = []
    
    print(rootFolder)
    
    retriveFiles(rootFolder,diffInventoryCSVFiles,"/*.csv")
    i = 0
    for f in diffInventoryCSVFiles:
        fobj = f[0]
        
        i = i + 1        
        print(f"Begin to processing file '{fobj}'")
        pdChunkReadNew(fobj,sqs,ddbtable)
                
        if is_limit_debug and i >= MAX_INVENTORY_NUM:
            break  
        print(f"End to process Azure Inventory Difference File {fobj}")



