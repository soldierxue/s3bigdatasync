## Read large number of Azure Blob Storage Inventory CSV files and Construct Format Message to SQS
import boto3
import json
import time

import pandas as pd
import json
from datetime import datetime, timezone
from inventory_lib import setLog, constructSQSMsg, AzureBlobEventType,retriveFiles,setAWSServices, \
     checkMsgIdsFromDDB, ddbSaveMsgLog,ddbBatchSaveMsgLogs

is_local_debug = False # 是否是本地调试
is_limit_debug = True # 是否通过MAX_INVENTORY_NUM，MAX_OBJ_TOTAL_NUM 或 MAX_OBJ_TOTAL_SIZE 三者来控制提交消息的数量
region = "us-east-2"
sqs_queue = "https://sqs.us-east-2.amazonaws.com/188869792837/recode_msg_test"
ddb_table = "inventoryMsgTable"

# global variables
rootFolder="/home/ec2-user/environment/s3/sampleInventory" # which root folder has the inventory manifest & csv files
MAX_INVENTORY_NUM = 1 # the max number of inventory files will be processed
MAX_OBJ_TOTAL_NUM = 51 # the max number of objects will be processed
MAX_OBJ_TOTAL_SIZE = 1024 # GB, the max accumulated obj size will be processed
CHUNK_ROWS_NUM = 1000 # 处理清单时，批量处理的对象行数


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

def pdChunkRead(filePath,azureEndpoint,sqs,ddbTable):
   # time taken to read data
    s_time = time.time()
    readerType = "Pandas Chunk Reader"
    # rowsEachTime = 10000
    pdReader = pd.read_csv(filePath, usecols=['Name', 'Content-Length'],iterator=True, chunksize=CHUNK_ROWS_NUM)
    
    totalCount=0
    totalSize=0
    batchJobs = [] # SQS support 10 in batch messages
    msgIds = [] # save the ids of the batch messages 
    batchSeq = 1    
    for i, chunk in enumerate(pdReader):
       logger.info(f"Enter Chunk {i}")
       for ri, row in chunk.iterrows():
          saContainerByChunk = "chunk_index-"+str(i)+"batch_index-"+str(batchSeq) # group by the same SA and
          _name = row['Name']
          _length = row['Content-Length']
          # begin to construct batch messages
          # SendMessage
          job = constructSQSMsg(_name,_length,filePath,azureEndpoint,AzureBlobEventType.CREATE)
          msgId=job['Id']
          if checkMsgIdsFromDDB(ddbTable,filePath, msgId) < 0: # 该对象还未被发送到 DDB
              msgIds.append(msgId)
              batchJobs.append(job)
          else:
              logger.info(f"Blob Has already been pushed to SQS {msgId}")          
          
          if len(batchJobs) == 10: # 刚好10个，或者虽然不足10但已经到了该Chunk最后一个
            try:
                # print(f"Begin Send SQS Messages {batchJobs}")
                sqs.send_message_batch(QueueUrl=sqs_queue, Entries=batchJobs)
                ddbBatchSaveMsgLogs(ddbTable, filePath,msgIds)
                batchJobs = []
                msgIds = []
                batchSeq = batchSeq +1
            except Exception as e:
                logger.error(f'Fail to send sqs message {str(e)}')              
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
                msgIdsStr = ",".join(str(element) for element in msgIds)
                ddbSaveMsgLog(ddbTable, filePath, saContainerByChunk, msgIdsStr)
                batchJobs = []
                msgIds = []
                batchSeq = batchSeq +1
            except Exception as e:
                logger.error(f'Fail to send sqs message: {str(batchJobs)}, {str(e)}')           
       ## 严格控制处理的总数据量
       if is_limit_debug and (totalCount >= MAX_OBJ_TOTAL_NUM or (totalSize/1024/1024/1024) >= MAX_OBJ_TOTAL_SIZE):
           break
    
    e_time = time.time() 
    logger.info(f"Read using {readerType} : {(e_time-s_time)} seconds,with {str(totalCount)} records and {totalSize} Bytes ")
    return {"TotalObjNum":totalCount,"TotalObjSize":totalSize}

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
        logger.error("File Not Found!")

    
# cep1prod1abcdw_2.csv 13G
# cep1prod1abcdw_283.csv 2G, 
## Read usingDask Reader:  359.83536529541016 seconds,with 157826 records
## Read usingPandas Chunk Reader:  355.2787973880768 seconds,with 3650843009 records
# cep1prod1abcdw_4.csv 4.9G
# cep1prod1abcdw_6.csv 39G


LoggingLevel = 'INFO'

# Main
if __name__ == '__main__':
    logger, log_file_name = setLog(LoggingLevel, 'inventorySQS-log')
    sqs, ddb = setAWSServices(is_local_debug,region)
    ddbtable = ddb.Table(ddb_table)

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

        pathOfMainiffest = manifestFilePath[:manifestFilePath.rindex("/")]
        logger.info(f"Begin to process its Inventory files of manifest {manifestFilePath}")
        inventoryCSVFiles = []
        retriveFiles(pathOfMainiffest,inventoryCSVFiles,"/*.csv")

        i = 0
        for f in inventoryCSVFiles:
            i = i + 1        
            logger.info(f"Begin to processing file '{f[0]}'")
            pdChunkRead(f[0],endpoint,sqs,ddbtable)
            
            if is_limit_debug and i >= MAX_INVENTORY_NUM:
                break  
        logger.info(f"End to process its Inventory files of manifest {manifestFilePath}")
        
        if is_limit_debug and i >= MAX_INVENTORY_NUM:
                    break        



