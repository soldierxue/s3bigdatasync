# PROJECT Lib for Transmission Azure Inventory to AWS S3
import datetime
import logging
import logging.handlers as handlers
import hashlib
import concurrent.futures
import threading
import base64
import urllib.request
import urllib.parse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
# from boto3.dynamodb import conditions
import json
import os
import glob
import sys
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from pathlib import PurePosixPath, Path
from enum import Enum
import boto3

logger = logging.getLogger()
# Configure logging
def setLog(LoggingLevel, this_file_name):
    logger.setLevel(logging.WARNING)
    if LoggingLevel == 'INFO':
        logger.setLevel(logging.INFO)
    elif LoggingLevel == 'DEBUG':
        logger.setLevel(logging.DEBUG)
    # File logging
    log_path = Path(__file__).parent / 'azure-inventory-sqs-log'
    if not Path.exists(log_path):
        Path.mkdir(log_path)
    start_time = datetime.now().isoformat().replace(':', '-')[:19]
    _log_file_name = str(log_path / f'{this_file_name}-{start_time}.log')
    print('Log file:', _log_file_name)
    fileHandler = handlers.RotatingFileHandler(filename=_log_file_name,maxBytes=52428800,backupCount=3)## 50MB each
    # fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(fileHandler)
    return logger, _log_file_name

def setAWSServices(is_local_debug,region):
    # Initial sqs client 
    if is_local_debug:  # 运行在本地电脑，读取~/.aws 中的本地密钥
        src_session = boto3.session.Session(profile_name='ww')
        sqs = src_session.client('sqs', region_name=region)
        dynamodb = src_session.client('dynamodb', region_name=region)
    else:  # 运行在EC2上，直接获取服务器带的 IAM Role
        sqs = boto3.client('sqs', region)
        dynamodb = boto3.resource('dynamodb', region)
        # ddbtable = dynamodb.Table(ddb_table)
    return sqs,dynamodb

class AzureBlobEventType(Enum):
    DELETE = 1
    CREATE = 2
# 构建同步工具需要的消息格式 Create/Delete Blob
def constructSQSMsg(objfile,size,inventoryCSVFile,endpoint,et:AzureBlobEventType):
    # job = {"file":objfile,"size":size}
    # get current datetime
    today = datetime.now(timezone.utc)
    # Get current ISO 8601 datetime in string format with UTC timezone
    iso_date = today.isoformat().replace("+00:00", "Z")
    eventType = "Microsoft.Storage.BlobCreated"
    api = "PutBlob"    
    
    if et.value == AzureBlobEventType.DELETE.value:
        eventType = "Microsoft.Storage.BlobDeleted"
        api = "DeleteBlob"
    msgId = hashlib.md5((endpoint+objfile+eventType).encode("utf-8")).hexdigest() ## generate a md5 by SA, Object Prefix,eventType
    msgBody = '''
            {
              "topic": "/aws/sqs'''+inventoryCSVFile+'''"
              "subject": "/blobServices/default/containers/'''+objfile+'''",
              "eventType": "'''+eventType+'''",
              "id": "'''+msgId+'''",
              "data": {
                "api": "'''+api+'''",
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
    sqsMsg = {"Id": msgId,"MessageBody": msgBody}
    # logger.info(msgBody)
    return sqsMsg
    
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
            fullMf= os.path.join(root,mf)
            manifestFiles.append(fullMf)
            # print(fullMf) 
     return manifestFiles    
    
# # Get Blob Object Message Hash String in DDB
# def checkMsgIdsFromDDB(table,fileName, saContainer, msgHashId):
#     logger.info(f'Checking Msg Id  {fileName}-{saContainer}-{msgHashId}')
#     rs = -1
#     try:
        
#         response = table.get_item(
#             Key= {'fileName':fileName,'sa_container':saContainer},
#             AttributesToGet=["msgIDs"]
#         )
#         msgIDs = response['Item']['msgIDs']
#         rs = msgIDs.find(msgHashId)
        
#     except Exception as e:
#         logger.error(f'Fail to query DDB for message hashkey {fileName}-{saContainer}-{msgHashId}- {str(e)}')
    
#     logger.info(f'The {msgHashId} exist in DDB? {rs}')
#     return rs

# Get Blob Object Message Hash String in DDB
def checkMsgIdsFromDDB(table,fileName, msgHashId):
    logger.info(f'Checking Msg Id  {fileName}-{msgHashId}')
    rs = -1
    try:
        
        response = table.get_item(
            Key= {'fileName':fileName,'sa_container':msgHashId}
        )
        item = response['Item']
        if len(item) > 0:
            rs = 1
        
    except Exception as e:
        logger.error(f'Fail to query DDB for message hashkey {fileName}-{msgHashId}- {str(e)}')
    
    logger.info(f'The {msgHashId} exist in DDB? {rs}')
    return rs    
    

# # Write log to DDB in first round of job
def ddbSaveMsgLog(table, fileName, saContainer, msgIDs):
    
    logger.info(f'Write log to DDB - {fileName}/{saContainer}')

    try:
        table.put_item(
            Item={'fileName':fileName,'sa_container':saContainer,'msgIDs':msgIDs}
        )
    except Exception as e:
        # 日志写不了
        logger.error(f'Fail to put log to DDB - {str(e)}')
        return    

def ddbBatchSaveMsgLogs(table, fileName, msgIds):
    msgIdsStr = ",".join(str(element) for element in msgIds)
    logger.info(f'Write log to DDB - {fileName}-{msgIdsStr}')

    try:
        with table.batch_writer() as batch:
            for msgId in msgIds:
                batch.put_item(Item={'fileName':fileName,'sa_container':msgId})        

    except Exception as e:
        # 日志写不了
        logger.error(f'Fail to put log to DDB - {str(e)}')
        return     