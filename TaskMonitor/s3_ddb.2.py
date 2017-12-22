#!/bin/python
# coding=utf-8

### Python 2.7 & Boto3
import json
import time 
import datetime
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import traceback
import logging
import random


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def current_time_seconds():
    seconds = time.time()+random.random()
    # logger.info(time.asctime( time.localtime(seconds)))
    return seconds

# Input: (1)以秒为单位的时间值(2)时间间隔，以分为单位
# Output: 整分开始时间（秒为0）
def getFormatTimeSeconds(timeSeconds,interval=0):
    logger.info("Input Time#"+str(timeSeconds))
    dt = datetime.datetime.fromtimestamp(timeSeconds)
    logger.info("Input time to Date time#"+dt.strftime("%Y-%m-%d %H:%M:%S"))
    dtFormatInput = datetime.datetime(dt.year,dt.month,dt.day,dt.hour,dt.minute,0)
    logger.info("Input time to format time#"+dtFormatInput.strftime("%Y-%m-%d %H:%M:%S"))
    timestampAfter = time.mktime(dtFormatInput.timetuple())+interval * 60
    dtAfter = datetime.datetime.fromtimestamp(timestampAfter)
    logger.info("Output time2#"+str(timestampAfter)+",Format String#"+time.asctime( time.localtime(timestampAfter)))
    return timestampAfter

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def loadCFAsString(cf_file):
    if os.path.exists(cf_file):
        with open(cf_file, 'r') as f:
            try:
                fbuffer = f.read().decode("utf-8")
                #logger.info("File content#"+json.dumps(fbuffer))
                f.close()
                return fbuffer
            except:
                return ""
    return ""        

def isStackExist(stackName,profile='default'):
    session = boto3.Session(profile_name=profile)
    cfClient = session.client('cloudformation') 
    try:
        response = cfClient.describe_stacks(
            StackName=stackName
        ) 
        return True 
    except ClientError as e:
        if e.response['Error']['Code'] == 'AmazonCloudFormationException':
            logger.info(" No Stack Exist!")
            return False  
        return False

def isDDBTableExist(tableName,profile='default'):
    session = boto3.Session(profile_name=profile)
    ddbClient = session.client('dynamodb') 
    try:
        # check whether or not the table is exist or not
        res = ddbClient.describe_table(TableName=tableName)
        logger.info(json.dumps(res, cls=ComplexEncoder)) 
        logger.info(" DDB Table is already exist, we will try to update the stack!")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':   
            return False
        return False

def prepareDDBResourceByCF(profile='default',monitorTableName="s3cross_monitor",statTableName="s3cross_stat"):
    logger.info(" Prepare DDB Table for S3 Cross Replication Monitoring ")
    session = boto3.Session(profile_name=profile)
    cfClient = session.client('cloudformation') 
    stackName = "s3cross-monitor-stack"
    cfBody = loadCFAsString("ddb.yaml")
    if cfBody == "":
        raise Exception("The CloudFormation Template is not found!") 
    if isStackExist(stackName,profile):
        # update the stack
        logger.info("Stack Already Exist, we will try to update it!!")
        updateStackRes = cfClient.update_stack(
            StackName = stackName,
            TemplateBody=cfBody,
            Capabilities=[
                'CAPABILITY_IAM',
            ]           
        )       
    else:
        # create the stack
        # check whether or not the DDB table is exist    
        if isDDBTableExist(monitorTableName,profile) or isDDBTableExist(statTableName,profile):
            # Error : The table is already exist while we are trying to create it
            raise Exception("The table is already exist while we are trying to create it!") 
        else:
            # prepare the ddb table by cf template
            createStackRes = cfClient.create_stack(
                StackName = stackName,
                TemplateBody=cfBody,
                Parameters=[
                    {
                        'ParameterKey': 'MonitorTableName',
                        'ParameterValue': monitorTableName
                    },
                    {
                        'ParameterKey': 'StatTableName',
                        'ParameterValue': statTableName
                    }                    
                ],
                TimeoutInMinutes=5,
                Capabilities=[
                    'CAPABILITY_IAM',
                ],
                OnFailure='ROLLBACK',
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'S3Cross'
                    },
                    {
                        'Key': 'Module',
                        'Value': 'TaskMonitor_Verification'
                    }                 
                ],
                ClientRequestToken='string',
                EnableTerminationProtection=False
            )     
## Private Function to init a start time item, that we can easily get the time when
# objects begins to replicate 
isInitTimeSuccess = False 
MONITOR_INIT_KEY = 'PlaceHolder_First_Monitor_Item_Key'
MONITOR_INIT_STATUS = '10'
MONITOR_INIT_TIMESTAMP = None
def initMonitorTime(profile='default',tableName="s3cross_monitor"):
    logger.info("** Init Monitor Time**")
    getInitMonitorTimestamp(profile,tableName)
    if isInitTimeSuccess is False or MONITOR_INIT_TIMESTAMP is None:
        session = boto3.Session(profile_name=profile)
        ddbClient = session.client('dynamodb')
        try:
            res = ddbClient.put_item(
                TableName=tableName,
                Item={
                    "ObjectKey":{"S":MONITOR_INIT_KEY}
                    ,"ReplicationStatus":{"N":MONITOR_INIT_STATUS}
                    ,"ReplicationTime":{"N":str(getFormatTimeSeconds(current_time_seconds()))}
                }
                ,ReturnValues='ALL_NEW',
            )  
            isInitTimeSuccess = True
        except:
            traceback.print_exc()          

def getInitMonitorTimestamp(profile='default',tableName="s3cross_monitor"):
    logger.info("** get init monitor item timestamp **")
    session = boto3.Session(profile_name=profile)
    ddbClient = session.client('dynamodb')
    try:
        res = ddbClient.get_item(
            TableName=tableName,
            Key={
                "ObjectKey":{"S":MONITOR_INIT_KEY}
                ,"ReplicationStatus":{"N":MONITOR_INIT_STATUS}
            }
        )  
        isInitTimeSuccess = True
        MONITOR_INIT_TIMESTAMP = res['Item']['ReplicationTime']['S']
        logger("Init Monitor Item Timestamp#"+MONITOR_INIT_TIMESTAMP)
        return MONITOR_INIT_TIMESTAMP
    except:
        isInitTimeSuccess = False
        traceback.print_exc() 
    return ""    

def private_batchPutStat(statItems,profile='default',tableName="s3cross_stat"):
    MAX_ITEM_ONE_BATCH = 2#25
    count = 0
    batchRequests = []
    pitems = []
    for mitem in statItems:
        count = count + 1
     
        pitem = {
           "PutRequest": {
                "Item": {
                    "TimeUnit":{"N":str(mitem['TimeUnit'])}
                    ,"StartTime":{"N":str(mitem['StartTime'])}
                }
           }    
        }
        if 'SuccessObjectSize' in mitem:
            pitem["PutRequest"]["Item"]["SuccessObjectSize"]={"N":str(mitem['SuccessObjectSize'])}
        if 'SuccessObjectNum' in mitem:
            pitem["PutRequest"]["Item"]["SuccessObjectNum"]={"N":str(mitem['SuccessObjectNum'])}
        if 'FailedObjectSize' in mitem:
            pitem["PutRequest"]["Item"]["FailedObjectSize"]={"N":str(mitem['FailedObjectSize'])}
        if 'FailedObjectNum' in mitem:
            pitem["PutRequest"]["Item"]["FailedObjectNum"]={"N":str(mitem['FailedObjectNum'])}
        # logger.info(json.dumps(pitem))

        if count < MAX_ITEM_ONE_BATCH:
            pitems.append(pitem)
        elif count == MAX_ITEM_ONE_BATCH:
            pitems.append(pitem)
            preq = {
                tableName:pitems
            }
            batchRequests.append(preq)
            pitems = []
            count = 0 
    if count > 0 and count < MAX_ITEM_ONE_BATCH:
        preq = {
            tableName:pitems
        }
        batchRequests.append(preq)
    
    if len(batchRequests) > 0:
        session = boto3.Session(profile_name=profile)
        ddbClient = session.client('dynamodb')
        for req in batchRequests:
            logger.info(json.dumps(req))
            ddbClient.batch_write_item(
                RequestItems=req
            )

def batchPutStatus(monitorItems,profile='default',tableName="s3cross_monitor"):
    MAX_ITEM_ONE_BATCH = 2#25
    count = 0
    batchRequests = []
    pitems = []
    for mitem in monitorItems:
        count = count + 1
     
        pitem = {
           "PutRequest": {
                "Item": {
                    "ObjectKey":{"S":mitem['Key'].decode('utf-8')}
                    ,"ReplicationStatus":{"N":str(mitem['ReplicationStatus'])}
                    ,"ReplicationTime":{"N":str(current_time_seconds())}
                }
           }    
        }
        if 'IsMultipartUploaded' in mitem:
            pitem["PutRequest"]["Item"]["IsMultipartUploaded"]={"BOOL":mitem['IsMultipartUploaded']}
        if 'Size' in mitem:
            pitem["PutRequest"]["Item"]["Size"]={"N":str(mitem['Size'])}
        if 'LastModified' in mitem:
            pitem["PutRequest"]["Item"]["LastModified"]={"S":mitem['LastModified']}
        if 'ETag' in mitem:
            pitem["PutRequest"]["Item"]["ETag"]={"S":mitem['ETag']}
        if 'StorageClass' in mitem:
            pitem["PutRequest"]["Item"]["StorageClass"]={"S":mitem['StorageClass']}
        # logger.info(json.dumps(pitem))

        if count < MAX_ITEM_ONE_BATCH:
            pitems.append(pitem)
        elif count == MAX_ITEM_ONE_BATCH:
            pitems.append(pitem)
            preq = {
                tableName:pitems
            }
            batchRequests.append(preq)
            pitems = []
            count = 0 
    if count > 0 and count < MAX_ITEM_ONE_BATCH:
        preq = {
            tableName:pitems
        }
        batchRequests.append(preq)
    
    if len(batchRequests) > 0:
        session = boto3.Session(profile_name=profile)
        ddbClient = session.client('dynamodb')
        for req in batchRequests:
            logger.info(json.dumps(req))
            ddbClient.batch_write_item(
                RequestItems=req
            )

def statRun(profile='default',monitorTableName="s3cross_monitor",statTableName="s3cross_stat"):
    logger.info("*** S3 Replication Statistic Run ***")
    statMinTime = getMinStatTimestamp(1,profile,statTableName)
    statMaxTime = getMinStatTimestamp(1,profile,statTableName)     
    monitorMinTime = getMinMonitorTimestamp("1",profile,monitorTableName)
    monitorMaxTime = getMaxMonitorTimestamp("1",profile,monitorTableName)
    beginTime = getFormatTimeSeconds(monitorMinTime)
    endTime = getFormatTimeSeconds(monitorMaxTime)   
    if statMaxTime > 0 :
        logger.info("")
        timeBefor1Hour = statMaxTime - 60*60
        # 考虑到监控表高并发写的问题，我们对于近1个小时的记录会再次更新统计;1个小时以前的统计不再更新
        if timeBefor1Hour > beginTime:
            beginTime = getFormatTimeSeconds(timeBefor1Hour)
    ## 按1分钟和10分钟为步长，统计目前 Monitor 表中的信息 
    timeUnit = 1
    statItems = []    
    SAVE_ITEM_SIZE = 1        
    while beginTime < endTime:
        nextEndTime = beginTime + timeUnit * 60
        items = private_getMonitorItems(beginTime,nextEndTime,"1",profile)
        if len(items) > 0:       
            totalSize = 0
            totalCount = len(items)
            for item in items:
                # logger.info("Item#"+json.dumps(item))
                size = 0
                if 'Size' in item:
                    size = float(item['Size']['N'])
                totalSize = totalSize + size
            # logger.info("Total#"+str(totalSize)+","+str(totalCount))
            statItem = {"StartTime":beginTime,"TimeUnit":timeUnit,"SuccessObjectSize":totalSize,"SuccessObjectNum":totalCount}
            statItems.append(statItem) 
            if len(statItems) >= SAVE_ITEM_SIZE:
                ## Save the Statistic Data 
                logger.info("Item#"+json.dumps(statItems))
                private_batchPutStat(statItems,profile,statTableName) 
                statItems = []
                     
        beginTime = nextEndTime  

    if len(statItems) < SAVE_ITEM_SIZE:
        private_batchPutStat(statItems,profile,statTableName)

# 获取 Monitor Table 最小的Time值，默认是当前时间
def getMinMonitorTimestamp(replicationStatus='1',profile='default',monitorTableName="s3cross_monitor",indexName='ReplicationStatus-ReplicationTime-index'):
    # logger.info("*** Get Monitor Timestamp (最小值) ***")
    timestamp = private_GetTimestamp('ReplicationStatus',replicationStatus,profile,monitorTableName,indexName)
    return timestamp;
def getMaxMonitorTimestamp(replicationStatus='1',profile='default',monitorTableName="s3cross_monitor",indexName='ReplicationStatus-ReplicationTime-index'):
    # logger.info("*** Get Monitor Timestamp (最大值) ***")
    timestamp = private_GetTimestamp('ReplicationStatus',replicationStatus,profile,monitorTableName,indexName,False)
    return timestamp;

def getMinStatTimestamp(timeUnit=1,profile='default',statTableName="s3cross_stat",indexName='gsiTimeUnit'):
    # logger.info("*** Get Stat Table Timestamp (最小值) ***")
    timestamp = private_GetTimestamp('TimeUnit',timeUnit,profile,statTableName,indexName)
    return timestamp; 

def getMaxStatTimestamp(timeUnit=1,profile='default',statTableName="s3cross_stat",indexName='gsiTimeUnit'):
    # logger.info("*** Get Stat Table Timestamp (最大值) ***")
    timestamp = private_GetTimestamp('TimeUnit',timeUnit,profile,statTableName,indexName,False)
    return timestamp; 

# 获取最小的Time值，如果返回值小于0表示表里面没有数据
def private_GetTimestamp(keyName,keyValue,profile='default',tableName="s3cross_monitor",indexName='gsiStatus',isAscending=True):
    # logger.info("*** Get Monitor Timestamp  ***")
    session = boto3.Session(profile_name=profile)
    ddbClient = session.client('dynamodb')
    timestamp = -1
    try:
        resSuccess = ddbClient.query(
            TableName=tableName
            ,IndexName=indexName
            ,Select='ALL_PROJECTED_ATTRIBUTES'#'COUNT',
            ,Limit=1
            ,ScanIndexForward=isAscending
            ,KeyConditions={
                keyName:{
                    'AttributeValueList':[
                        {'N':str(keyValue)}
                    ]
                    ,'ComparisonOperator':'EQ'
                }               
            }           
        )
        if 'Count' in resSuccess and resSuccess['Count'] == 1:
            item = resSuccess['Items'][0]
            if 'ReplicationTime' in item:
                timestamp = float(item['ReplicationTime']['N'])
            if 'TimeUnit' in item:
                timestamp = float(item['TimeUnit']['N'])
        logger.info("timestamp#"+str(timestamp))        
    except:
        traceback.print_exc()  
    return timestamp


def private_getMonitorItems(startTime,endTime,replicationStatus='1',profile='default',tableName='s3cross_monitor',indexName='gsiStatus'):
    logger.info("*** DDB Query Request2 ***")
    session = boto3.Session(profile_name=profile)
    ddbClient = session.client('dynamodb')
    paginator = ddbClient.get_paginator('query')
    totalItems = [] 
    try:
        fe = Key('ReplicationTime').between(startTime,endTime-1)
        iterator = paginator.paginate(
            TableName=tableName
            ,IndexName=indexName
            ,Select='ALL_PROJECTED_ATTRIBUTES'#'COUNT',
            ,KeyConditions={
                'ReplicationStatus':{
                    'AttributeValueList':[
                        {'N':replicationStatus}
                    ]
                    ,'ComparisonOperator':'EQ'
                }
            }
            # ,QueryFilter={
            #     'ReplicationTime':{
            #         'AttributeValueList':[
            #             {'N':str(startTime)}
            #         ]
            #         ,'ComparisonOperator':'GE'#|'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'
            #     }
            #     ,'ReplicationTime':{
            #         'AttributeValueList':[
            #             {'N':str(endTime)}
            #         ]
            #         ,'ComparisonOperator':'LT'#|'NE'|'IN'|'LE'|'LT'|'GE'|'GT'|'BETWEEN'|'NOT_NULL'|'NULL'|'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'
            #     }                          
            # }
            ,FilterExpression=fe#"ReplicationTime >= :startTime and ReplicationTime < :endTime"
            ,PaginationConfig={
                # 'MaxItems': 10,
                'PageSize': 10
            }            
        )
        for page in iterator:
            items = page['Items']
            totalItems.extend(items)   
            # logger.info("resume token#"+iterator.resume_token) 
        logger.info("Len#"+str(len(totalItems)))        
    except:
        traceback.print_exc()
        return []    
    return totalItems
           

def main():
    logger.info("**** Monitoring S3 Copying Progress *****! ")

    # prepareDDBResourceByCF("bjs")
    testMonitorObjects = [
        {
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "IsMultipartUploaded":True,
                "ReplicationStatus":0                                                                                                                 
        }  
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "ReplicationStatus":0                                                                                                                 
        }
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "IsMultipartUploaded":False,
                "ReplicationStatus":0                                                                                                                 
        }
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "ReplicationStatus":0                                                                                                                 
        }
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "ReplicationStatus":1                                                                                                                 
        }
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "ReplicationStatus":0                                                                                                                 
        }
        ,{
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "6e8c28243c55edd44cc8a796a332f1c2",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "ReplicationStatus":1                                                                                                                
        }                                
    ]
    # batchPutStatus(testMonitorObjects,"bjs")
    statRun("bjs")
    # ddbQuery_MonitorItems2("1","bjs")
    # ddbQuery_MonitorItems2("0","bjs")
    # ddbQuery_MonitorItems2("10","bjs")
    # getFormatTimeSeconds(current_time_seconds(),0)
    # getFormatTimeSeconds(current_time_seconds(),1)
    # getFormatTimeSeconds(current_time_seconds(),5)

if __name__ == "__main__":
    main()     