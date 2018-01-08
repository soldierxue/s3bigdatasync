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
    # logger.info("Input Time#"+str(timeSeconds))
    dt = datetime.datetime.fromtimestamp(timeSeconds)
    # logger.info("Input time to Date time#"+dt.strftime("%Y-%m-%d %H:%M:%S"))
    dtFormatInput = datetime.datetime(dt.year,dt.month,dt.day,dt.hour,dt.minute,0)
    # logger.info("Input time to format time#"+dtFormatInput.strftime("%Y-%m-%d %H:%M:%S"))
    timestampAfter = time.mktime(dtFormatInput.timetuple())+interval * 60
    dtAfter = datetime.datetime.fromtimestamp(timestampAfter)
    # logger.info("Output time2#"+str(timestampAfter)+",Format String#"+time.asctime( time.localtime(timestampAfter)))
    return timestampAfter

class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def private_updateItem(startTime=None,timeUnit=None,replicationStatus=1,totalSize=0,totalNum=0,profile='default',tableName="s3cross_stat"):
    logger.info("begin to update item")
    if startTime is not None and timeUnit is not None:
        session = boto3.Session(profile_name=profile)
        ddbClient = session.client('dynamodb')
        if replicationStatus == 1:
            res = ddbClient.update_item(
                TableName=tableName
                ,Key={
                    'StartTime':{'N':str(startTime)}
                    ,'TimeUnit':{'N':str(timeUnit)}
                }
                ,AttributeUpdates={
                    'SuccessObjectSize':{
                        'Action':'PUT'
                        ,'Value':{'N':str(totalSize)}
                    }  
                    ,'SuccessObjectNum':{
                        'Action':'PUT'
                        ,'Value':{'N':str(totalNum)}
                    }
                }
                ,ReturnValues='ALL_OLD'
            ) 
            # logger.info("success objects res# %s",json.dumps(res)) 
        elif replicationStatus == 0: 
            res = ddbClient.update_item(
                TableName=tableName
                ,Key={
                    'StartTime':{'N':str(startTime)}
                    ,'TimeUnit':{'N':str(timeUnit)}
                }
                ,AttributeUpdates={
                    'FailedObjectSize':{
                        'Action':'PUT'
                        ,'Value':{'N':str(totalSize)}
                    }  
                    ,'FailedObjectNum':{
                        'Action':'PUT'
                        ,'Value':{'N':str(totalNum)}
                    }
                }
                ,ReturnValues='ALL_OLD'
            )
            # logger.info("failed objects res# %s",json.dumps(res))                       

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

def getIterator(beginTime=None,replicationStatus="1",profile='default',monitorTableName="s3cross_monitor",indexMonitor="ReplicationStatus-ReplicationTime-index"):
    logger.info(" Start to get the iterator of the monitor items **")
    try:
        session = boto3.Session(profile_name=profile)
        ddbClient = session.client('dynamodb')
        paginator = ddbClient.get_paginator('query')        
        iterator = paginator.paginate(
            TableName=monitorTableName
            ,IndexName=indexMonitor
            ,Select='ALL_PROJECTED_ATTRIBUTES'
            ,KeyConditions={
                'ReplicationStatus':{
                    'AttributeValueList':[
                        {'N':replicationStatus}
                    ]
                    ,'ComparisonOperator':'EQ'
                }
                ,'ReplicationTime':{
                    'AttributeValueList':[
                        {'N':str(beginTime)}
                    ]
                    ,'ComparisonOperator':'GE'
                }
            }
            ,PaginationConfig={
                'PageSize': 2
            }         
        ) 
        return iterator 
    except:
        traceback.print_exc()
        raise Exception("Failed to query the monitor table!")               

def getStatCategories(beginTime):
    cats = [
        {
            "StartTime":getFormatTimeSeconds(beginTime)
            ,"TimeUnit":1
            ,"FailedObjectSize":0
            ,"FailedObjectNum":0
            ,"SuccessObjectSize":0
            ,"SuccessObjectNum":0
            ,"ObjectKeys":[]
        }
        ,{
            "StartTime":getFormatTimeSeconds(beginTime)
            ,"TimeUnit":5
            ,"FailedObjectSize":0
            ,"FailedObjectNum":0
            ,"SuccessObjectSize":0
            ,"SuccessObjectNum":0
            ,"ObjectKeys":[]
        } 
        ,{
            "StartTime":getFormatTimeSeconds(beginTime)
            ,"TimeUnit":60
            ,"FailedObjectSize":0
            ,"FailedObjectNum":0
            ,"SuccessObjectSize":0
            ,"SuccessObjectNum":0
            ,"ObjectKeys":[]
        }                   
    ]
    return cats

def statRunByScanItems(profile='default',monitorTableName="s3cross_monitor",indexMonitor="ReplicationStatus-ReplicationTime-index",statTableName="s3cross_stat"):
    logger.info("*** S3 Replication Statistic Run - statRunByScanItems***")
    statMaxTime = getMaxStatTimestamp(1,profile,statTableName)#get max timestamp within the items whose time unit = 1 
    monitorMinTime = getMinMonitorTimestamp(1,profile,monitorTableName)
    monitorMinTimeFaileItems = getMinMonitorTimestamp(0,profile,monitorTableName)
    beginTime = statMaxTime
    beginTimeFailItems = statMaxTime
    if monitorMinTime < 0 and monitorMinTimeFaileItems < 0:
        return;

    if statMaxTime < 0:
        if monitorMinTime > 0:
            beginTime = getFormatTimeSeconds(monitorMinTime)
        else:
            beginTime = None
        if monitorMinTimeFaileItems > 0:
            beginTimeFailItems = getFormatTimeSeconds(monitorMinTimeFaileItems)
        else:
            beginTimeFailItems = None
    elif statMaxTime > 0:
        beginTime = getFormatTimeSeconds(statMaxTime) 
        beginTimeFailItems = getFormatTimeSeconds(statMaxTime)
 
    # logger.info("Start to query all items whose replication time is greater or equals with %d (replication success) and %d (replication failed)",beginTime,beginTimeFailItems)
    try:
        successIterator = None
        failedIterator = None
        if beginTime is not None:
            successIterator = getIterator(beginTime,"1",profile,monitorTableName,indexMonitor)
        if beginTimeFailItems is not None:
            if beginTime is None:
                beginTime = beginTimeFailItems
            failedIterator = getIterator(beginTime,"0",profile,monitorTableName,indexMonitor)
    
        if successIterator is not None:
            countStatItems(successIterator,beginTime,profile,statTableName)                                                                               
        if failedIterator is not None:
            countStatItems(failedIterator,beginTimeFailItems,profile,statTableName)   
    except:
        traceback.print_exc()     

def countStatItems(iterator,beginTime,profile,tableName):
    statMatrix = getStatCategories(beginTime)
    statItems = []  

    if iterator is not None:
        # loop from the success copied items
        for page in iterator:
            items = page['Items']
            for item in items:
                # logger.info("Item# %s",json.dumps(item))
                rtime = float(item['ReplicationTime']['N'])
                osize = 0
                rstatus = item['ReplicationStatus']['N']
                okey = item['ObjectKey']['S']
                fRTime = getFormatTimeSeconds(rtime)
                if 'Size' in item:
                    osize = float(item['Size']['N'])
                for cat in statMatrix:                      
                    catEndTime = cat['StartTime']+cat['TimeUnit']*60
                    if rtime >= cat['StartTime'] and rtime < catEndTime:
                        if rstatus == "1":
                            cat['SuccessObjectSize'] = cat['SuccessObjectSize'] + osize
                            cat['SuccessObjectNum'] = cat['SuccessObjectNum'] + 1
                        elif rstatus == "0":
                            cat['FailedObjectSize'] = cat['FailedObjectSize'] + osize
                            cat['FailedObjectNum'] = cat['FailedObjectNum'] + 1  
                    else:
                        # Time Range scan finished, be ready to save
                        logger.info("Begin to save the time range result# [%d,%d] ",cat['StartTime'],catEndTime)
                        if cat['SuccessObjectNum'] > 0:
                            private_updateItem(cat['StartTime'],cat['TimeUnit'],1, cat['SuccessObjectSize'],cat['SuccessObjectNum'],profile,tableName)
                        if cat['FailedObjectNum'] > 0:
                            private_updateItem(cat['StartTime'],cat['TimeUnit'],0, cat['FailedObjectSize'],cat['FailedObjectNum'],profile,tableName)    
                        statItems.append(cat.copy())
                        cat['StartTime'] = fRTime
                        cat['SuccessObjectSize'] = 0 
                        cat['SuccessObjectNum'] = 0 
                        cat['FailedObjectSize'] = 0 
                        cat['FailedObjectNum'] = 0  
                        if rstatus == "1":
                            cat['SuccessObjectSize'] = osize
                            cat['SuccessObjectNum'] = 1
                        elif rstatus == "0":
                            cat['FailedObjectSize'] = osize
                            cat['FailedObjectNum'] = 1      
        for cat in statMatrix:  
            if cat['SuccessObjectNum'] >= 1 or cat['FailedObjectNum'] >= 1:
                logger.info("There are success or failed objects need to be added to statistic data#")
                statItems.append(cat.copy())
    

    logger.info("Items#"+json.dumps(statItems))

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
            if 'StartTime' in item:
                timestamp = float(item['StartTime']['N'])
        logger.info("timestamp#"+str(timestamp))        
    except:
        traceback.print_exc()  
    return timestamp
           

def main():
    logger.info("**** Monitoring S3 Copying Progress - Statistic *****! ")
    statRunByScanItems()

if __name__ == "__main__":
    main()     