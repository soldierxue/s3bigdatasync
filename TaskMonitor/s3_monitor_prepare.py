#!/bin/python
# coding=utf-8

### Python 2.7 & Boto3
import json
import time
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import traceback
import logging
import random


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Current Timestamp in seconds
def current_time_seconds():
    seconds = time.time()+random.random()
    # logger.info(time.asctime( time.localtime(seconds)))
    return seconds

# Customized Encoder for JSON datatime
class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

# Load Cloud Formation file as string
def loadCFileAString(cf_file):
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
# Check Whether or not the Stack is exist
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
# Check whether or not the ddb table is exist
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

# Create the Required DDB Stack or update the stack
def prepareDDBResourceByCF(profile='default',monitorTableName="s3cross_monitor",statTableName="s3cross_stat"):
    logger.info(" Prepare DDB Table for S3 Cross Replication Monitoring ")
    session = boto3.Session(profile_name=profile)
    cfClient = session.client('cloudformation') 
    stackName = "s3cross-monitor-stack"
    cfBody = loadCFileAString("ddb.yaml")
    if cfBody == "":
        raise Exception("The CloudFormation Template is not found!") 
    if isStackExist(stackName,profile):
        # update the stack
        logger.info("Stack Already Exist, we will try to update it!!")
        try:
            updateStackRes = cfClient.update_stack(
                StackName = stackName,
                TemplateBody=cfBody,
                Capabilities=[
                    'CAPABILITY_IAM',
                ]           
            ) 
        except:
            traceback.print_exc()          
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

# Interface to task execution to save monitor items
# Inputs:
#      monitorItems =[
#         {  
#           "ReplicationStatus":0|1 #required
#           ,"Key":"" #required
#           ,"Size": 85697 # optional, better includes for total size summary
#           ,"LastModified": "2016-06-22T09:54:26.000Z" # optional
#           ,"ETag": "6e8c28243c55edd44cc8a796a332f1c2 # optional
#           ,"StorageClass": "STANDARD" # optional
#           ,"IsMultipartUploaded":True|False # optional
#         }
#      ]
#       profile # the IAM role profile name, default value is 'default'
#       tableName # the table name of the monitor table, default is "s3cross_monitor"  
# 
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

def main():
    logger.info("**** Monitoring S3 Copying Progress - Prepare Model *****! ")
    logger.info("**** (1) Create DDB Tables (2) Provide interface [batchPutStatus] for task execution to add monitor items *****! ")
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
    # prepareDDBResourceByCF("bjs")
    batchPutStatus(testMonitorObjects,"bjs")

if __name__ == "__main__":
    main()     