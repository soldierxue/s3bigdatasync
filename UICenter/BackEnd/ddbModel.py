import boto3
import botocore
import urllib2
import json
import metadataModel
from enum import Enum
import time
import random
import os

class dataType(Enum):
    allValid = 0
    timeNoNeed = 1
    onlySuccess = 2
    onlySuccessSize = 3
    onlyStartTime = 4

IAMSecurityUrl = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
validColumn = ['TimeUnit', 'StartTime', 'SuccessObjectNum', 'SuccessObjectSize', 'FailedObjectNum', 'FailedObjectSize']
dataTypeResponse = [
    'TimeUnit, StartTime, SuccessObjectNum, SuccessObjectSize, FailedObjectNum, FailedObjectSize',
    'SuccessObjectNum, SuccessObjectSize, FailedObjectNum, FailedObjectSize',
    'SuccessObjectNum, SuccessObjectSize',
    'SuccessObjectSize',
    'StartTime'
]
projectStartTime = metadataModel.getConfigurationValue('project_start_time')
tableName = metadataModel.getConfigurationValue('table_name')
bucketName = metadataModel.getConfigurationValue('bucket_name')
speedCollectDelay = metadataModel.getConfigurationValue('speed_collect_delay')
S3ManifestPath = metadataModel.getConfigurationValue('manifest_path')

# Get Temporary Certificate for service.
def getTemporaryCertificate(service):
    response = urllib2.urlopen(IAMSecurityUrl)
    response = urllib2.urlopen(IAMSecurityUrl + response.read())
    IAMTemporaryCertificate = json.loads(response.read())
    awsRegionID = metadataModel.getConfigurationValue('region_id')

    awsAK = IAMTemporaryCertificate['AccessKeyId']
    awsSK = IAMTemporaryCertificate['SecretAccessKey']
    awsToken = IAMTemporaryCertificate['Token']

    client = boto3.client(
        service,
        aws_access_key_id = awsAK,
        aws_secret_access_key = awsSK,
        region_name = awsRegionID,
        aws_session_token = awsToken
    )
    
    return client

# Get manifest data from S3 bucket.
# Input:    Request value [].
# Output:   Response Dir.
def getManifestDataFromS3(requestValue):
    manifestBucketName = S3ManifestPath[:S3ManifestPath.index('/')]
    manifestKeyPath = S3ManifestPath[(S3ManifestPath.index('/')+1):]
    responseDir = {}
    
    # client = boto3.client('s3')
    client = getTemporaryCertificate('s3')
    try:
        client.download_file(manifestBucketName, manifestKeyPath, 'manifest.json')
    except botocore.exceptions.ClientError as e:
        print "Error: Unvaild manifest path input."
        exit(1)
    else:
        with open('manifest.json') as json_input:
            data = json.load(json_input)

        for item in requestValue:
            if data.has_key(item):
                responseDir[item] = data[item]
            
        os.remove('manifest.json')
        return responseDir

# Batch get items.
# Input:    Array. All actions.
# Output:   Response.
def batchGetItemsByArray(data):
    # client = boto3.client('dynamodb')
    client = getTemporaryCertificate('dynamodb')
    
    response = client.batch_get_item(
        RequestItems = {
            tableName: {
                'Keys': data
            }
        }
    )
    
    return response['Responses'][tableName]

# Batch write items.
# Input:    Array. All actions.
# Output:   Dir. Failure actions. 
def batchWriteItemsByArray(data):
    # client = boto3.client('dynamodb')
    client = getTemporaryCertificate('dynamodb')
    
    response = client.batch_write_item(
        RequestItems = {
            tableName: data
        }
    )
    
    return response['UnprocessedItems']

# Get DDB Item by attributes.
def getItemByAttr(timeUnit, startTime, dataRange):
    # client = boto3.client('dynamodb')
    client = getTemporaryCertificate('dynamodb')
    
    response = client.get_item(
        TableName = tableName,
        Key = {
            'TimeUnit': createDDBNumberFormat(timeUnit),
            'StartTime': createDDBNumberFormat(startTime)
        },
        ProjectionExpression = dataTypeResponse[dataRange]
    )
    
    if response.has_key('Item'):
        return response['Item']
    else:
        return {}

# Query DDB by attributes.
# Input:    timeUnit, dataRange(dataType.enum), limit(-1: All).
# Output:   Query response.
def queryByAttr(timeUnit, dataRange, limit=100):
    # client = boto3.client('dynamodb')
    client = getTemporaryCertificate('dynamodb')
    
    if limit != -1:
        response = client.query(
            TableName = tableName,
            Limit = limit,
            KeyConditionExpression = 'TimeUnit = :timeUnit',
            ExpressionAttributeValues = {
                ':timeUnit': createDDBNumberFormat(timeUnit)
            },
            ProjectionExpression = dataTypeResponse[dataRange]
        )
        
        return response['Items']
    else:
        items = []
        
        response = client.query(
            TableName = tableName,
            Limit = 100,
            KeyConditionExpression = 'TimeUnit = :timeUnit',
            ExpressionAttributeValues = {
                ':timeUnit': createDDBNumberFormat(timeUnit)
            },
            ProjectionExpression = dataTypeResponse[dataRange]
        )
        items = items + response['Items']
        
        while response.has_key('LastEvaluatedKey'):
            response = client.query(
                TableName = tableName,
                Limit = 100,
                KeyConditionExpression = 'TimeUnit = :timeUnit',
                ExpressionAttributeValues = {
                    ':timeUnit': createDDBNumberFormat(timeUnit)
                },
                ProjectionExpression = dataTypeResponse[dataRange],
                ExclusiveStartKey = response['LastEvaluatedKey']
            )
            
            items = items + response['Items']
            
        return items

# Create DDB Number Format.
# Input:    Int.
# Output:   Format Dir.
def createDDBNumberFormat(number):
    return { 'N': str(number) }

# Create DDB Data Format.
# Input:    DDB Data Item.
# Output:   DDB Data Format.
def createDDBDataFormat(timeUnit, startTime, successObjectNum=-1, successObjectSize=-1, failedObjectNum=-1, failedObjectSize=-1):
    data = {}
    
    data[validColumn[0]] = createDDBNumberFormat(timeUnit)
    data[validColumn[1]] = createDDBNumberFormat(startTime)
    if successObjectNum == -1:
        data[validColumn[2]] = createDDBNumberFormat(random.randint(0, 10 * timeUnit))
    else:
        data[validColumn[2]] = createDDBNumberFormat(successObjectNum)
    if successObjectSize == -1:
        data[validColumn[3]] = createDDBNumberFormat(random.randint(0, 200 * timeUnit))
    else:
        data[validColumn[3]] = createDDBNumberFormat(successObjectSize)
    if failedObjectNum == -1:
        data[validColumn[4]] = createDDBNumberFormat(random.randint(0, 10 * timeUnit))
    else:
        data[validColumn[4]] = createDDBNumberFormat(failedObjectNum)
    if failedObjectSize == -1:
        data[validColumn[5]] = createDDBNumberFormat(random.randint(0, 200 * timeUnit))
    else:
        data[validColumn[5]] = createDDBNumberFormat(failedObjectSize)
        
    return data

# Update project start time.
# Output:   True|False
def updateProjectStartTime():
    ddbResponse = queryByAttr(1, dataType.onlyStartTime, 1)
    
    if ddbResponse == []:
        return False
    else:
        projectStartTime = int(ddbResponse[0]['StartTime']['N'])
        metadataModel.updateConfigurationValue('project_start_time', projectStartTime)
        return True

# Return data to server controller.
def returnTotalProgressData():
    if updateProjectStartTime():
        currentTimestamp = int(time.time()) / 60 * 60
        
        returnData = {'startTime': projectStartTime}
        
        manifest = getManifestDataFromS3(['statistics'])
        if manifest != {}:
            returnData['totalSize'] = manifest['statistics']['totalObjectsSizeBytes']
            returnData['totalObjects'] = manifest['statistics']['totalObjects']
            returnData['successSize'] = 0
            returnData['successObjects'] = 0
        else:
            return {}
        
        success1MData = queryByAttr(1, dataType.onlySuccess, -1)
        for item in success1MData:
            returnData['successSize'] += int(item['SuccessObjectSize']['N'])
            returnData['successObjects'] += int(item['SuccessObjectNum']['N'])
            
        itemResponse = getItemByAttr(1, currentTimestamp - speedCollectDelay * 60, dataType.onlySuccessSize)
        if itemResponse == {}:
            returnData['estimateSpeed'] = int(getItemByAttr(1, projectStartTime, dataType.onlySuccessSize)['SuccessObjectSize']['N'])
        else:
            returnData['estimateSpeed'] = int(itemResponse['SuccessObjectSize']['N'])
            
        return returnData
    else:
        return {}
        
def returnDayTasksGraphData():
    currentDayTimestamp = int(time.time()) / 3600 / 24 * 3600 * 24
    
    # Batch get items in DynamoDB
    data = []
    for i in xrange(24):
        data.append({
            'TimeUnit': createDDBNumberFormat(60),
            'StartTime': createDDBNumberFormat(currentDayTimestamp + i * 3600)
        })
    response = batchGetItemsByArray(data)
    
    returnData = {'successObjects': [], 'failureObjects': []}
    for i in xrange(24):
        returnData['successObjects'].append(0)
        returnData['failureObjects'].append(0)
    for item in response:
        itemStartTime = int(item['StartTime']['N'])
        returnData['successObjects'][(itemStartTime - currentDayTimestamp) / 3600] = int(item['SuccessObjectNum']['N'])
        returnData['failureObjects'][(itemStartTime - currentDayTimestamp) / 3600] = int(item['FailedObjectNum']['N'])
        
    return returnData
    
def returnTasksGraphData():
    currentHourTimestamp = int(time.time()) / 3600 * 3600
    
    # Batch get items in DynamoDB
    data = []
    for i in xrange(60):
        data.append({
            'TimeUnit': createDDBNumberFormat(1),
            'StartTime': createDDBNumberFormat(currentHourTimestamp + i * 60)
        })
    response = batchGetItemsByArray(data)
    
    returnData = {'successObjects': [], 'failureObjects': []}
    for i in xrange(60):
        returnData['successObjects'].append(0)
        returnData['failureObjects'].append(0)
    for item in response:
        itemStartTime = int(item['StartTime']['N'])
        if item.has_key('SuccessObjectNum'):
            returnData['successObjects'][(itemStartTime - currentHourTimestamp) / 60] = int(item['SuccessObjectNum']['N'])
        if item.has_key('FailedObjectNum'):
            returnData['failureObjects'][(itemStartTime - currentHourTimestamp) / 60] = int(item['FailedObjectNum']['N'])
        
    return returnData

# Create test data in DDB.
def createTestDataToS3AndDDB():
    testHourLength = 5
    testHourLengthPredict = 3
    currentTimestamp = int(time.time())
    
    # Create manifest to S3
    testStartTimestamp = currentTimestamp - 3600 * testHourLength
    testStartTimeDay = time.strftime("%Y-%m-%d", time.gmtime(testStartTimestamp))
    manifestJson = {
        'sourceBucket': 'test-source-bucket',
        'destinationBucket': 'test-destination-bucket',
        'statistics': {
            'totalObjects': 2294893,
            'totalObjectsSizeGB': 22985
        },
        'fileFormat' : 'json'
    }
    
    fp = file('test.json', 'w')
    json.dump(manifestJson, fp)
    fp.close()
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('test.json', bucketName, objectKeyPathPrefix + testStartTimeDay + objectKeyPathSuffix)
    os.remove('test.json')
    
    # Create items in DynamoDB
    data = []
    for i in xrange(testHourLength + testHourLengthPredict):
        data.append({
            'PutRequest': {
                'Item': createDDBDataFormat(60, testStartTimestamp / 3600 * 3600 + i * 3600)
            }
        })
    batchWriteItemsByArray(data)
    
    for i in xrange(3 * testHourLengthPredict + 3 * testHourLength):
        data = []
        for j in xrange(20):
            data.append({
                'PutRequest': {
                    'Item': createDDBDataFormat(1, testStartTimestamp / 60 * 60 + (i * 20 + j) * 60)
                }
            })
        batchWriteItemsByArray(data)

# createTestDataToS3AndDDB()
