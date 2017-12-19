import boto3
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

# Get Temporary Certificate for service.
def getTemporaryCertificate(service):
    response = urllib2.urlopen(IAMSecurityUrl)
    response = urllib2.urlopen(IAMSecurityUrl + response.read())
    IAMTemporaryCertificate = json.loads(response.read())
    awsRegionID = metadataModel.getConfigurationValue('region_id')

    awsAK = IAMTemporaryCertificate['AccessKeyId']
    awsSK = IAMTemporaryCertificate['SecretAccessKey']
    awsToken = IAMTemporaryCertificate['Token']

    global client
    client = boto3.client(
        service,
        aws_access_key_id = awsAK,
        aws_secret_access_key = awsSK,
        region_name = awsRegionID,
        aws_session_token = awsToken
    )

# Batch write items.
# Input:    Array. All actions.
# Output:   Dir. Failure actions. 
def batchWriteItemsByArray(data):
    client = boto3.client('dynamodb')
    
    response = client.batch_write_item(
        RequestItems = {
            tableName: data
        }
    )
    
    return response['UnprocessedItems']

# Query DDB by attributes.
# Input:    timeUnit, dataRange(dataType.enum), limit.
# Output:   Query response.
def queryByAttr(timeUnit, dataRange, limit=100):
    client = boto3.client('dynamodb')
    
    response = client.query(
        TableName = tableName,
        Limit = limit,
        KeyConditionExpression = 'TimeUnit = :timeUnit',
        ExpressionAttributeValues = {
            ':timeUnit': createDDBNumberFormat(timeUnit)
        }
        # ProjectionExpression = dataTypeResponse[dataRange.value]
    )
    
    return response

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
def updateProjectStartTime():
    print queryByAttr(1, dataType.onlyStartTime, 1)

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
            'totalObjects': 2294893
        },
        'fileFormat' : 'json'
    }
    
    fp = file('test.json', 'w')
    json.dump(manifestJson, fp)
    fp.close()
    
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('test.json', bucketName, 'aws-collections/aws-collections-inventory/' + testStartTimeDay + 'T08-00Z/manifest.json')
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

updateProjectStartTime()
