import boto3
import urllib2
import json
import metadataModel
from enum import Enum
import time

class dataType(Enum):
    allValid = 0
    timesetNoNeed = 1
    onlySuccess = 2
    onlySuccessSize = 3

IAMSecurityUrl = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
validColumn = ["Timeset", "Objects", "Size", "FailureObjects", "FailureSize"]
dataTypeResponse = [
    "Timeset, Objects, Size, FailureObjects, FailureSize",
    "Objects, Size, FailureObjects, FailureSize",
    "Objects, Size",
    "Size"
]
startTime = metadataModel.getConfigurationValue('project_start_time')
tableName = metadataModel.getConfigurationValue('table_name')

def getTemporaryCertificate():
    response = urllib2.urlopen(IAMSecurityUrl)
    response = urllib2.urlopen(IAMSecurityUrl + response.read())
    IAMTemporaryCertificate = json.loads(response.read())
    awsRegionID = metadataModel.getConfigurationValue('region_id')

    awsAK = IAMTemporaryCertificate['AccessKeyId']
    awsSK = IAMTemporaryCertificate['SecretAccessKey']
    awsToken = IAMTemporaryCertificate['Token']

    global client
    client = boto3.client(
        'dynamodb',
        aws_access_key_id = awsAK,
        aws_secret_access_key = awsSK,
        region_name = awsRegionID,
        aws_session_token = awsToken
    )

/
def updateStartTime():
    originStartTime = metadataModel.getConfigurationValue('origin_start_time')
    originStartDayTimestamp = time.mktime(time.strptime(originStartTime[:-6],"%Y-%m-%d"))
    
    # Query By Days.
    for i in xrange(10):
        originStartTimeDayTimestamp = originStartDayTimestamp + 3600 * 24 * i
        originStartTimeDay = time.strftime("%Y-%m-%d", time.gmtime(originStartTimeDayTimestamp))
        
        print originStartTimeDay
    
    return False

def isStarted():
    if startTime != "":
        return True
    
    result = getItemByTimesetValue("TargetAll", dataType.onlySuccessSize)
    
    if result.has_key('Size'):
        if result['Size'] > 0:
            updateStartTime()
            return True
    
    return False

def queryItemByContainInTimeset(contain):
    if not isset("client"):
        getTemporaryCertificate()
    
    response = client.query(
        KeyConditionExpression = "Timeset  ```` :timeset",
        KeyConditionExpression = "Timeset ```` :timeset",
        ExpressionAttributeValues = {
            ":timeset": {"S": contain}
        }
        # ProjectionExpression = dataTypeResponse[dataRange.value]
    )
    
    print response

def getItemByTimesetValue(value, dataRange):
    if not isset("client"):
        getTemporaryCertificate()
    
    response = client.get_item(
        TableName = tableName,
        Key = {
            'Timeset': {
                'S': value
            }
        },
        ProjectionExpression = dataTypeResponse[dataRange.value]
    )
    
    if response.has_key('Item'):
        return cutDirFromDDB(response['Item'])
    else:
        return {}

def getFirstCallItems():
    if isStarted():
        
        keysData = []
        keysData.append(createDirForDDB('Timeset', 'S', "SourceAll"))
    else:
        return {}

def isset(v): 
   try : 
     type (eval(v)) 
   except : 
     return  0
   else : 
     return  1
     
def cutDirFromDDB(data):
    cutData = {}
    
    for item in data:
        for key in data[item]:
            if key == 'N':
                cutData[item] = int(data[item][key])
            else:
                cutData[item] = data[item][key]
    
    return cutData
    
def createDirForDDB(key, valueType, value):
    return { key: { valueType: value } }

# print isStarted()
# updateStartTime()
queryItemByContainInTimeset("Sour")
