#!/bin/python
# coding=utf-8

import boto3
import botocore
import random
import string
import commands
import json
import os
import time

AMI = {'us-east-1': 'ami-55ef662f'}
s3BucketName = 's3-big-data-sync'
ddbTableName = 's3cross_stat'
hostRegion = 'us-east-1'
speedDelay = 10

# Create Random String
# Input:    String Length.
# Output:   String.
def createRandomString(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

# Create S3 Bucket (Will add random suffix if bucket name already exist.)
# Input:    Name String. Region String.
# Output:   Bucket Location.
def createS3Bucket(name, region, profile):
    session = boto3.Session(profile_name = profile)
    
    if region != 'us-east-1':
        location = region
    else:
        location = 'EU'
    suffix = ''
    
    client = session.client('s3')
    
    while True:
        try:
            response = client.create_bucket(
                ACL = 'public-read',
                Bucket = name + suffix,
                CreateBucketConfiguration = {
                    'LocationConstraint': location
                }
            )
        except botocore.exceptions.ClientError:
            suffix = '-' + createRandomString(14)
            continue
        else:
            return name + suffix

def putS3BucketWebsite(bucketName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('s3')
    
    response = client.put_bucket_website(
        Bucket = bucketName,
        WebsiteConfiguration = {
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        }
    )

def uploadFileToS3(sourcePath, targetPath, bucketName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('s3')
    
    fp = file(sourcePath, 'r')
    contentType = 'binary/octet-stream'
    if (sourcePath[-5:] == '.html'):
        contentType = 'text/html'
    
    print 'Upload file: ' + sourcePath + ' to S3 bucket: ' + bucketName + ' ...'
    response = client.put_object(
        ACL = 'public-read',
        Body = fp,
        Bucket = bucketName,
        Key = targetPath,
        ContentType = contentType
    )
    
    return response

def loadCFileAString(file):
    if os.path.exists(file):
        with open(file, 'r') as f:
            try:
                fbuffer = f.read().decode("utf-8")
                #logger.info("File content#"+json.dumps(fbuffer))
                f.close()
                return fbuffer
            except:
                return ""
    return ""

def isStackExist(stackName, profile):
    session = boto3.Session(profile_name = profile)
    cfClient = session.client('cloudformation') 
    try:
        response = cfClient.describe_stacks(
            StackName = stackName
        ) 
        return True 
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AmazonCloudFormationException':
            return False  
    return False

def makeS3FilePublic(packagePath, bucketName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('s3')
    
    response = client.put_object_acl(
        ACL = 'public-read',
        Bucket = bucketName,
        Key = packagePath
    )

def getStackStatus(stackID, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation')
    
    response = client.describe_stacks(
        StackName = stackID
    )
    
    return response['Stacks'][0]['StackStatus']

def getStackOutput(stackID, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation')
    
    response = client.describe_stacks(
        StackName = stackID
    )
    
    return response['Stacks'][0]['Outputs']

def createServerSources(packagePath, region, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation') 
    
    fileBody = loadCFileAString("./ServerBackEnd.json")
    stackName = 'Server-Back-End'
    userData = '#!/bin/sh \npip install boto3 \npip install enum \nwget https://s3-' + 'eu-west-1' + '.amazonaws.com/' + s3BucketName + '/' + packagePath + ' \nmkdir /var/s3bigdatasync \nmv backend.tar.gz /var/s3bigdatasync/backend.tar.gz \ncd /var/s3bigdatasync/ \n tar -zxvf backend.tar.gz \ncd BackEnd \n python server.py \n'
    
    if isStackExist(stackName, profile):
        try:
            response = client.update_stack(
                StackName = stackName,
                TemplateBody = fileBody,
                Parameters=[
                    {
                        'ParameterKey': 'KeyName',
                        'ParameterValue': 'self-USE1'
                    },
                    {
                        'ParameterKey': 'UserData',
                        'ParameterValue': userData
                    }
                ],
                Capabilities = [
                    'CAPABILITY_IAM'
                ],
                OnFailure = 'ROLLBACK',
                EnableTerminationProtection = False
            )
            
            return response['StackId']
        except:
            print "Already there."
    else:
        response = client.create_stack(
            StackName = stackName,
            TemplateBody = fileBody,
            Parameters=[
                {
                    'ParameterKey': 'KeyName',
                    'ParameterValue': 'self-USE1'
                },
                {
                    'ParameterKey': 'UserData',
                    'ParameterValue': userData
                }
            ],
            Capabilities = [
                'CAPABILITY_IAM'
            ],
            OnFailure = 'ROLLBACK'
        )
        
        return response['StackId']

def getFileNameAndFilePathFrom(rootPath):
    if rootPath[-1] != '/':
        rootPath += '/'
    fileData = []
    
    (status, output) = commands.getstatusoutput('ls ' + rootPath)
    if status == 0:
        files = output.split('\n')
    else:
        files = []
    
    for item in files:
        if item.find('.') != -1:
            fileData.append({'fileName': item, 'filePath': rootPath + item})
        else:
            fileData += getFileNameAndFilePathFrom(rootPath + item + '/')
    
    return fileData

def createAndUploadBackEndPackage(profile):
    fp = file('./BackEnd/metadata.json', 'w')
    json.dump({
        'project_start_time': 0,
        "bucket_name": s3BucketName,
        "table_name": ddbTableName,
        "region_id": hostRegion,
        "speed_collect_delay": speedDelay
    }, fp)
    fp.close()
    
    (status, output) = commands.getstatusoutput('tar -zcvf ./backend.tar.gz ./BackEnd')
    if status == 0:
        uploadFileToS3('./backend.tar.gz', 'tmp/backend.tar.gz', s3BucketName, profile)
        # makeS3FilePublic('tmp/backend.tar.gz', s3BucketName, profile)
        commands.getstatusoutput('rm backend.tar.gz')
        
    return 'tmp/backend.tar.gz'    
    
def createFrontEndInS3(profile='default'):
    global s3BucketName
    bucketName = createS3Bucket(s3BucketName, hostRegion, profile)
    s3BucketName = bucketName
    putS3BucketWebsite(s3BucketName, profile)
    
    fileData = getFileNameAndFilePathFrom('./FrontEnd')
    
    for item in fileData:
        sourcePath = item['filePath']
        targetPath = sourcePath[11:]
        
        uploadFileToS3(sourcePath, targetPath, bucketName, profile)
        # makeS3FilePublic(targetPath, bucketName, profile)

def createBackEndInEC2(profile='default'):
    packagePath = createAndUploadBackEndPackage(profile)
    stackID = createServerSources(packagePath, hostRegion, profile)
    
    print 'Stack start, please wait for about 5 mins.'
    time.sleep(180)
    stackOutput = []
    backEndEIP = ''
    
    while True:
        stackStatus = getStackStatus(stackID, profile)
        if (stackStatus == 'CREATE_IN_PROGRESS'):
            time.sleep(60)
            print 'Stack still in progress.'
            continue
        elif (stackStatus == 'CREATE_COMPLETE'):
            print 'Stack has been done.'
            stackOutput = getStackOutput(stackID, profile)
            break
        else:
            break
        
    for item in stackOutput:
        if (item['OutputKey'] == 'InstanceIPAddress'):
            backEndEIP = item['OutputValue']
    
    fp = file('./configuration.js', 'w')
    fp.write('const BytesConverterNumber = 1000;const APIEndpoint = "http://' + backEndEIP + ':8000/"\n');
    fp.close()
    
    uploadFileToS3('./configuration.js', 'assets/js/configuration.js', s3BucketName, profile)
    # makeS3FilePublic('assets/js/configuration.js', s3BucketName, profile)

def main():
    createFrontEndInS3()
    createBackEndInEC2()
    
    print "UICenter is installed successfully."
    print "WebsiteURL: http://" + s3BucketName + ".s3-website-" + "eu-west-1" + ".amazonaws.com/"

if __name__ == "__main__":
    main()
