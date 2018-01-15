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
from sys import argv

SYS_VALUED_TYPES = ['-s3-bucket-name', '-ddb-table-name', '-project-host-region', '-frontend-speed-delay', '-iam-profile', '-ec2-key', '-s3-manifest-path']
SYS_HELP_TYPES = ['-h', '--help']
SYS_PARAMETERS_SET = {
    '-s3-bucket-name': 's3BucketName',
    '-ddb-table-name': 'ddbTableName',
    '-project-host-region': 'hostRegion',
    '-frontend-speed-delay': 'speedDelay',
    '-iam-profile': 'IAMProfile',
    '-ec2-key': 'EC2Key',
    '-s3-manifest-path': 'S3ManifestPath'
}

S3ManifestPath = ''
s3BucketName = 's3-big-data-sync'
ddbTableName = 's3cross_stat'
hostRegion = 'us-east-1'
IAMProfile = 'default'
EC2Key = ''
speedDelay = 10

# Create Random String.
# Input:    String length.
# Output:   String random string.
def createRandomString(length):
    letters = string.ascii_lowercase
    
    return ''.join(random.choice(letters) for i in range(length))
    
# Load File As String.
# Input:    String file(file path).
# Output:   String decoded file contents.
def loadCFileAString(file):
    if os.path.exists(file):
        with open(file, 'r') as f:
            try:
                fbuffer = f.read().decode("utf-8")
                f.close()
                return fbuffer
            except:
                return ""
    return ""

# Get Location Name By Region ID.
# Input:    String region.
# Output:   String location.
def getLocationByRegion(region):
    location = region
    if region == 'us-east-1':
        location = ''
        
    return location

# Create S3 Bucket (Will add random suffix if bucket name already exist.)
# Input:    String name. String region. String profile.
# Output:   String bucket name.
def createS3Bucket(name, region, profile):
    session = boto3.Session(profile_name = profile)
    
    location = getLocationByRegion(region)
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

# Put S3 Bucket As Website.
# Input:    String bucket name. String profile.
# Output:   Void. (Status success by default)
def putS3BucketWebsite(bucketName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('s3')
    
    client.put_bucket_website(
        Bucket = bucketName,
        WebsiteConfiguration = {
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        }
    )

# Upload Local File To S3 Bucket.
# Input:    String source path. String target path. String bucket name. String profile.
# Output:   Upload response. More details: http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object.
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

# Check If The CloudFormation Stack Is Exist.
# Input:    String stack name. String profile.
# Output:   Boolean.
def isStackExist(stackName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation') 
    
    try:
        client.describe_stacks(
            StackName = stackName
        ) 
        return True 
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AmazonCloudFormationException':
            return False  
    return False

# Make S3 Bucket File Public.
# Input:    String file path. String bucket name. String profile.
# Output:   Void.
def makeS3FilePublic(filePath, bucketName, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('s3')
    
    client.put_object_acl(
        ACL = 'public-read',
        Bucket = bucketName,
        Key = filePath
    )

# Get CloudFormation Stack Status.
# Input:    String stack ID. String profile.
# Output:   String stack status 'CREATE_IN_PROGRESS'|'CREATE_FAILED'|'CREATE_COMPLETE'|'ROLLBACK_IN_PROGRESS'|'ROLLBACK_FAILED'|'ROLLBACK_COMPLETE'|'DELETE_IN_PROGRESS'|'DELETE_FAILED'|'DELETE_COMPLETE'|'UPDATE_IN_PROGRESS'|'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS'|'UPDATE_COMPLETE'|'UPDATE_ROLLBACK_IN_PROGRESS'|'UPDATE_ROLLBACK_FAILED'|'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS'|'UPDATE_ROLLBACK_COMPLETE'|'REVIEW_IN_PROGRESS'
def getStackStatus(stackID, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation')
    
    response = client.describe_stacks(
        StackName = stackID
    )
    
    return response['Stacks'][0]['StackStatus']

# Get CloudFormation Stack Output(s).
# Input:    String stack ID. String profile.
# Output:   Array stack Outputs.
def getStackOutput(stackID, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation')
    
    response = client.describe_stacks(
        StackName = stackID
    )
    
    return response['Stacks'][0]['Outputs']

# Create Back-End Server Sources by CloudFormation.
# Input:    String package path (Back-End tar package). String region. String profile.
# Output:   String CloudFormation stack ID.
def createServerSources(packagePath, keyName, region, profile):
    session = boto3.Session(profile_name = profile)
    client = session.client('cloudformation')
    
    fileBody = loadCFileAString("./ServerBackEnd.json")
    stackName = 'S3-Big-Data-Sync-Back-End-Server'
    location = getLocationByRegion(region)
    if location != '':
        location = '-' + location
        
    endpointSuffix = '/'
    if (region == 'cn-north-1') or (region == 'cn-northwest-1'):
        location = '.' + region
        endpointSuffix = '.cn/'
        userData = '#!/bin/sh \npip install boto3 -i http://pypi.douban.com/simple \npip install enum -i http://pypi.douban.com/simple \nwget https://s3' + location + '.amazonaws.com' + endpointSuffix + s3BucketName + '/' + packagePath + ' \nmkdir /var/s3bigdatasync \nmv backend.tar.gz /var/s3bigdatasync/backend.tar.gz \ncd /var/s3bigdatasync/ \n tar -zxvf backend.tar.gz \ncd BackEnd \nchmod u+x keep_alive.sh \n./keep_alive.sh >> server.log \necho "/var/s3bigdatasync/BackEnd/keep_alive.sh >> server.log" >> /etc/rc.d/rc.local'
    else:
        userData = '#!/bin/sh \npip install boto3 \npip install enum \nwget https://s3' + location + '.amazonaws.com' + endpointSuffix + s3BucketName + '/' + packagePath + ' \nmkdir /var/s3bigdatasync \nmv backend.tar.gz /var/s3bigdatasync/backend.tar.gz \ncd /var/s3bigdatasync/ \n tar -zxvf backend.tar.gz \ncd BackEnd \nchmod u+x keep_alive.sh \n./keep_alive.sh >> server.log \necho "/var/s3bigdatasync/BackEnd/keep_alive.sh >> server.log" >> /etc/rc.d/rc.local \n'
    
    if isStackExist(stackName, profile):
        try:
            response = client.update_stack(
                StackName = stackName,
                TemplateBody = fileBody,
                Parameters = [
                    {
                        'ParameterKey': 'KeyName',
                        'ParameterValue': keyName
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
            Parameters = [
                {
                    'ParameterKey': 'KeyName',
                    'ParameterValue': keyName
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

# Get All File Name And File Path From Folder Path.
# Input:    String root path.
# Output:   Array folders file data [{'fileName': fileName, 'filePath': filePath}].
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

# Create And Upload Back-End Package To S3 Bucket.
# Input:    String profile.
# Output:   String package path (in S3).
def createAndUploadBackEndPackage(profile):
    fp = file('./BackEnd/metadata.json', 'w')
    json.dump({
        'project_start_time': 0,
        "bucket_name": s3BucketName,
        "table_name": ddbTableName,
        "region_id": hostRegion,
        "speed_collect_delay": speedDelay,
        "manifest_path": S3ManifestPath
    }, fp)
    fp.close()
    
    (status, output) = commands.getstatusoutput('tar -zcvf ./backend.tar.gz ./BackEnd')
    if status == 0:
        uploadFileToS3('./backend.tar.gz', 'tmp/backend.tar.gz', s3BucketName, profile)
        # makeS3FilePublic('tmp/backend.tar.gz', s3BucketName, profile)
        commands.getstatusoutput('rm backend.tar.gz')
        commands.getstatusoutput('rm ./BackEnd/metadata.json')
        
    return 'tmp/backend.tar.gz'
    
# Create Front End In S3.
# Input:    String profile.
# Output:   Void.
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

# Create Back End In EC2.
# Input:    String profile.
# Output:   Void.
def createBackEndInEC2(profile='default', key=''):
    packagePath = createAndUploadBackEndPackage(profile)
    stackID = createServerSources(packagePath, key, hostRegion, profile)
    
    print 'Stack start, please wait for about 5 mins.'
    time.sleep(180)
    stackOutput = []
    backEndEIP = ''
    
    while True:
        stackStatus = getStackStatus(stackID, profile)
        if (stackStatus == 'CREATE_IN_PROGRESS'):
            print 'Stack still in progress.'
            time.sleep(60)
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
    fp.write('const BytesConverterNumber = 1000;const APIEndpoint = "http://' + backEndEIP + ':80/"\n');
    fp.close()
    
    uploadFileToS3('./configuration.js', 'assets/js/configuration.js', s3BucketName, profile)
    # makeS3FilePublic('assets/js/configuration.js', s3BucketName, profile)
    commands.getstatusoutput('rm configuration.js')

def main():
    createFrontEndInS3(IAMProfile)
    createBackEndInEC2(IAMProfile, EC2Key)
    
    print "UICenter is installed successfully."
    location = getLocationByRegion(hostRegion)
    if location != '':
        location = '-' + location
    if (hostRegion == 'cn-north-1') or (hostRegion == 'cn-northwest-1'):
        print "WebsiteURL: http://" + s3BucketName + ".s3-website." + hostRegion + ".amazonaws.com.cn/"
    else:
        print "WebsiteURL: http://" + s3BucketName + ".s3-website" + location + ".amazonaws.com/"

def help_messages():
    print "Message: please read README.md file for more details. "

def error_with_argv(value):
    print "Error: Wrong sys argv input between value: " + value
    
def error_messages():
    print "Error: Invalid sys argv input."

if __name__ == "__main__":
    if (len(argv) == 2) and (argv[1] in SYS_HELP_TYPES):
        help_messages()
    elif (len(argv) == 1):
        main()
    elif (len(argv) % 2 == 1):
        flag = ''
        isError = False
        for i in xrange(1, len(argv)):
            if (i % 2 == 0) and (flag == ''):
                error_with_argv(argv[i-1])
                isError = True
                break
            elif (i % 2 == 1) and (argv[i] in SYS_VALUED_TYPES):
                flag = argv[i]
            elif (i % 2 == 1):
                error_with_argv(argv[i])
                isError = True
                break
            else:
                locals()[SYS_PARAMETERS_SET[flag]] = argv[i]
                flag = ''
        
        if not isError:
            main()
    else:
        error_messages()
