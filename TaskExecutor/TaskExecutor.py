#!/usr/bin/python
# -*- coding: utf8 -*-

import sys
import random
from s3_utils import *
from sqs_utils import *
from s3_monitor_prepare import batchPutStatus

# TODO: https://docs.python.org/2/library/argparse.html#module-argparse

#./TaskExecutor.py s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json

def TaskExecutor_test(**kwargs):
    for name,value in kwargs.items():
        print("{}={}".format(name, value))

def check_queue(qurl, dead_queue):
    print('check_queue(%s, %s)'%(qurl, dead_queue))
    #return {'number':0}

    # FIXME TODO later from config
    response = sqs_client.receive_message(
        QueueUrl=qurl,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=1
    )

    #pprint(response)

    if 'Messages' not in response:
        return {'number':0}

    if not isinstance(response['Messages'], list):
        return {'number':0, 'reason':'Messages is not list'}

    ret={}
    ret['number']=0

    monitor_logs=[]

    for item in response['Messages']:
        if 'ReceiptHandle' not in item or 'Body' not in item:
            continue

        ReceiptHandle=item['ReceiptHandle']
        body=json.loads(item['Body'])

        #pprint(body)

        success=True

        for action in body:
            src_bucket=action['Bucket']
            key=action['Key']
            dst_bucket=action['dst_bucket']

            IsMultipartUploaded=False
            if 'IsMultipartUploaded' in action and action['IsMultipartUploaded']=='true':
                IsMultipartUploaded=True

            ReplicationStatus=0
            if 'ReplicationStatus' in action and action['ReplicationStatus']!='':
                ReplicationStatus=1

            item_log = {
                "LastModified": action['LastModifiedDate'],
                "ETag": action['ETag'],
                "StorageClass": action['StorageClass'],
                "Key": action['Key'],
                "Size": action['Size'],
                "IsMultipartUploaded": IsMultipartUploaded
            }

            if s3_copy(src_bucket, dst_bucket, key):
                print('s3_copy({0}, {1}, {2} ok'.format(src_bucket, dst_bucket, key))
                item_log['ReplicationStatus']=1
            else:
                print('s3_copy({0}, {1}, {2} failed'.format(src_bucket, dst_bucket, key))
                item_log['ReplicationStatus']=0
                # Just send the failed part to dead queue to verify, and make the job never failed
                #success=False
                send_msg_to_sqs(dead_queue, action)
                success=True
                break


            monitor_logs.append(item_log)

        if success is True:
            response = sqs_client.delete_message(
                QueueUrl=qurl,
                ReceiptHandle=ReceiptHandle
            )

            # Adding logs to ddb dynamodb
            #pprint(monitor_logs)
            batchPutStatus(monitor_logs)

        ret['number']+=len(body)

    return ret


def TaskExecutor(job_position=None):
    if job_position is None:
        print("None job. Quit")
        return None

    # Download job.json 
    print("Download "+job_position)
    #./TaskExecutor.py s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json
    bucket_name=job_position.split('/')[2]
    key_name='/'.join(job_position.split('/')[3:])

    print(bucket_name)
    print(key_name)

    data = load_json_from_s3_object(bucket_name, key_name)

    if 'job_info' not in data:
        print("Invalid Job")
        return None

    job_info=data['job_info']

    pprint(job_info)

    # Start Copy Job
    qurl_endpoint=job_info['queue_url_prefix']
    dead_queue='{}-dead-letter'.format(qurl_endpoint)
    q_number=job_info['queue_num']

    #response=check_queue('{0}-{1}'.format(qurl_endpoint, q_number), dead_queue)
    #pprint(response)
    #sys.exit(0)

    # Random start point and infinite loop to avoid hot spot scan
    check_point=random.randint(1, q_number)
    while True:
        if check_point > q_number:
            check_point=1
        response = check_queue('%s-%03d'%(qurl_endpoint, check_point), dead_queue)

        print("Process {} files".format(response['number']))
        check_point+=1

    sys.exit(0)

def main():
    TaskExecutor_test(a=1,b='11')

def usage(basename):
    print("Usage:")
    print("\t{} [Job_Position]".format(basename))


if __name__ == '__main__':
    basename=sys.argv[0].split('/')[-1]

    # Check
    argc = len(sys.argv)

    if argc < 2:
        print("Missing para")
        usage(basename)
        sys.exit(1)

    TaskExecutor(sys.argv[1])
