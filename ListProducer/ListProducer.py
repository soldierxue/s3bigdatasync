#!/usr/bin/python
# -*- coding: utf8 -*-

from pprint import pprint
from s3_utils import *
from sqs_utils import *
import random

# 0. Job
job_info = {
    'src_type': 's3_inventory',
    'inventory_bucket': 'leo-bjs-inventory-bucket',
    'inventory_manifest_dir': 'leodatacenter/leodatacenter/2017-12-30T08-00Z/',
    'queue_url_prefix': 'https://sqs.cn-north-1.amazonaws.com.cn/358620020600/s3sync-worker',
    'queue_num': 1, # How many SQS to use
    'message_body_max_num': 100, # How many objects in one message body
    'dst_bucket': 'leo-zhy-datacenter'
}

# Inventory process

def parse_inventory_data_file(data_file, job_info=None):
    msg_body=[]

    number=0

    with gzip.open(data_file, 'rb') as f:
        for line in f.readlines():
            number+=1
            sections = line.split(',')

            if len(sections) < 7:
                return 1

            src_bucket = sections[0].split('"')[1]
            key = sections[1].split('"')[1]

            #print src_bucket,key,dst_bucket
            #print src_bucket,key

            msg_body.append({'src_bucket':src_bucket, 'key':key, 'dst_bucket':job_info['dst_bucket']})

            if len(msg_body) == job_info['message_body_max_num']:
                qurl='%s-%03d'%(job_info['queue_url_prefix'], random.randint(1, job_info['queue_num']))
                send_msg_to_sqs(qurl, msg_body)
                msg_body=[]

    if len(msg_body) > 0:
        qurl='%s-%03d'%(job_info['queue_url_prefix'], random.randint(1, job_info['queue_num']))
        send_msg_to_sqs(qurl, msg_body)

    stat = {
        'total_number': number
    }
    return stat 

def downlad_bucket_manifest():
    ''' test FIXME '''
    # leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/
    data = load_json_from_s3_object(job_info['inventory_bucket'], job_info['inventory_manifest_dir']+'manifest.json')
    #print(data)
    return data

def main():
    # 1. Get Source information
    manifest = downlad_bucket_manifest()

    #pprint(manifest)
    if 'files' in manifest:
        for item in manifest['files']:
            pprint(item)
            download_filename = download_s3_object_from_inventory(job_info['inventory_bucket'], item)

            stat = parse_inventory_data_file(download_filename, job_info)
            print(stat)
            
if __name__ == '__main__':
    main()
