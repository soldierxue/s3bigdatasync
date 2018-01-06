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
    'src_bucket': 'leodatacenter',
    'dst_bucket': 'leo-zhy-datacenter'
}

object_key_list= ['totalObjects', 'totalObjectsSub1GB', 'totalObjectsSub5GB', 'totalObjectsSub10GB', 'totalObjectsSub50GB', 'totalObjectsSub100GB', 'totalObjectsSub1TB', 'totalObjectsSub5TB']
# Inventory process

def parse_inventory_data_file(data_file, job_info=None):
    msg_body=[]

    stat={}

    for key in object_key_list:
        stat[key]=0

    with gzip.open(data_file, 'rb') as f:
        for line in f.readlines():
            sections = line.split(',')

            if len(sections) < 7:
                return 1

            src_bucket = sections[0].split('"')[1]
            key = sections[1].split('"')[1]
            size = int(sections[2].split('"')[1])

            item = {
                'Bucket': src_bucket,
                'Key'   : key,
                'Size'  : size,
                'LastModifiedDate': sections[3].split('"')[1],
                'ETag': sections[4].split('"')[1],
                'StorageClass': sections[5].split('"')[1],
                'IsMultipartUploaded': sections[6].split('"')[1],
                'ReplicationStatus': sections[7].split('"')[1],
                'dst_bucket':job_info['dst_bucket']
            }

            # Do Stat
            stat['totalObjects'] += 1

            if size > 5*1000*1000*1000:
                pass
                print(">5T...s3://%s/%s"%(src_bucket, key))
            elif size > 1000*1000*1000:
                stat['totalObjectsSub5TB'] += 1
            elif size > 100*1000*1000:
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1
            elif size > 50*1000*1000:
                stat['totalObjectsSub100GB'] += 1
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1
            elif size > 10*1000*1000:
                stat['totalObjectsSub50GB'] += 1
                stat['totalObjectsSub100GB'] += 1
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1
            elif size > 5*1000*1000:
                stat['totalObjectsSub10GB'] += 1
                stat['totalObjectsSub50GB'] += 1
                stat['totalObjectsSub100GB'] += 1
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1
            elif size > 1000*1000:
                stat['totalObjectsSub5GB'] += 1
                stat['totalObjectsSub10GB'] += 1
                stat['totalObjectsSub50GB'] += 1
                stat['totalObjectsSub100GB'] += 1
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1
            else:
                stat['totalObjectsSub1GB'] += 1
                stat['totalObjectsSub5GB'] += 1
                stat['totalObjectsSub10GB'] += 1
                stat['totalObjectsSub50GB'] += 1
                stat['totalObjectsSub100GB'] += 1
                stat['totalObjectsSub1TB'] += 1
                stat['totalObjectsSub5TB'] += 1

            # Bucket, Key, Size, LastModifiedDate, ETag, StorageClass, IsMultipartUploaded, ReplicationStatus
            # "leodatacenter","AWS+SKO+2015/2015+AWS+Sales+Kickoff+Agenda.pdf","360461","2015-08-29T06:56:01.000Z","eef4ce1bc8503f5ee6c98553ccb9f496","STANDARD","false",""

            #print src_bucket,key,dst_bucket
            #print src_bucket,key

            #msg_body.append({'src_bucket':src_bucket, 'key':key, 'dst_bucket':job_info['dst_bucket']})
            msg_body.append(item)

            if len(msg_body) == job_info['message_body_max_num']:
                qurl='%s-%03d'%(job_info['queue_url_prefix'], random.randint(1, job_info['queue_num']))
                send_msg_to_sqs(qurl, msg_body)
                msg_body=[]

    if len(msg_body) > 0:
        qurl='%s-%03d'%(job_info['queue_url_prefix'], random.randint(1, job_info['queue_num']))
        send_msg_to_sqs(qurl, msg_body)

    pprint(stat)

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

    manifest['statistics'] = {}
    for key in object_key_list:
        manifest['statistics'][key]=0

    #pprint(manifest)
    if 'files' in manifest:
        for item in manifest['files']:
            pprint(item)
            download_filename = download_s3_object_from_inventory(job_info['inventory_bucket'], item)

            stat = parse_inventory_data_file(download_filename, job_info)
            #print(stat)

            for key in object_key_list:
                manifest['statistics'][key] += stat[key]

    print("============")
    manifest['job_info'] = job_info

    pprint(manifest)

    #save back manifest 
    save_json_to_s3_object(manifest, job_info['inventory_bucket'], job_info['inventory_manifest_dir']+'job.json')

    data = load_json_from_s3_object(job_info['inventory_bucket'], job_info['inventory_manifest_dir']+'job.json')

    pprint(data)


if __name__ == '__main__':
    main()
