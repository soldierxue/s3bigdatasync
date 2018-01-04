#!/usr/bin/python
# -*- coding: utf8 -*-


INVENTORY_BUCKET="leo-bjs-inventory-bucket"
INVENTORY_KEY_DIR="leodatacenter/leodatacenter/2017-12-30T08-00Z/"


from pprint import pprint
from s3_utils import *



# Inventory process


def parse_inventory_data_file(data_file):

    msg_body=[]

    with gzip.open(data_file, 'rb') as f:
        for line in f.readlines():
            sections = line.split(',')

            if len(sections) < 7:
                return 1

            src_bucket = sections[0].split('"')[1]
            key = sections[1].split('"')[1]

            #print src_bucket,key,dst_bucket
            print src_bucket,key

            #msg_body.append({'src_bucket':src_bucket, 'key':key, 'dst_bucket':dst_bucket})


    '''
            if len(msg_body) == MESSAGE_BODY_NUM:
                qurl='{0}-{1}'.format(QUEUE_ENDPOINT, random.randint(1, QUEUE_NUM))
                send_msg_to_sqs(qurl, msg_body)
                msg_body=[]

    if len(msg_body) > 0:
        qurl='{0}-{1}'.format(QUEUE_ENDPOINT, random.randint(1, QUEUE_NUM))
        send_msg_to_sqs(qurl, msg_body)
    '''

def downlad_bucket_manifest():
    ''' test FIXME '''
    # leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/

    data = load_json_from_s3_object(INVENTORY_BUCKET, INVENTORY_KEY_DIR+'manifest.json')
    #print(data)
    return data

def main():
    print("main")

    # 1. Get Source information
    manifest = downlad_bucket_manifest()

    #pprint(manifest)
    if 'files' in manifest:
        for item in manifest['files']:
            pprint(item)
            download_filename = download_s3_object_from_inventory(INVENTORY_BUCKET, item)

            parse_inventory_data_file(download_filename)
            
if __name__ == '__main__':
    main()
