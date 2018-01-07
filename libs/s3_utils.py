#!/usr/bin/python
# -*- coding: utf8 -*-

import boto3
import gzip
import json
import os

print("boto3.version [{}]".format(boto3.__version__))

from utils import *

s3 = boto3.resource('s3')

s3_client = boto3.client('s3')

def s3_copy(src_bucket, dst_bucket, key):
    """
    Return Value:
    True:  Copy succesful
    False: Copy failed
    """
    #return True
    copy_source = {
        'Bucket': src_bucket,
        'Key': key
    }

    try:
        res = s3.meta.client.copy(copy_source, dst_bucket, key)
    except Exception,data:
        print "s3_copy error:", data
        return False

    return True

def save_json_to_s3_object(json_data, dst_bucket, dst_key):
    # with open('filename', 'rb') as data:
    object = s3.Object(dst_bucket, dst_key)
    #print(json.dumps(json_data))
    object.put(Body=json.dumps(json_data))
    #s3_client.upload_fileobj(json.dumps(json_data), dst_bucket, dst_key)

def s3_download(bucket_name=None, key=None):
    ''' s3_download(bucket_name=None, key=None):
        Desc: download s3 bucket
        Paras:
        Return : download filename
    '''
    if bucket_name is None or key is None:
        return
    
    obj = s3.Object(bucket_name, key)

    print('{}: length:{}'.format(obj, obj.content_length))

    dst='/tmp/{}.{}'.format(key.split('/')[-1], os.getpid())
    obj.download_file(dst)

    print("DST: {}".format(dst))

    return dst

    '''
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(key)
    with open('filename.json', 'wb') as data:
        obj.download_fileobj(data)

    print(data)
    '''

    #bucket = s3.Bucket('mybucket')
    #obj = bucket.Object('mykey')

def load_json_from_s3_object(bucket_name=None, key=None):
    filename = s3_download(bucket_name, key)
    data = load_json_from_file(filename)

    return data


def valid_inventory_data_file(filename, MD5checksum, size):

    print('Filename[{}] MD5checksum[{}] size[{}]'.format(filename, MD5checksum, size))
    
    return True

def download_s3_object_from_inventory(bucket_name=None, inventory_item=None):
    '''
        Return Values:
            'InvalidPara': Invalid Input paras
            'InvalidDataFile': Invalid inventory data file, wrong MD5checksum or size
    '''
    if bucket_name is None or inventory_item is None:
        return 'InvalidPara'

    filename = s3_download(bucket_name, inventory_item['key'])

    # FIXME : File validation
    if not valid_inventory_data_file(filename, inventory_item['MD5checksum'], inventory_item['size']):
        return 'InvalidDataFile'

    return filename


if __name__ == '__main__':
    
    A={'a':1, 'b':'bb'}
    save_json_to_s3_object(A, 'leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/job.json')

    data = load_json_from_s3_object('leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/job.json')

    #data = load_json_from_s3_object('leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/manifest.json')
    '''
    filename = s3_download('leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/manifest.json')
    print(filename)

    data = load_json_from_file(filename)

    '''
    print(data)
    #s3_download('leo-bjs-inventory-bucket', 'leodatacenter/leodatacenter/2017-12-25T08-00Z/manifest.checksum')
    #s3_download('leoaws', 'a.pdf')
