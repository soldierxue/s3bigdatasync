#!/usr/bin/python
# -*- coding: utf8 -*-

import sys
from s3_utils import *

# TODO: https://docs.python.org/2/library/argparse.html#module-argparse

#./TaskExecutor.py s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json


def TaskExecutor_test(**kwargs):
    for name,value in kwargs.items():
        print("{}={}".format(name, value))

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

    job_info = load_json_from_s3_object(bucket_name, key_name)

    pprint(job_info)





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


