#!/bin/bash


pip-2.7 install --proxy http://127.0.0.1:8080/ boto3==1.5.7 enum

python install.py -project-host-region cn-north-1 -ec2-key poc -s3-manifest-path leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json

exit 0
