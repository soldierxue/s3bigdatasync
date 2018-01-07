#!/bin/bash


pip-2.7 install --proxy http://127.0.0.1:8080/ boto3==1.5.7 enum

python install.py -project-host-region cn-north-1 -ec2-key poc

exit 0
