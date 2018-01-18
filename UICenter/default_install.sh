#!/bin/bash


pip install boto3==1.5.7 enum

python install.py -r cn-north-1 -k self-BJS -m leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json -p bjs -s ec2-backend -b s3-big-data-sync-lszkfrmyuswvyj

exit 0
