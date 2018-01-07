#!/bin/bash

# 下载测试Job
SAMPLE_JOB="s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json"
./TaskExecutor.py ${SAMPLE_JOB}

exit 0
