# 模块  Module 0
    ListProducer

# 作者
  Leo

# 功能
* Generate Bucket Objects List
* Input:
    Bucket
* Output:
    Object List(json)

# Object Data Schema:
```Bash
{
    "TotalSizeMB": 1000,
        "TotalCount": 10384,
        "Bucket": "bname"
            [
            {
                "LastModified": "2016-06-22T09:54:26.000Z",
                "ETag": "\"6e8c28243c55edd44cc8a796a332f1c2\"",
                "StorageClass": "STANDARD",
                "Key": "qwikLabs/屏幕快照 2016-03-17 下午4.24.34.png",
                "Size": 85697,
                "IsMultipartUploaded":True|False,
                "ReplicationStatus":""
            }
            ]
}

```

# Files
* 


# 安装过程
  默认安装 100 个默认region内的s3sync-worker-001 ~ s3sync-worker-100 的SQS队列，包含死信队列s3sync-worker-dead-letter
```Bash
./install.sh
```
# 运行过程
```Bash
./start.sh
....
=== Job description is at s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json
```
最后生成任务列表 job.json, 后续 TaskExcutor,TaskMonitor,UICenter 使用


```Bash
暂时修改 ListProducer.py 中以下部分, 完成Job定义
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
```

# TODO
 - [ ] 增加任务描述输入: ./ListProducer.py [src_bucket] [dst_bucket] [src_key_prefix] [dst_key_prefix]
 - [ ] 基于bucket名称获取inventory信息
 - [ ] 自动扫描获取最新inventory信息



