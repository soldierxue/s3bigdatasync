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

