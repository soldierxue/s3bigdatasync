# 模块  Module III 
    TaskMonitor

# 作者
  Jason

# 功能
    Monitor Task Progress
    Input:
        Objects / Object Transfer Status
    Output:
        Object Transfer Stat

# 实现模块说明 - s3_monitor_prepare.py

本模块主要实现两个功能：
- 利用 CloudFormation 模板准备和创建监控所需要的 DynamoDB 表和相应的 GSI
```python
# Inputs:
#   profile # required, the IAM role profile name, default value is 'default'
#   monitorTableName # optional, if you modify the table name during creating stack
#   statTableName # optional, if you modify the table name during creating stack
def prepareDDBResourceByCF(profile='default',monitorTableName="s3cross_monitor",statTableName="s3cross_stat")
```
- 提供外部接口，保存监控信息
```python
# Inputs:
#      monitorItems =[
#         {  
#           "ReplicationStatus":0|1 #required
#           ,"Key":"" #required
#           ,"Size": 85697 # optional, better includes for total size summary
#           ,"LastModified": "2016-06-22T09:54:26.000Z" # optional
#           ,"ETag": "6e8c28243c55edd44cc8a796a332f1c2 # optional
#           ,"StorageClass": "STANDARD" # optional
#           ,"IsMultipartUploaded":True|False # optional
#         }
#      ]
#       profile # the IAM role profile name, default value is 'default'
#       tableName # the table name of the monitor table, default is "s3cross_monitor"  
# 
def batchPutStatus(monitorItems,profile='default',tableName="s3cross_monitor")
```
        


