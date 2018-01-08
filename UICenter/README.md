# 模块  Module V
UICenter

# 作者
David

# 功能
    User Interface
    Input:
        Configuration value
        Data
    Output:
        Web UI

# 执行方式
```sh
python ${Project_Path}/UICenter/install.py [Option Value ...]
```

## 帮助
-h / --help  
获取帮助信息。
    
示例：
    
```sh
python ${Path}/install.py -h
```
    
## 参数设置
-Parameter_Name Value  
设置相关参数。
    
### 参数 s3-bucket-name
-s3-bucket-name String_Value  
设置静态网站存储的存储桶名。（使用前无需另外判断存储桶是否重名，如果重名，将会自动生成唯一的后缀，加在存储桶名后）  
默认值： s3-big-data-sync
    
### 参数 ddb-table-name
-ddb-table-name String_Value  
设置DynamoDB 表名，表为 TaskMonitor 模块生成的 s3 monitor status table  
默认值： s3cross_stat
    
### 参数 project-host-region
-project-host-region String_Value  
设置项目存放的AWS Region名，必须是合法的region id ，如 cn-north-1  
默认值： us-east-1
    
### 参数 frontend-speed-delay
-frontend-speed-delay String_Value  
设置速度统计的延迟值，单位为分钟。由于这里的速度只是一个参考值，一般建议不修改此值。  
默认值：  10
    
### 参数 iam-profile
-iam-profile String_Value  
设置 install.py 用到的 IAM Profile 名。为保证项目正常进行，建议使用具备Admin权限的IAM User。  
默认值： default
    
### 参数 ec2-key
-ec2-key String_Value  
设置启动的后端机器使用的Key名称，用于ssh登录ec2实例。如果设置此项，输入的Key名必须存在。  
默认值： 无（默认不设置EC2 Key，即无法ssh登录）

### 参数 s3-manifest-path
-s3-manifest-path String_Value
设置manifest 文件的路径，格式为 Bucket_name/Key_path。
默认值： 无（如果不输入，会导致程序意外终止）
    
### 参数示例
输入
    
```sh
python ${Path}/install.py -iam-profile bjs -project-host-region cn-north-1 -ec2-key self-BJS -s3-manifest-path leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json
```
    
输出
    
```text
Upload file: ./FrontEnd/about.html to S3 bucket: s3-big-data-sync ...
Upload file: ./FrontEnd/assets/css/bootstrap.min.css to S3 bucket: s3-big-data-sync ...
......
Upload file: ./backend.tar.gz to S3 bucket: s3-big-data-sync ...
Stack start, please wait for about 5 mins.
Stack still in progress.
Stack has been done.
Upload file: ./configuration.js to S3 bucket: s3-big-data-sync ...
UICenter is installed successfully.
WebsiteURL: http://s3-big-data-sync-bphtrxgyqrobjp.s3-website.cn-north-1.amazonaws.com.cn/
```
    
输出最后一行的URL为生成的网站地址。
