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
    
### 参数 b bucket
-b String_Value  
设置静态网站存储的存储桶名。（使用前无需另外判断存储桶是否重名，如果重名，将会自动生成唯一的后缀，加在存储桶名后）  
默认值： s3-big-data-sync
    
### 参数 t table
-t String_Value  
设置DynamoDB 表名，表为 TaskMonitor 模块生成的 s3 monitor status table  
默认值： s3cross_stat
    
### 参数 r region
-r String_Value  
设置项目存放的AWS Region名，必须是合法的region id ，如 cn-north-1  
默认值： us-east-1
    
### 参数 p profile
-p String_Value  
设置 install.py 用到的 IAM Profile 名。为保证项目正常进行，建议使用具备Admin权限的IAM User。  
默认值： default
    
### 参数 k key
-k String_Value  
设置启动的后端机器使用的Key名称，用于ssh登录ec2实例。如果设置此项，输入的Key名必须存在。  
默认值： 无（默认不设置EC2 Key，即无法ssh登录）

### 参数 m manifest
-m String_Value
设置manifest 文件的路径，格式为 Bucket_name/Key_path。
必需

### 参数 M mode
-M String_Value
设置程序运行的模式，['prod', 'test']，如果选择为test模式，将会删除真实数据并产生测试数据，建议只在开发环境下使用
默认值： prod

### 参数 s selection
-s String_Value
设置程序安装的模块，['all', 's3-frontend', 'ec2-backend']，可以选择全部安装all，或者在已经安装一部分的情况下安装另一部分
默认值： all
    
### 参数示例
输入
    
```sh
python install.py -r cn-north-1 -k poc -m leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/job.json
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
--------------------------------------------------------------------------------
WebsiteURL: http://s3-big-data-sync-bphtrxgyqrobjp.s3-website.cn-north-1.amazonaws.com.cn/
```
    
输出最后一行的URL为生成的网站地址。
