# Samples
src: s3://leodatacenter in cn-north-1 region
src inventory position: s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter
dst: s3://leo-zhy-datacenter  in cn-northwest-1 region
dst inventory position: s3://leo-zhy-inventory-bucket/leo-zhy-datacenter/daily-check

# Code Snippets
aws s3 ls s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/

# Sub-tasks
[X] Buck list S3 buckets size/files [GitHub Pages](https://github.com/iceflow/aws-demo/blob/master/s3/cli/s3-stat.sh)
[ ] Get latest bucket related manifest pos  
[ ] Getting specific manifest, downloading data and parsing the tasks
Options:
[ ] Get bucket inventory position information
