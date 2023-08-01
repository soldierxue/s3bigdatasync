# -*- coding:utf-8 -*-
import re
import sys
import math
import json
import random
import logging
import asyncio
import time
from datetime import datetime
import pandas as pd
import queue
import psutil
import boto3
from botocore.exceptions import ClientError
from hashlib import md5
from azure.storage.blob.aio import BlobClient
from azure.core.exceptions import AzureError
import conf
from mpmgr import MpReactorManager

__version__ = "1.0"

logger = logging.getLogger(__name__)

def calc_part_md5(data):
    return md5(data).digest()

def calc_etag(digests):
    return md5(b''.join(digests)).hexdigest() + '-' + str(len(digests))

# calc parts count and parts range
#   size_hint format: 'bytes 0-78366/78367'
#   @return:
#     part_size - size of part chunk
#     parts_count - num of parts
#     content_len - size of object
def calc_parts_props(size_hint):
    m = re.match('bytes (\d+)-(\d+)/(\d+)', size_hint)
    start = int(m[1])
    end = int(m[2])
    total = int(m[3])

    part_size = end + 1
    if part_size == total:
        multipart = False
    else:
        multipart = True
    
    parts_count = (total + part_size - 1)//part_size

    return (part_size, parts_count, total)

class TaskContext():
    def __init__(self):
        self.s3_client = boto3.client('s3')

def task_init_func():
    ctx = TaskContext()
    return ctx

async def task_func_inner(ctx, task_msg, proc_id):

    task_msg.ts_start = int(time.time())

    # get metadata from S3
    # this is sync call will block
    resp = ctx.s3_client.head_object(Bucket=task_msg.s3_bucket_name, Key=task_msg.s3_key_name, PartNumber=1)
    if resp['Metadata'].get('mtime') == None:
        raise RuntimeError('无法获取 S3 对象的 x-amz-meta-mtime')

    s3_meta_mtime = int(resp['Metadata']['mtime'])

    # get metadata from blob
    blob = BlobClient.from_connection_string(
        conn_str=conf.BLOB_CONN_STR,
        container_name=task_msg.blob_container_name,
        blob_name=task_msg.blob_name)
    blob_props = await blob.get_blob_properties()
    blob_mtime = int(blob_props.last_modified.timestamp())
    blob_size = blob_props.size
    blob_etag = blob_props.etag

    # calc S3 object part size and parts count
    size_hint = resp['ResponseMetadata']['HTTPHeaders']['content-range']
    props = calc_parts_props(size_hint)
    s3_etag = resp['ETag'].replace('"', '')
    parts_count = resp.get('PartsCount', 0)
    chunk_size = props[0]
    s3_object_size = props[2]

    # compare mtime and size
    if s3_meta_mtime != blob_mtime or s3_object_size != blob_size:
        raise RuntimeError('S3 mtime: {}, size: {} 与 Blob mtime: {}, size:{} 不一致'.format(s3_meta_mtime, s3_object_size, blob_mtime, blob_size))

    if parts_count == 0:
        #assert props[1] == 1
        is_multipart_obj = False
    else:
        #assert props[1] == parts_count
        is_multipart_obj = parts_count

    md5_digests = []
    if is_multipart_obj:
        for i in range(0, parts_count):
            stream = await blob.download_blob(offset=i*chunk_size, length=chunk_size)
            data = await stream.readall()
            md5sum = calc_part_md5(data)
            md5_digests.append(md5sum)
    else:
        stream = await blob.download_blob(offset=i*chunk_size, length=chunk_size)
        data = await stream.readall()
        md5sum = calc_part_md5(data)
        md5_digests.append(md5sum)

    # calc etag
    if is_multipart_obj:
        etag = calc_etag(md5_digests)
    else:
        etag = md5_digests[0].hex()

    logger.debug("calculated ETag {} - S3 ETag {}".format(etag, s3_etag))
    setattr(task_msg, 'etag', '{}/{}'.format(etag, s3_etag))
    if s3_etag != etag:
        task_msg.err_msg = 'ETag 校验不一致 - Blob ETag 计算结果: {}, S3 Etag: {}'.format(etag, s3_etag)
        task_msg.ts_end = int(time.time())
        return {"result": "failed", "msg": task_msg}

    # do second check with blob props
    if conf.BLOB_PROPS_DOUBLE_CHECK == True:
        blob_props2 = await blob.get_blob_properties()
        blob_mtime2 = int(blob_props2.last_modified.timestamp())
        blob_size2 = blob_props2.size
        blob_etag2 = blob_props2.etag
        if blob_mtime != blob_mtime2 or blob_size != blob_size2 or blob_etag != blob_etag2:
            task_msg.err_msg = 'Blob 属性二次检查失败 mtime: {}/{}, size: {}/{}, etag: {}/{}'.format(
                        blob_mtime, blob_mtime2,
                        blob_size, blob_size2,
                        blob_etag.replace('"',''), blob_etag2.replace('"',''))
            task_msg.ts_end = int(time.time())
            return {"result": "failed", "msg": task_msg}

    # save blob mtime/ blob size/ blob etag/s3 etag
    task_msg.err_msg = '{}/{}/{}/{}'.format(blob_mtime, blob_size, blob_etag.replace('"',''), s3_etag)
    task_msg.ts_end = int(time.time())
    return {"result": "success", "msg": task_msg}

async def task_func(ctx, task_msg, proc_id):
    try:
        result = await task_func_inner(ctx, task_msg, proc_id)
    except ClientError as e:
        http_code = e.response['ResponseMetadata']['HTTPStatusCode']
        err_msg = 'S3 客户端错误: {}, HTTP CODE: {}'.format(str(e), http_code)

        task_msg.err_msg = err_msg
        task_msg.ts_end = int(time.time())
        result = {'result': 'exception', 'msg': task_msg}
    except AzureError as e:
        http_code = e.status_code
        # there is \n in blob error messages, remove it
        err_msg = 'Blob 客户端错误: {}, HTTP CODE: {}'.format(e.message.replace('\n', ' '), http_code)

        task_msg.err_msg = err_msg
        task_msg.ts_end = int(time.time())
        result = {'result': 'exception', 'msg': task_msg}
    except RuntimeError as e:
        task_msg.err_msg = e.args[0]
        task_msg.ts_end = int(time.time())
        result = {'result': 'failed', 'msg': task_msg}
    except Exception as e:
        # raise any other unhandled exception
        raise e

    return result

class TaskMsg:
    # input row is a pd dataframe
    def __init__(self, row):
        self.storage_account = row['StorageAccount']
        self.blob_container_name = row['ContainerName']
        self.blob_name = row['BlobName']
        self.s3_bucket_name = row['S3Bucket']
        self.s3_key_name = row['S3Key']
        # if S3 key field is null, use blob name directly
        if pd.isnull(row['S3Key']):
            self.s3_key_name = row['BlobName']

        # other props for task msg lifecycle tracking
        self.err_msg = ''
        self.ts_init = int(time.time())
        self.ts_start = 0
        self.ts_end = 0

    def to_csv_line(self):
        # output format
        # "StorageAccount","ContainerName","BlobName","S3Bucket","S3Key","ProcessTime","TimeCost","Message"
        return '"{}","{}","{}","{}","{}","{}","{}","{}"\n'.format(
            self.storage_account,
            self.blob_container_name,
            self.blob_name,
            self.s3_bucket_name,
            self.s3_key_name,
            datetime.fromtimestamp(self.ts_start).strftime('%Y-%m-%dT%H:%M:%S'),
            self.ts_end - self.ts_init,
            self.err_msg
        )

class MainContext():
    def __init__(self):
        pass

def main_init_func(ctx):

    setattr(ctx, 'start_time', time.time())

    chunk_reader = pd.read_csv(ctx.input_file, iterator=True)
    setattr(ctx, 'chunk_reader', chunk_reader)

    setattr(ctx, 'total_row_count', 0)
    setattr(ctx, 'processed_success_row_count', 0)
    setattr(ctx, 'processed_errors_row_count', 0)
    setattr(ctx, 'input_task_finished', False)
    setattr(ctx, 'all_task_processed', False)

    # gen job uuid
    import uuid
    job_uuid = uuid.uuid1()
    setattr(ctx, 'job_uuid', job_uuid)

    output_success = '{}{}.csv'.format(conf.OUTPUT_SUCCESS_PREFIX, job_uuid)
    f_success = open(output_success, "a", buffering=10)
    f_success.write(conf.OUTPUT_CSV_HEADER)
    f_success.flush()
    setattr(ctx, 'output_success', f_success)

    output_errors = '{}{}.csv'.format(conf.OUTPUT_ERRORS_PREFIX, job_uuid)
    f_errors = open(output_errors, "a", buffering=10)
    f_errors.write(conf.OUTPUT_CSV_HEADER)
    f_errors.flush()
    setattr(ctx, 'output_errors', f_errors)

    return ctx

def main_task_gen_func(ctx):

    try:
        chunk = ctx.chunk_reader.get_chunk(conf.INPUT_CHUNK_SIZE)
    except StopIteration:
        ctx.input_task_finished = True
        return []

    ctx.total_row_count += chunk.shape[0]
    return [TaskMsg(row) for idx, row in chunk.iterrows()]

def main_result_collector_func(ctx, results):
    
    # handle failed and exception messages
    failed = list(filter(lambda x: (x['result'] != 'success'), results))
    row_count = len(failed)
    for result in failed:
        msg = result['msg']
        ctx.output_errors.write(msg.to_csv_line())
    ctx.output_errors.flush()
    ctx.processed_errors_row_count += row_count

    success = list(filter(lambda x: (x['result'] == 'success'), results))
    row_count = len(success)
    for result in success:
        msg = result['msg']
        ctx.output_success.write(msg.to_csv_line())
    ctx.output_success.flush()
    ctx.processed_success_row_count += row_count

    if ctx.processed_errors_row_count + ctx.processed_success_row_count == ctx.total_row_count:
        assert ctx.input_task_finished == True
        ctx.all_task_processed = True
        logger.info("job: {} - 完成对象比较 总数: {}, 错误数: {}, 总耗时: {} 秒".format(
            ctx.job_uuid,
            ctx.total_row_count,
            ctx.processed_errors_row_count,
            int(time.time() - ctx.start_time))
        )
        import os, signal
        os.kill(os.getppid(), signal.SIGTERM)
        sys.exit(0)

    return

# >= 1:  scale up
# <= -1: scale down
# else:  do nothing
def scale_condition_func():
    cpu_load1, cpu_load5, cpu_load15 = psutil.getloadavg()
    mem = psutil.virtual_memory()

    if cpu_load1 < 50 and mem.percent <= 40.0:
        return 1
    if cpu_load5 > 60:
        return -1

    return 0

def main(input):

    ctx = MainContext()
    setattr(ctx, 'count', 0)
    setattr(ctx, 'input_file', input)

    logger.info('etag checker reactor @v%s starting ...' % __version__)
    reactor = MpReactorManager(ctx, main_init_func, main_task_gen_func, main_result_collector_func, task_init_func, task_func, scale_condition_func)
    reactor.run()

