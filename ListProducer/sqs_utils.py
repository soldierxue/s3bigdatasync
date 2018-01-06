#!/usr/bin/python
# -*- coding: utf8 -*-

# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#SQS.Client.list_queues
# http://boto3.readthedocs.io/en/latest/reference/services/sqs.html#queue

import sys
from pprint import pprint
import boto3
import json

#session = boto3.Session(profile_name='joyou@ctrip')
#client = session.client('sqs')
sqs_client = boto3.client('sqs')

def send_msg_to_sqs(qurl, body=None):

    if body is None:
        return False

    response = sqs_client.send_message(
        QueueUrl=qurl,
        MessageBody=json.dumps(body)
    )
    print "send_msg_to_sql:({0}..Number[{1}].".format(qurl, len(body))
    #print response

    return True

def list_test():
    response = sqs_client.list_queues(
        QueueNamePrefix='https://cn-north-1.queue.amazonaws.com.cn/358620020600/s3sync-worker-dead1'
    )

    pprint(response)

def send_test():
    response = sqs_client.send_message(
        QueueUrl='https://sqs.eu-west-1.amazonaws.com/888250974927/s3-copy-list-1',
        MessageBody='string'
    )

    pprint(response)


def recv_test():
    response = sqs_client.receive_message(
        QueueUrl='https://eu-west-1.queue.amazonaws.com/888250974927/s3-copy-list-18',
        MaxNumberOfMessages=10
    )

    pprint(response)

def get_queue_attributes(queue_url=None):
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['All']
    )

    pprint(response)

    return(response)

def get_queue_attributes_arn(queue_url=None):
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['QueueArn']
    )

    #pprint(response)

    return(response['Attributes']['QueueArn'])
    
def delete_queue(queue_url=None):
    response = sqs_client.delete_queue(
        QueueUrl=queue_url
    )


def create_sqs(queue_name=None, enable_dead_letter=False, redrive_policy=None ):

    '''
    redrive_policy = {
        'deadLetterTargetArn': dead_letter_queue_arn,
        'maxReceiveCount': '3'
    }
    '''

    Attributes={}
    if enable_dead_letter:
        Attributes['RedrivePolicy'] = json.dumps(redrive_policy)
    
    Attributes['VisibilityTimeout']='60'
    Attributes['ReceiveMessageWaitTimeSeconds']='5'

    response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes=Attributes
    )

    #pprint(response)

    return(response['QueueUrl'])

def create_test_queues(queue_name=None, queue_num=None):
    dead_letter_queue_url = create_sqs('%s-dead-letter'%queue_name)
    dead_letter_queue_arn = get_queue_attributes_arn(dead_letter_queue_url)
    #dead_letter_queue_arn = 'arn:aws-cn:sqs:cn-north-1:358620020600:s3sync-worker-dead'
    #pprint(dead_letter_queue_arn)
    
    redrive_policy = {
        'deadLetterTargetArn': dead_letter_queue_arn,
        'maxReceiveCount': '3'
    }


    for n in range(1, queue_num+1):
        queue_url = create_sqs('%s-%03d'%(queue_name, n), enable_dead_letter=True, redrive_policy=redrive_policy)
        print(queue_url)
    

def delete_test_queues(queue_name=None, queue_num=None):
    pass
    

if __name__ == '__main__':
    create_test_queues('s3sync-worker', 100)
    #create_test_queues('d3sync-worker', 2)

   # delete_test_queues('s3sync-worker', 100)
    #list_test()

#    send_test()

#    recv_test()

    sys.exit(0);

