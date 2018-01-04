#!/bin/bash



# Downlaod inventory: Shell
# sync_inventory_list.sh


# Create SQS 
#./sqs_utils.py   #  create_test_queues('s3sync-worker', 100)

# Download inventory: Python
./ListProducer.py

exit 0
