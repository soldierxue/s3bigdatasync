#!/bin/bash

num=1
iNum=0
#path=/var/s3bigdatasync/BackEnd/
path=/Users/davwan/Desktop/Projects/CS-EAST-SA/s3bigdatasync/UICenter/BackEnd/
echo $$
while(( $num < 5 ))
do
    cd $path
	sn=`ps -ef | grep server.py | grep -v grep |awk '{print $2}'`
	echo $sn
	if [ "${sn}" = "" ]
	then
		let "iNum++"
		echo $iNum
		./server.py >> traceback.log
		echo "start successful!"
	fi
	sleep 5
done
