#!/bin/bash

num=1
iNum=0
path=/var/s3bigdatasync/BackEnd/
while(( $num < 5 ))
do
    cd $path
	sn=`ps -ef | grep server.py | grep -v grep |awk '{print $2}'`
	echo $sn
	if [ "${sn}" = "" ]
	then
		let "iNum++"
		echo $iNum
		./server.py &
		echo "Server start successful!"
	fi
	
	if [ "${1}" = "test" ]
	then
		sn=`ps -ef | grep ddbModel.py | grep -v grep |awk '{print $2}'`
		echo $sn
		if [ "${sn}" = "" ]
		then
			let "iNum++"
			echo $iNum
			python ddbModel.py &
			echo "DDBModel start successful!"
		fi
	fi
	
	sleep 10
done
