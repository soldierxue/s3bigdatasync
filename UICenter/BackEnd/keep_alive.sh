#!/bin/bash
num=1
iNum=0
path=/var/s3bigdatasync/BackEnd/server.py
echo $$
while(( $num < 5 ))
do
	sn=`ps -ef | grep $path | grep -v grep |awk '{print $2}'`
	echo $sn
	if [ "${sn}" = "" ]
	then
		let "iNum++"
		echo $iNum
		python $path
		echo "start successful!"
	fi
	sleep 5
done
