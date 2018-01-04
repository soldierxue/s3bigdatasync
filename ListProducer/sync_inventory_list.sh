#!/bin/bash 

run()
{
	echo $1
	$1
}


# aws s3 ls s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter/2017-12-30T08-00Z/

DIR="s3://leo-bjs-inventory-bucket/leodatacenter/leodatacenter"
D="`date -d yesterday +%Y-%m-%d`T08-00Z"



rm -vfr manifest.json manifest.checksum


CMD="aws s3 sync $DIR/$D ."

run "$CMD"

exit 0
