#!/bin/bash
echo "waiting 5 minute before destroy"
sleep 300
aws s3api delete-object --bucket movie-similarity-bucket-xqf052 --key movie.json
aws cloudformation delete-stack --stack-name movie-similarity-stack
aws ec2 delete-key-pair --key-name MyKey