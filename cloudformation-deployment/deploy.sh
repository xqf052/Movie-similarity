#!/bin/bash
aws ec2 create-key-pair --key-name MyKey --query 'KeyMaterial' --output text > /var/jenkins_home/workspace/movie-similarity-pipeline-github/cloudformation-deployment/MyKey.pem
chmod 400 /var/jenkins_home/workspace/movie-similarity-pipeline-github/cloudformation-deployment/MyKey.pem
aws cloudformation create-stack --stack-name movie-similarity-stack --template-body file://cloudformation.yml --parameters ParameterKey=MyKey,ParameterValue=MyKey --capabilities CAPABILITY_NAMED_IAM
echo "waiting 20 minute for instance to spin up"
sleep 1000
export publicIp=$(aws cloudformation --region ap-southeast-2 describe-stacks --stack-name movie-similarity-stack --query "Stacks[0].Outputs[0].OutputValue")
echo $publicIp
cp /var/jenkins_home/workspace/movie-similarity-pipeline-github/ansible/inventory /var/jenkins_home/workspace/movie-similarity-pipeline-github/ansible/hosts
echo 'test1 ansible_host='${publicIp}' ansible_user=hadoop ansible_private_key_file=../cloudformation-deployment/MyKey.pem' >> /var/jenkins_home/workspace/movie-similarity-pipeline-github/ansible/hosts

