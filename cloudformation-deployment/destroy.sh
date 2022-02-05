#!/bin/bash
aws cloudformation delete-stack --stack-name movie-similarity-stack
aws ec2 delete-key-pair --key-name MyKey