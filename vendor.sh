#!/bin/bash

if [ -e vendor ]; then
    echo "vendor folder already exists"
    exit 1
fi

gb vendor fetch -tag v1.10.14 github.com/aws/aws-sdk-go/aws
gb vendor fetch -tag v1.10.14 github.com/aws/aws-sdk-go/service/s3
gb vendor fetch -revision a98ad7ee00ec53921f08832bc06ecf7fd600e6a1 github.com/vaughan0/go-ini
gb vendor fetch -tag v0.9.3 gopkg.in/fsnotify.v0
