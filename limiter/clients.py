#!/bin/bash/env python

import boto3

def dynamodb():
    boto3.setup_default_session(region_name='us-east-1')
    return boto3.resource('dynamodb')
