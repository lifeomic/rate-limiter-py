#!/bin/bash/env python
import boto3

def dynamodb():
    """ Create a DynamoDB resource instance. """
    return boto3.resource('dynamodb')
