#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='rate-limiter-py',
    version='0.2.1',
    description='Rate-limiter module which leverages DynamoDB to enforce resource limits.',
    keywords=['lifeomic', 'dynamodb', 'rate', 'limit'],
    author='Matthew Tieman',
    author_email='mjtieman55@gmail.com',
    url='https://github.com/lifeomic/rate-limiter-py',
    download_url='https://github.com/lifeomic/rate-limiter-py/archive/0.2.1.tar.gz',
    packages=find_packages(),
    license='MIT',
    install_requires=[
        'boto3==1.6.4',
        'botocore==1.9.4',
        'future==0.16.0'
    ]
)
