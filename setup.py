#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='rate-limiter-py',
    version='0.0.0',
    description='',
    author='Matthew Tieman',
    author_email='matthew.tieman@lifeomic.com',
    url='',
    packages=find_packages(),
    license='Proprietary',
    install_requires=[
        'boto3==1.6.4',
        'botocore==1.9.4'
    ]
)
