#!/usr/bin/env python
from setuptools import setup, find_packages

with open('./requirements.txt', 'r') as file:
    requirements = file.read().splitlines()

setup(name='rate-limiter-py',
    version='0.3.0',
    description='Rate-limiter module which leverages DynamoDB to enforce resource limits.',
    keywords=['lifeomic', 'dynamodb', 'rate', 'limit'],
    author='Matthew Tieman',
    author_email='mjtieman55@gmail.com',
    url='https://github.com/lifeomic/rate-limiter-py',
    download_url='https://github.com/lifeomic/rate-limiter-py/archive/0.3.0.tar.gz',
    packages=find_packages(),
    license='MIT',
    python_requires='>=3.6.0',
    install_requires=requirements
)
