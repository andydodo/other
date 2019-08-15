#!/usr/bin/python
import time
import boto3, sys
from botocore.client import Config

#bucket_name = sys.argv[1]
#obj_prefix = sys.argv[2]

s3_cli = boto3.client('s3', 'sh-bt-1',
    config=Config(signature_version='s3v4',s3={'addressing_style': 'path'}), use_ssl=False,
    endpoint_url='http://10.20.3.13/',
    aws_secret_access_key='81ezbc3SbrBC8qXUPpKzSY3c8B',
    aws_access_key_id='xJJf6V3AvgOaJn3y')

url=s3_cli.generate_presigned_url('get_object', Params={'Bucket': "test-bucket", 'Key': "test-obj"}, ExpiresIn=3600)

print(url)
