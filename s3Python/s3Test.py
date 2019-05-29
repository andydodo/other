#!/usr/bin/python
import time
import boto3, sys
from botocore.client import Config
import os

s3_cli = boto3.client('s3', 'sh-bt-1',
    config=Config(signature_version='s3v4'), use_ssl=False,
    endpoint_url='http://',
    aws_secret_access_key='',
    aws_access_key_id='')

def create_bucket(bucket_name):
    response = s3_cli.create_bucket(ACL='private',Bucket=bucket_name)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        print("create bucket error")

def upload_file(bucket_name):
    put_file_list = os.listdir(file_path)
    for file in put_file_list:
        src_file = os.path.join("%s/" %(file_path),file)
        with open(src_file, 'rb') as data:
            s3_cli.upload_fileobj(data, bucket_name, file)


def list_objects(bucket_name):
    response = s3_cli.list_objects(Bucket="%s" %(bucket_name))
    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
         if "Contents" in response.keys():
             for s3_object in response["Contents"]:
                 yield s3_object["Key"]
         else:
             raise SystemError("{0} has objects is zero".format(response["Name"]))
    else:
        raise ConnectionError("connect s3 timeout,please call s3 develop")


def delete_objects(bucket_name):
        delete_list = []

        for file_name in list_objects(bucket_name=bucket_name):
            if file_name == "version":
                pass
            else:
                delete_list.append({"Key":file_name})
        if delete_list:
            response_del = s3_cli.delete_objects(Bucket="%s" %(bucket_name),
                                       Delete={
                                           'Objects': delete_list,
                                       }
                                   )
            print("Bucket %s clean_ok" %(bucket_name))

if __name__ == '__main__':
    num=0
    while True:
        file_path = '/data11/test'
        bucket_name = '001-09-12-test'
        bucket_name = bucket_name + str(num)
        num += 1 
        create_bucket(bucket_name)
	time.sleep(1)
        upload_file(bucket_name)
    #list_objects(bucket_name)
    #delete_objects(bucket_name)
