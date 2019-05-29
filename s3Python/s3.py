#!/usr/bin/env python
# -*- coding:utf-8 -*-
import boto3
import socket
import os
import sys
import json
import hashlib
import requests
import optparse
import time
from botocore.client import Config
from conf import settings
from prettytable import PrettyTable
from utils.logger import logger_write

class Push_BsLine_To_S3(object):
    def __init__(self,shbt_s3,shyc2_s3,zzzc_s3,alarm_users,api_url):
        self.hostname = socket.gethostname()
        self.idc = self.hostname.split(".")[-3]
        self.shbt_s3_settings = shbt_s3
        self.shyc2_s3_settings = shyc2_s3
        self.zzzc_s3_settings = zzzc_s3
        self.alarm_users = alarm_users
        self.send_alarm_url = api_url
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.check_path = "/data/adadmin/bs_baseline/output"
        self.lock_path = "/home/s/ops/s3_upload/bsline_lock"
        self.shard_id_list = [1509,]
        self.info_log = logger_write('info')
        self.error_log = logger_write('error')

    @classmethod
    def from_conf(cls):
        return cls(settings.SHBT,settings.SHYC2,settings.ZZZC,settings.ALARM_USER_DICT,settings.SEND_MESSAGE_URL)

    @property
    def show_set(self):
        x = PrettyTable(["endpoint_url", "aws_access_key_id", "aws_secret_access_key", "region_name"])
        x.align["name"] = "l"
        x.padding_width = 1
        x.add_row([self.shyc2_s3_settings["endpoint_url"],self.shyc2_s3_settings["aws_access_key_id"],\
                   self.shyc2_s3_settings["aws_secret_access_key"], self.shyc2_s3_settings["region_name"]])
        x.add_row([self.shbt_s3_settings["endpoint_url"],self.shbt_s3_settings["aws_access_key_id"],\
                   self.shbt_s3_settings["aws_secret_access_key"], self.shbt_s3_settings["region_name"]])
        x.add_row([self.zzzc_s3_settings["endpoint_url"],self.zzzc_s3_settings["aws_access_key_id"],\
                   self.zzzc_s3_settings["aws_secret_access_key"], self.zzzc_s3_settings["region_name"]])
        print(x)

    def upload_file(self,file_path,bucket_name):
        #s3_obj = self.create_obj(self.shyc2_s3_settings)
        #send_bucket_name = "%s_shyc2" %(bucket_name)
        #s3_obj.put_object(Bucket=send_bucket_name, Key='version',
        #                       Body='20180424')
        '''上传文件逻辑'''
        self.info_log.info("begin check file")
        #生成当前的时间戳
        stuck_time = time.localtime(time.time())
        now_date = time.strftime("%Y%m%d", stuck_time)
        file_exit = self.file_update_file(now_date)
        #判断锁是否存在
        if self.is_lock():
            self.info_log.info("this task is locked")
            sys.exit(1)
        else:
            pass
        # 判断时间戳是否不同
        if self.version_check(bucket_name,now_date):
            pass
        else:
            self.info_log.info("check version ok do not update")
            sys.exit(2)

        if not file_exit:
            self.info_log.info("fsline file is not exsit")
            sys.exit(3)
        else:
            os.system("touch %s" % (self.lock_path))#创建上传锁

        #清空s3桶
        self.info_log.info("ready to clean s3 bucket")
        self.delete_objects(bucket_name)
        self.info_log.info("bucket is clean ok")
        put_file_list = os.listdir("%s/%s" %(file_path,now_date))
        #根据机房选择对应s3集群
        if self.idc == "shgt" or self.idc == "shbt":
            s3_obj = self.create_obj(self.shbt_s3_settings)
            send_bucket_name = "%s_shbt" %(bucket_name)
        elif self.idc == "zzzc" or self.idc == "gzst":
            s3_obj = self.create_obj(self.zzzc_s3_settings)
            send_bucket_name = "%s_zzzc" % (bucket_name)
        elif self.idc == "shyc2":
            s3_obj = self.create_obj(self.shyc2_s3_settings)
            send_bucket_name = "%s_shyc2" % (bucket_name)

        for file in put_file_list:
            """
            循环上传文件对象到指定s3的bucket里
            """
            src_file = os.path.join("%s/%s/" %(file_path,now_date),file)
            with open(src_file, 'rb') as data:
                s3_obj.upload_fileobj(data, "%s_shyc2" %(bucket_name), file)
            print("\033[1;35m文件名:{0} 上传成功 \033[0m".format(file))
            self.info_log.info("%s file is update success" % (file))

        src_baseline_file = os.path.join("%s/baseline.txt" % (file_path))
        with open(src_baseline_file, 'rb') as f:
            s3_obj.upload_fileobj(f, "%s_shyc2" % (bucket_name), "baseline.txt")
        print("\033[1;35m文件名:{0} 上传成功 \033[0m".format("baseline.txt"))
        self.info_log.info("%s file is update success" % ("baseline.txt"))

        s3_obj.put_object(Bucket=send_bucket_name, Key='version',
                               Body='%s' % (time.strftime("%Y%m%d", stuck_time)))
        self.info_log.info("update version to s3 %s" % (time.strftime("%Y%m%d", stuck_time)))

        os.system("rm -rf  %s" % (self.lock_path)) #删除锁
        self.info_log.info("remove lock, this task is done")
    def create_obj(self,obj):
        '''实例化s3对象'''
        s3_cli = boto3.client('s3', obj["region_name"],
                              config=Config(signature_version='s3v4'), use_ssl=False,
                              endpoint_url=obj["endpoint_url"],
                              aws_secret_access_key=obj["aws_secret_access_key"],
                              aws_access_key_id=obj["aws_access_key_id"])
        return s3_cli
    def file_update_file(self,file_date):
        if os.path.exists("%s/%s" %(self.check_path,file_date)) \
                and os.path.exists("%s/baseline.txt" %(self.check_path)):
            return 1
        else:
            return 0

    def version_check(self,bucket_name,today_date):
        if self.idc == "shbt" or self.idc == "shgt":
            s3_obj = self.create_obj(self.shbt_s3_settings)
            my_idc = "shbt"
        elif self.idc == "shyc2":
            s3_obj= self.create_obj(self.shyc2_s3_settings)
            my_idc = "shyc2"
        elif self.idc == "zzzc" or self.idc == "gzst":
            s3_obj = self.create_obj(self.zzzc_s3_settings)
            my_idc = "zzzc"
        res_version = s3_obj.get_object(Bucket="%s_%s" %(bucket_name,my_idc), Key='version')
        s3_version = res_version["Body"].read()
        if s3_version.strip() == today_date:
            return False
        else:
            return True
    def is_lock(self):
        """
        判断是否存在锁文件
        :return: 
        """
        if os.path.exists(self.lock_path):
            return True
        else:
            return False


    def delete_objects(self,bucket_name):
        delete_list = []
        if self.idc == "shbt" or self.idc == "shgt":
            s3_obj = self.create_obj(self.shbt_s3_settings)
            my_idc = "shbt"
        elif self.idc == "shyc2":
            s3_obj= self.create_obj(self.shyc2_s3_settings)
            my_idc = "shyc2"
        elif self.idc == "zzzc" or self.idc == "gzst":
            s3_obj = self.create_obj(self.zzzc_s3_settings)
            my_idc = "zzzc"

        for file_name in self.list_objects(bucket_name=bucket_name,s3_obj=s3_obj,idc=my_idc):
            if file_name == "version":
                pass
            else:
                delete_list.append({"Key":file_name})
        if delete_list:
            response_del = s3_obj.delete_objects(Bucket="%s_%s" %(bucket_name,my_idc),
                                       Delete={
                                           'Objects': delete_list,
                                       }
                                   )
            print("Bucket %s clean_ok" %("%s_%s" %(bucket_name,my_idc)))


    def list_objects(self,bucket_name,s3_obj,idc):
        response = s3_obj.list_objects(Bucket="%s_%s" %(bucket_name,idc))
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            if "Contents" in response.keys():
                for s3_object in response["Contents"]:
                    yield s3_object["Key"]
            else:
                raise SystemError("{0} has objects is zero".format(response["Name"]))
        else:
            raise ConnectionError("connect s3 timeout,please call s3 develop")

    def get_host(self,sid, tag_ids=None, idc=None, secret_key="5438c69ea7545499b02f8ac37352181c", mid='1001'):
        my_list = []
        url = "http://open.hulk.corp.qihoo.net?router=/web/host/api/host/get-host-list"
        params = int(time.time())
        if idc:
            src_data = {
                "t": params,
                "sid": sid,
                "tag_ids": tag_ids,
                "idc": idc,
            }
        else:
            src_data = {
                "t": params,
                "sid": sid,
                "tag_ids": tag_ids,
            }
        dic = sorted(src_data.items(), key=lambda key: key[0])  # sort排序
        for info in dic:
            bstr = str(info[0]) + '=' + str(info[1])
            my_list.append(bstr)
        my_list = '&'.join(my_list)
        m = hashlib.md5()
        m.update(my_list.encode('utf-8'))
        inner = m.hexdigest()
        g = hashlib.md5()
        g.update(str('%s%s' % (inner, secret_key)).encode('utf-8'))  # 生成证书验证
        send_url = url + "&mid={0}&sign={1}&{2}".format(mid, g.hexdigest(), my_list)
        response = requests.get(send_url)
        res = json.loads(response.text)
        return res['data']

    def get_shard_id(self):
        for tag_id in self.shard_id_list:
            host_list = self.get_host(sid="4403",tag_ids="%s" %(tag_id),idc=self.idc)
            for host in host_list:
                if host["hostname"].split("w-")[-1] == self.hostname:
                    return host["memo"].split(":")[-1]


    def parse_option(self):
        parser = optparse.OptionParser()
        parser.add_option("-b", "--bucket", dest="Bucket", help="Add Upload Bucket")
        parser.add_option("-c", "--clean", dest="Clean", help="Clean Bucket",default=False)
        parser.add_option("-s","--show",dest="Show",help="Show S3 Settings")
        parser.add_option("-d","--dir",dest="Path",help="Upload Dir")
        options, args = parser.parse_args()
        if options.Bucket and not options.Clean and options.Path:
            shard_id = self.get_shard_id()
            Upload.upload_file(file_path=options.Path, bucket_name="%s%s" %(options.Bucket,shard_id))
        elif options.Bucket and options.Clean:
            Upload.delete_objects(bucket_name=options.Bucket)
        elif not options.Bucket and not options.Clean and not options.Path and options.Show:
            Upload.show_set

if __name__ == '__main__':
    #获取settings配置
    Upload = Push_BsLine_To_S3.from_conf()
    Upload.parse_option()
