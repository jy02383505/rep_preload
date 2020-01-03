#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import json
import os
import traceback
import time
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

def monitor(host):
    print ('---------------------------------------------------')
    url = 'http://{}:15672/api/queues'.format(host)

    r = requests.get(url, auth=("bermuda", "bermuda"), timeout=5)
    parsed = json.loads(r.content)
    #print parsed
    error_list = []
    for i in parsed:
        k = i.get('name')  # 队列名
        v = i.get('messages')  # 待处理队列数
        c = i.get('consumers')  # 消费者数
        m = i.get('memory')  ##队列消耗内存
        #print k, v, c, m
        if v > 4000 and k!='async_devices':
            print (v)
            error_str = "host:{},queue:{},number:{}".format(host,k,v)
            error_list.append(error_str)
    return error_list
def warn_email(error_all):
    error_str = '\r\n'.join(error_all)
    send_email(today='',  content=error_str)

def send_email(today='',  content=''):
    subject = "刷新预加载紧急报警！！！！"
    # _user = "refresh@chinacache.com"
    # _pwd  = "jio90JIO" # for @chinacache.com

    _user = "refresh@maggy.club"
    # _user = "noreply@chinacache.com"
    # _user = "yanming.liang@chinacache.com"
    _pwd  = "qazQAZ123@"
    _to   = ['pengfei.hao@chinacache.com','565875791@qq.com']
    #_to   = ["yanming.liang@chinacache.com", 'forgivemee@qq.com']
    str_s = "下面是出现问题的队列信息,请查看具体情况,如果连续超过两次收到邮件,请紧急处理.\r\n"
    content = str_s+content

    msg = MIMEText(content)
    msg["Subject"] = subject
    msg["From"]    = _user
    msg["To"]      = ','.join(_to)
    try:
        s = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        s.login(_user, _pwd)
        s.sendmail(_user, _to, msg.as_string())
        s.quit()
        #print('success_%s' % today if today else '')
    except smtplib.SMTPException as e:
        print("Error: %s" % traceback.format_exc())

def main():
    print (datetime.now())
    host_list = ['223.202.203.74','223.202.203.84','223.202.203.79','223.202.203.89']
    error_all = []
    for host in host_list:
        error_list = monitor(host)
        if error_list :
            error_all.extend(error_list)
    if error_all:
        warn_email(error_all)

if __name__ =="__main__":
    main()