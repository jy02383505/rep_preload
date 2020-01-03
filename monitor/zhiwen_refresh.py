#-*- coding:utf-8 -*-
import pymongo
import datetime
import urllib
import urllib2
import json

Host = '223.202.203.93'
def get_host(host,db_name):
    client = pymongo.MongoClient(host=host, port=27017)
    db = client[db_name]
    db.authenticate('bermuda', 'bermuda_refresh')
    return db

def zhiwen_ip2(url="",dir=""):
    task = json.dumps({'urls': [url],'purge_dirs': [dir]})

    f = urllib.urlopen("http://101.251.97.247/refresh", task)
    # f = urllib.urlopen("http://"+ip+"/internal/refreshDevice" , params)
    print f.read()


def zhiwen_ip(url):
    test_data = {'url': [url],"ip":["127.0.0.1"]}
    test_data_urlencode = urllib.urlencode(test_data)

    requrl = "http://101.251.97.247/refresh"

    req = urllib2.Request(url=requrl, data=test_data_urlencode)
    print req

    res_data = urllib2.urlopen(req)
    res = res_data.read()
    print res
def zhi_wen2(url):
    import urllib
    import httplib
    test_data = {'urls': [url],'purge_dirs': [""],"ip":['192.168.10.11']}
    test_data_urlencode = urllib.urlencode(test_data)

    requrl = "http://101.251.97.247/refresh"

    conn = httplib.HTTPConnection("101.251.97.247")

    conn.request(method="POST", url=requrl, body=test_data_urlencode)

    response = conn.getresponse()

    res = response.read()

    print res
def main():
    end_time = datetime.datetime.now()
    act_time = end_time - datetime.timedelta(minutes=10)
    print (end_time,act_time)
    mgdb = get_host(Host,db_name="bermuda")
    url_list = mgdb['url'].find({"created_time":{"$gt":act_time,"$lt":end_time}})
    for url in url_list:
        url_url = url.get('url')
        type_url = url.get('isdir')
        if type_url:

            zhiwen_ip2(dir=url_url)
        else:

            zhiwen_ip2(url=url_url)
        #print url_url

if __name__ =="__main__":
    main()
