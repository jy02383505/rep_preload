#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement
import simplejson as json
import sys
import traceback, logging, time
from datetime import datetime
from core import my_queue as queue
from core import database
from core.config import config
from util.tools import get_mongo_str
from core import auth_redis
import copy


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logfile = '/Application/rep_preload/logs/router.log'
fh = logging.FileHandler(logfile, mode='a')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

db_s1 = database.s1_db_session()

REDIS_CACHE = auth_redis.getDB(1)


class Refresh_router(object):
    def __init__(self, batch_size=10000, package_size=60):
        self.batch_size = batch_size
        self.package_size = package_size
        self.merged_refresh = {}
    def run(self):
        logger.info("refresh_router.start")
        self.refresh_router()
        logger.info("refresh_router.end")

    def refresh_router(self, queue_name='result_task'):
        '''
        回调数据打包
        '''

        messages = queue.get("result_task", 10000)
        if not messages:
            return
        for body in messages:
            task = json.loads(body)
            self.merge_refresh_new(task)
        #logger.info('self.merged_refresh {}'.format(self.merged_refresh))
        #for key_db_k in self.merged_refresh:
        #    logger.info('merged_refresh for in {}'.format(key_db_k))
        #    logger.info('merged_refresh for in {}'.format(self.merged_refresh.get(key_db_k)))
        #    self.update_refresh_result(self.merged_refresh.pop(key_db_k))
        logger.info('self.merged_refresh {}'.format(self.merged_refresh))
        copy_message_refresh = copy.deepcopy(self.merged_refresh)
        logger.info('copy_message_refresh action {}'.format(copy_message_refresh ))
        for key_db_k in copy_message_refresh:
            #logger.info('merged_refresh for in {}'.format(key_db_k))
            logger.info('merged_refresh for in {}'.format(copy_message_refresh.get(key_db_k)))
            self.update_refresh_result2(copy_message_refresh.get(key_db_k))
        else:
            logger.info('self.merged_refresh else {}'.format(self.merged_refresh))
        logger.info('copy_message_refresh end {}'.format(copy_message_refresh ))

    def merge_refresh_new(self, task):
        session_id = task['session_id']
        vip = task['host']
        host =vip # vip.replace('.','#')
        result = task['result']
        logger.info("refresh_router :{}|{}|{}".format(session_id, task.get('host'), task.get('hostname')))
        db_key = get_mongo_str(session_id, 10)
        #logger.info("refresh_router_key :{}|{}".format(session_id,db_key))
        insert_dict = {host:result}
        #添加过滤条件
        if not REDIS_CACHE.hexists(session_id,host):
            REDIS_CACHE.hmset(session_id,insert_dict)
            REDIS_CACHE.expire(session_id,60*60*24)

            self.merged_refresh.setdefault(db_key, []).append(task)
            #if len(self.merged_refresh.get(db_key)) > self.package_size:
            if len(self.merged_refresh.get(db_key)) > 60:
                logger.info('pop data  is {}'.format(self.merged_refresh.get(db_key)))
                self.update_refresh_result(self.merged_refresh.pop(db_key))
        else:
            REDIS_CACHE.hmset(session_id, insert_dict)
            REDIS_CACHE.expire(session_id, 60 * 24)

    def update_refresh_result(self, results):
        #logger.info('update_refresh_result is {}'.format(results))
        if not results:
            return
        #logger.info('update_refresh_result number:%s' % (len(results)))
        for result in results:
            timestr = result['time']
            result['time'] = datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S %f')
        #str_num = ''
        num_str = config.get('refresh_result', 'num')
        str_num = get_mongo_str(str(results[0].get('session_id')), num_str)

        logger.info('insert mongodb  is {}'.format(results))
        db_s1['refresh_result' + str_num].insert_many(results, ordered=False)
        logger.info('insert mongodb end')

        time.sleep(0.05)
    def update_refresh_result2(self, results):
        logger.info('update_refresh_result is {}'.format(results))
        if not results:
            return
        logger.info('update_refresh_result number:%s' % (len(results)))
        for result in results:
            timestr = result['time']
            result['time'] = datetime.strptime(timestr,'%Y-%m-%d %H:%M:%S %f')
        #str_num = ''
        num_str = config.get('refresh_result', 'num')
        str_num = get_mongo_str(str(results[0].get('session_id')), num_str)

        logger.info('insert mongodb  is {}'.format(results))
        db_s1['refresh_result' + str_num].insert_many(results, ordered=False)
        logger.info('insert mongodb end')

    def load_task(self, task):
        try:
            return json.loads(task)
        except Exception:
            logger.warn(task, exc_info=sys.exc_info())


if __name__ == "__main__":
    logger.info("router begining...")
    now = datetime.now()
    print (now)
    router = Refresh_router()
    router.run()
    end = datetime.now()
    print (end - now)
    logger.info("router end.")
    # message = queue.get("result_task", 1000)
    # print(message)