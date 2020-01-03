#!/usr/bin/env python
# -*- coding: utf-8 -*-

import simplejson as json
import sys
import traceback
import logging
import time
from datetime import datetime
import copy
import uuid

from core import my_queue as queue
from core import database
from core.config import config
from core import auth_redis
from util.preloadTools import db_update, doPost
from util.preloadTools import send as send_mail
from util.preloadTools import ObjectId, AioClient, doTheLoop, getPostStatus


exec('log_debug = %s' % (config.get('routerLogLevel', 'log_debug'), ))
exec('log_info = %s' % (config.get('routerLogLevel', 'log_info'), ))

logger = logging.getLogger()
logger.setLevel(log_debug)
logfile = '/Application/rep_preload/logs/preload_router.log'
fh = logging.FileHandler(logfile, mode='a')
fh.setLevel(log_info)
formatter = logging.Formatter(
    "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

s1_db = database.s1_db_session()
db = database.db_session()

PRELOAD_CACHE = auth_redis.getBR(1)
# PRELOAD_DEVS = auth_redis.getBR(5)

PRELOAD_STATUS_SUCCESS = 200
STATUS_CONNECT_FAILED = 503
PRELOAD_STATUS_MD5_DIFF = 406
PRELOAD_STATUS_FAILED = 500
STATUS_UNPROCESSED = 0


class PreloadRouter(object):

    def __init__(self, batch_size=3000, package_size=60):
        self.batch_size = batch_size
        self.package_size = package_size
        self.tasksByUrlID = {}
        self.errorTasks = {}

    def run(self):
        logger.info("PreloadRouter.start")
        self.preload_router()
        logger.info("PreloadRouter.end")

    def preload_router(self, queue_name='preload_report'):
        '''
        回调数据打包
        '''
        messages = queue.get(queue_name, self.batch_size)
        if not messages:
            return

        urlDict = {}
        for message in messages:
            report_body = json.loads(message)
            url_id = report_body['url_id']
            if urlDict.get(url_id) is None:
                urlDict[url_id] = 1
            else:
                urlDict[url_id] += 1
        logger.info("preload_router[urlAndNumberOfItsReport] urlDict: %s" % (urlDict, ))

        #---devInfosInitialization
        for url_id in urlDict.keys():
            self.tasksByUrlID[url_id] = self.get_preload_dev_cache(url_id)

        for message in messages:
            report_body = json.loads(message)

            #---merge them before doOtherThings.
            self.merge_tasks(report_body)

            # self.save_fc_report(report_body)

        if self.errorTasks:
            logger.warn('merge_tasks[reportError.] errorTasks: %s' % self.errorTasks)

        logger.info('preload_router tasksByUrlID: %s' % self.tasksByUrlID)
        for url_id, devsMap in self.tasksByUrlID.items():
            k_dev = '%s_dev' % url_id
            rHmset = PRELOAD_CACHE.hmset(k_dev, devsMap)
            rExpire = PRELOAD_CACHE.expire(k_dev, int(3600*24*5.5))
            logger.info('preload_router k_dev: %s|| rHmset: %s|| rExpire: %s' % (k_dev, rHmset, rExpire))

    def merge_tasks(self, task):
        '''
        task:
        {"sessionid": "aadc97bc536311e986d2001b21bdf684", "preload_status": 200, "url_id": "5ca0318a37d015de93a77bc2", "download_mean_rate": "33583.734962", "cache_status": "HIT", "http_status_code": 200, "response_time": "-", "last_modified": "-", "content_length": 36625078, "check_type": "BASIC", "check_result": "", "lvs_address": "116.242.1.108", "finish_time": "2019-03-31 11:18:35", "final_status": "Done.", "origin_remote_ip": "111.200.33.193", "remote_addr": "116.242.1.108"}
        '''
        url_id = task.get('url_id')
        host = task.get('remote_addr')
        preload_status = task.get('preload_status')

        if url_id is None:
            logger.info('merge_tasks [wrongData.] url_id not exists!')
            return

        logger.info('merge_tasks [beforeUpdate.] tasksByUrlID: %s' % (self.tasksByUrlID, ))
        if preload_status == PRELOAD_STATUS_SUCCESS:
            self.tasksByUrlID[url_id][host].update(task)
        else:
            self.errorTasks.setdefault(url_id, {}).update({errorTasks[url_id][host]: task})
        logger.info('merge_tasks [afterUpdate.] tasksByUrlID: %s' % (self.tasksByUrlID, ))

    def save_fc_report(self, report_body):
        try:
            logger.info("save_fc_report host: %s|| url_id: %s|| report_body: %s" %
                        (report_body.get("remote_addr"), report_body.get("url_id"), report_body))
            remote_addr = report_body.get("remote_addr")
            url_id = report_body.get("url_id")
            cache_body = PRELOAD_CACHE.get(url_id)
            if cache_body:
                cache_body = json.loads(cache_body)
            dev_cache = self.get_preload_dev_cache(url_id, host=remote_addr)
            logger.info('<1>save_fc_report dev_cache: %s|| cache_body: %s' %
                        (dev_cache, cache_body))

            if not cache_body or not dev_cache:
                logger.info("save_fc_report url_id: %s|| [No cache_body: %s] or [No dev_cache: %s]" % (
                    url_id, cache_body, dev_cache))
                return

            if report_body.get('preload_status') == PRELOAD_STATUS_SUCCESS and (
                    cache_body.get('check_type') == 'MD5' or cache_body.get('check_type') == 'BASIC'):
                logger.info("save_fc_report [SUCCESS] url_id: %s|| remote_addr: %s" % (
                    report_body.get("url_id"), report_body.get("remote_addr")))
                self.reset_preload_dev_cache(cache_body, dev_cache, report_body)
                logger.info('<2>save_fc_report url_id: %s|| dev_cache: %s|| cache_body: %s' %
                            (url_id, dev_cache, cache_body))
                if cache_body.get('first_layer_first') and cache_body.get('f_and_nf'):
                    self.nf_tasks_begin(url_id, cache_body)
                if cache_body.get('status') in ['PROGRESS']:
                    self.save_result(url_id, cache_body)
                    # number of device less than 3, how to delete the cache.
                    dev_all = self.get_preload_dev_cache(url_id)
                    dev_f = [d for d in dev_all.values() if d.get('firstLayer')]
                    dev_nf = [d for d in dev_all.values() if not d.get('firstLayer')]
                    if (len(dev_f) < 3 and not dev_nf) or (not dev_f and len(dev_nf) < 3) or (len(dev_f) < 3 and len(dev_nf) < 3):
                        self.recycle_redis_task_dev_cache(url_id)
                else:
                    self.recycle_redis_task_dev_cache(url_id)
                # 删除错误任务
                # s1_db.preload_error_task.remove(
                #     {"url_id": str(report_body.get("url_id")), "host": remote_addr})
            else:
                logger.warn("save_fc_report [handle_error_task!!!] report_body: %s|| cache_body: %s|| dev_cache: %s" % (
                    report_body, cache_body, dev_cache))
                # handle_error_task(report_body, cache_body, dev_cache)
                pass
        except Exception:
            logger.error("save_fc_report error: %s " % traceback.format_exc())

    def get_preload_dev_cache(self, url_id, host=None):
        '''
        获取任务设备缓存
        '''
        res_dict = {}
        if host:
            res = PRELOAD_CACHE.hget("%s_dev" % (url_id), host)
            if res:
                res_dict = json.loads(res)
            return res_dict
        else:
            res = PRELOAD_CACHE.hgetall("%s_dev" % (url_id))
            if res:
                for k, v in res.items():
                    res_dict[k] = json.loads(v)
            return res_dict

    def reset_preload_dev_cache(self, cache_body, dev_cache, report_body, is_failed=False):
        """
        重置cache内容
        :param cache_body:
        :param report_body:
        :param is_failed:
        """
        if is_failed:
            dev_cache["preload_status"] = STATUS_CONNECT_FAILED
        else:
            # explanation , if basic, return report status 200,
            # if check_type is MD5     cache_body md5 == report_body md5 return report
            # 200, not eq return 406
            dev_cache["preload_status"] = report_body.get("preload_status") if cache_body.get(
                'check_type') == 'BASIC' or cache_body.get('check_result') == report_body.get(
                'check_result') else PRELOAD_STATUS_MD5_DIFF
            self.set_value(report_body, dev_cache, ["content_length", "check_result", "check_type", "last_modified",
                                                    "cache_status", "http_status_code", "download_mean_rate",
                                                    "finish_time", "final_status"])
        url_id = report_body.get('url_id')
        self.update_preload_dev_cache(url_id, report_body.get('remote_addr'), dev_cache)

    def set_value(self, source_dict, new_dict, keys):
        for key in keys:
            if source_dict.get(key):
                new_dict[key] = source_dict.get(key)

    def update_preload_dev_cache(self, url_id, host, value):
        '''
        dev缓存更新
        '''
        logger.info("update_preload_dev_cache begin url_id: %s|| host: %s|| value: %s " %
                    (url_id, host, value))
        if isinstance(value, dict):
            save_value = json.dumps(value)
        else:
            save_value = value
        s_key = '%s_dev' % (url_id)
        r_dev_hset = PRELOAD_CACHE.hset(s_key, host, save_value)
        r_dev_expire = PRELOAD_CACHE.expire(s_key, int(3600 * 24 * 5.5))
        logger.info("update_preload_dev_cache end url_id: %s|| host: %s|| value:%s|| r_dev_hset: %s|| r_dev_expire: %s" %
                    (url_id, host, PRELOAD_CACHE.hget(s_key, host)))

    def nf_tasks_begin(self, url_id, cache_body):
        """
        make the tasks of non-first-layer begin.
        """
        try:
            # Whether non-first-layer-device tasks begin.
            devices = self.get_preload_dev_cache(url_id)
            dev_list = devices.values()
            preload_200_num = 0
            for dev in dev_list:
                if dev.get('preload_status') in [200, '200', 406, '406']:
                    preload_200_num += 1

            dev_first_num = len([d for d in dev_list if d.get('firstLayer')])
            logger.info("nf_tasks_begin url_id: %s|| dev_first_num: %s|| preload_200_num: %s" %
                        (url_id, dev_first_num, preload_200_num))

            nf_begin = False
            if cache_body.get('first_layer_first') and preload_200_num >= (dev_first_num * 0.6):
                nf_begin = PRELOAD_CACHE.setnx('%s_lock' % (url_id, ), 1)
                PRELOAD_CACHE.expire('%s_lock' % url_id, int(3600 * 24 * 5.5))

                logger.info("nf_tasks_begin [first_layer_first!!!preload_200_num(%s) >= dev_first_num(%s)] url_id: %s|| nf_begin: %s" % (
                    preload_200_num, dev_first_num, url_id, nf_begin))
            if nf_begin:
                results = []
                urls = [u for u in s1_db.preload_url.find({'_id': ObjectId(url_id)})]
                dev_id = urls[0].get('dev_id')
                devs = s1_db.preload_dev.find_one({'_id': dev_id})
                # logger.info("nf_tasks_begin type(devs.get('devices')): %s|| devs.get('devices'): %s" % (type(devs.get('devices')), devs.get('devices')))
                pre_devs = devs.get('devices')
                layer_dev = [dev for dev in pre_devs.values() if not dev.get("firstLayer")]

                if layer_dev:
                    f_res = self.send(layer_dev, urls)
                    if f_res:
                        results += f_res

                logger.info("nf_tasks_begin [nf_begin sendDone.] url_id: %s|| nf_begin: %s|| results: %s" % (url_id, nf_begin, results, ))

                self.update_db_dev(dev_id, results)
        except Exception:
            logger.error("nf_tasks_begin [error]: %s" % traceback.format_exc())

    def update_db_dev(self, dev_id, results):
        """
         更新任务状态,preload_dev
        :param dev_id:
        :param results:
        """
        try:
            now = datetime.now()
            db_dev = s1_db.preload_dev.find_one({"_id": ObjectId(dev_id)})
            db_dev["finish_time"] = now
            db_dev["finish_time_timestamp"] = time.mktime(now.timetuple())
            devices = db_dev.get("devices")
            logger.info("update_db_dev dev_id: %s|| results: %s" % (dev_id, results))
            for ret in results:
                devices.get(ret["name"])["code"] = ret.get("code", 0)
                devices.get(ret["name"])["a_code"] = ret.get("a_code", 0)
                devices.get(ret["name"])["r_code"] = ret.get("r_code", 0)
                unprocess_num = int(db_dev.get('unprocess'))
                db_dev["unprocess"] = unprocess_num - 1 if unprocess_num > 0 else 0

            logger.info("update_db_dev dev_id: %s|| db_dev: %s" % (dev_id, db_dev))
            try:
                s1_db.preload_dev.save(db_dev)
            except:
                # 3.0.3 pymongo update
                update_dev = copy.deepcopy(db_dev)
                if '_id' in update_dev:
                    update_dev.pop('_id')
                s1_db.preload_dev.update_one({"_id": ObjectId(dev_id)}, {'$set': update_dev})
                logger.info("update_db_dev dev_id: %s|| update_dev: %s" % (dev_id, update_dev))
        except Exception:
            logger.error("update_db_dev[error]: %s" % traceback.format_exc())

    def save_result(self, url_id, cache_body):
        """
        存任务执行结果，只有没有未处理的设备才会结束
        :param url_id:
        :param cache_body:
        """
        logger.info("save_result begin url_id: %s" % url_id)
        if self.get_unprocess(cache_body, url_id):
            PRELOAD_CACHE.set(url_id, json.dumps(cache_body))
            PRELOAD_CACHE.expire(url_id, int(3600 * 24 * 5.5))
            logger.info("save_result not finished reset cache url_id: %s" % url_id)
        else:
            cache_body['status'] = 'FINISHED'
            logger.info("save_result url_id: %s|| cache_body: %s" % (url_id, cache_body))
            PRELOAD_CACHE.set(url_id, json.dumps(cache_body))
            PRELOAD_CACHE.expire(url_id, int(3600 * 24 * 5.5))
            self.set_finished(url_id, cache_body, 'FINISHED' if cache_body.get(
                'status') == 'PROGRESS' else cache_body.get('status'))
        logger.info("save_result end url_id: %s|| cache_body: %s" % (url_id, cache_body))

    def get_unprocess(self, cache_body, url_id):
        """
        统计未接收到返回的设备数与连接失败的设备数
        PRELOAD_STATUS_FAILED(500),STATUS_UNPROCESSED(0)
        如果上层设备100%，下层设备 80%即为完成
        :param cache_body:
        :return:
        """
        firstlayer_count = 0
        unprocess_firstlayer_count = 0
        un_firstlayer_count = 0
        unprocess_un_firstlayer_count = 0
        dev_cache = self.get_preload_dev_cache(url_id)
        logger.debug("get_unprocess url_id: %s|| cache_body: %s|| dev_cache: %s" % (url_id, cache_body, dev_cache, ))
        if not dev_cache:
            logger.warn("get_unprocess [error no dev_cache] url_id: %s" % (url_id, ))
            return 999

        for dev in dev_cache.values():

            if dev.get("firstLayer") in ["true", "True", True]:
                firstlayer_count += 1
                if dev.get("preload_status") in [PRELOAD_STATUS_FAILED, STATUS_UNPROCESSED, STATUS_CONNECT_FAILED]:
                    unprocess_firstlayer_count += 1
            else:
                un_firstlayer_count += 1
                if dev.get("preload_status") in [PRELOAD_STATUS_FAILED, STATUS_UNPROCESSED, STATUS_CONNECT_FAILED]:
                    unprocess_un_firstlayer_count += 1
        try:
            cache_body['unprocess'] = unprocess_firstlayer_count + unprocess_un_firstlayer_count
            # 只有上层设备时,回调成功设备数量大于60%即置任务状态为FINISHED
            if firstlayer_count > 0 and un_firstlayer_count == 0:
                if (unprocess_firstlayer_count / float(firstlayer_count)) < 0.4:
                    cache_body['unprocess'] = 0
                else:
                    cache_body['unprocess'] = unprocess_firstlayer_count
            # 只有下层设备时,回调成功设备数量大于50%即置任务状态为FINISHED
            elif firstlayer_count == 0 and un_firstlayer_count > 0:
                if (unprocess_un_firstlayer_count / float(un_firstlayer_count)) < 0.5:
                    cache_body['unprocess'] = 0
                else:
                    cache_body['unprocess'] = unprocess_un_firstlayer_count
            # 上下层设备均有时,回调成功上层设备数量大于60%且下层回调成功设备数量大于50%时,即置任务状态为FINISHED
            elif firstlayer_count > 0 and un_firstlayer_count > 0:
                if ((unprocess_firstlayer_count / float(firstlayer_count)) < 0.4) and ((unprocess_un_firstlayer_count / float(un_firstlayer_count)) < 0.5):
                    cache_body['unprocess'] = 0
                else:
                    cache_body['unprocess'] = unprocess_firstlayer_count + unprocess_un_firstlayer_count
            else:
                cache_body['unprocess'] = unprocess_firstlayer_count + unprocess_un_firstlayer_count

            logger.info('get_unprocess url_id: %s|| unprocess: %s|| un_firstlayer_count: %s|| unprocess_un_firstlayer_count: %s' % (
                url_id, cache_body['unprocess'], un_firstlayer_count, unprocess_un_firstlayer_count))
            return cache_body.get('unprocess')
        except Exception:
            logger.error('get_unprocess[error]: url_id: %s|| %s' % (url_id, traceback.format_exc(), ))
            return 999

    def set_finished(self, url_id, cache_body, status):
        """
        处理URL结果存入DB
        """
        logger.info("set_finished begin url_id: %s|| cache_body: %s|| status: %s " %
                    (url_id, cache_body, status))
        now = datetime.now()
        now_t = time.mktime(now.timetuple())
        db_update(s1_db.preload_url, {'_id': ObjectId(url_id)}, {
            "$set": {'status': status, 'finish_time': now, 'finish_time_timestamp': now_t}})
        dev_cache = self.get_preload_dev_cache(url_id)
        if not dev_cache:
            logger.info("set_finished [no dev_cache] url_id: %s" % (url_id))
            return

        cache_body['_id'] = ObjectId(url_id)
        cache_body['status'] = status
        save_body = copy.deepcopy(cache_body)
        save_body['devices'] = {v['name']: v for v in dev_cache.values()}
        save_body['created_time'] = datetime.now()
        try:
            s1_db.preload_result.insert_one(save_body)
        except Exception:
            # logger.info("set_finished insert error: %s|| url_id: %s " % (traceback.format_exc(e), url_id))
            pass

        task_info = self.get_information(dev_cache)
        preload_info = s1_db.preload_url.find_one({"_id": ObjectId(url_id)})
        logger.info("set_finished url_id: %s|| dev_cache: %s|| task_info: %s|| preload_info: %s" % (
            url_id, dev_cache, task_info, preload_info))
        # prevent from mailing more than once.
        has_callback_lock = False
        has_callback_lock = PRELOAD_CACHE.setnx('%s_callback_lock' % (url_id, ), 1)
        PRELOAD_CACHE.expire('%s_callback_lock' % url_id, int(3600 * 24 * 5.5))
        if has_callback_lock:
            self.preload_callback(
                db.preload_channel.find_one({"channel_code": cache_body.get(
                    "channel_code"), "username": preload_info.get("username")}),
                {"task_id": cache_body.get('task_id'), "status": cache_body.get(
                    'status'), "percent": task_info.get('percent')},
                task_info, id=cache_body.get("_id")
            )
        logger.info("set_finished end url_id: %s|| status: %s|| has_callback_lock: %s" %
                    (url_id, status, has_callback_lock))

    def preload_callback(self, channel, callback_body, info={}, id=None):
        """
        任务处理完成的汇报，处理预加载结果，调用提交的callback(email,url)
        """
        try:
            content = {}
            content['username'] = channel.get('username', '')
            content['uid'] = callback_body.get('_id', '')
            if id:
                content['uid'] = id
            if channel.get('callback', {}).get('email'):
                email = {}
                try:
                    send_mail(channel.get('callback').get('email'),
                              'preload callback', str(callback_body))
                    logger.info("preload_callback email task_id: %s" %
                                (callback_body.get("task_id")))
                    email['email_url'] = channel.get('callback').get('email')
                    email['code'] = 200
                except Exception:
                    logger.info("preload_callback email task_id: %s|| error: %s" %
                                (callback_body.get("task_id"), traceback.format_exc()))
                    email['code'] = 0
                email['send_times'] = 1

                content['email'] = email

            if channel.get('callback', {}).get('url'):
                url = {}
                url['email_url'] = []
                url['email_url'].append(channel.get('callback').get('url'))
                status = doPost(channel.get('callback').get('url'), json.dumps(callback_body))
                url['send_times'] = 1
                logger.info("preload_callback task_id: %s|| status: %s|| dest_url: %s" % (
                    callback_body.get("task_id"), status, channel.get('callback', {}).get('url')))
                if status != 200:
                    for i in range(3):
                        status = doPost(channel.get('callback').get(
                            'url'), json.dumps(callback_body))
                        logger.info('preload_callback [retryNo.%s] status: %s|| url: %s' % (i,
                                                                                            status, channel.get('callback').get('url')))
                        url['send_times'] = i + 1
                        if status == 200:
                            break
                url['code'] = status
                content['url'] = url

            try:
                if content.get('email') or content.get('url') or content.get('snda_portal'):
                    content['datetime'] = datetime.now()
                    db.email_url_result.insert(content)
            except Exception:
                logger.info('preload_callback insert email_url_result error: %s' %
                            traceback.format_exc())
        except Exception:
            logger.error("preload_callback url_id: %s|| error: %s" %
                         (callback_body.get("url_id"), traceback.format_exc()))

    def recycle_redis_task_dev_cache(self, url_id):
        """
        delete the cache after using when the condition is satisfied and save the data.
        """
        try:
            devices = self.get_preload_dev_cache(url_id)
            dev_list = devices.values()
            preload_200_num = 0
            for dev in dev_list:
                if dev.get('preload_status') in [200, '200']:
                    preload_200_num += 1

            dev_total_num = len(dev_list)
            logger.info("recycle_redis_task_dev_cache url_id: %s|| dev_total_num: %s|| preload_200_num: %s" % (
                url_id, dev_total_num, preload_200_num))
            update_body = PRELOAD_CACHE.get(url_id)
            if update_body is None:
                logger.info("recycle_redis_task_dev_cache [url_id: %s] update_body is None, returned." % (url_id, ))
                return
            logger.info("type(update_body): %s|| update_body: %s" %
                        (type(update_body), update_body))
            update_body = json.loads(update_body)
            status = 'FINISHED'
            update_body['status'] = status
            update_body['devices'] = {v['name']: v for v in devices.values()}
            update_body['created_time'] = datetime.now()
            # logger.info("recycle_redis_task_dev_cache url_id: %s|| update_body: %s" % (url_id, update_body, ))

            #-# 用于将达到阈值后（任务状态为FINISHED）的设备缓存状态同步到mongodb中的preload_result表中
            try:
                db_update(s1_db.preload_result, {'_id': ObjectId(url_id)}, {
                          "$set": {'devices': update_body['devices']}})
            except Exception:
                logger.info("recycle_redis_task_dev_cache [preload_resultUpdating(url_id: %s) error]: %s" % (
                    url_id, traceback.format_exc(), ))

            if preload_200_num and dev_total_num == preload_200_num:
                try:
                    s1_db.preload_result.update_one({'_id': ObjectId(url_id)}, {
                                                    '$set': update_body}, upsert=True)
                    db_update(s1_db.preload_url, {'_id': ObjectId(url_id)}, {
                              "$set": {'status': status}})
                except Exception:
                    logger.info("recycle_redis_task_dev_cache insert error: %s|| url_id: %s " % (
                        traceback.format_exc(), url_id))
                PRELOAD_CACHE.delete(url_id)
                PRELOAD_CACHE.delete('%s_dev' % (url_id))
                PRELOAD_CACHE.delete('%s_lock' % (url_id))
                PRELOAD_CACHE.delete('%s_callback_lock' % (url_id))
                logger.info(
                    "recycle_redis_task_dev_cache [cache deleted successfully.] url_id: %s" % (url_id, ))
        except Exception:
            logger.error("recycle_redis_task_dev_cache [error]: %s" % traceback.format_exc())

    def get_information(self, cache_body):
        """
        计算完成比例
        :param dev_cache: <type 'dict'>
        {'status': 'FINISHED', 'check_result': '', 'check_type': 'BASIC',
        'task_id': 'c32f4eaa-c56a-485e-99ba-0dfffa43bb37', 'lock': False,
        'devices': {'CHN-ZI-2-3g9': {'content_length': '-', 'check_result': '-',
        'name': 'CHN-ZI-2-3g9', 'download_mean_rate': '-', 'host': '222.186.47.10',
         'firstLayer': False, 'preload_status': 0, 'check_type': '-'},'unprocess': 99,
         '_id': ObjectId('53b4b5da3770e1604118b954'), 'channel_code': '56893'}
        :return:
        """
        count = 0
        percent = 0
        file_size = 0
        total_count = 0
        firstLayer_count = 0
        firstLayer_success = 0
        for dev in cache_body.values():
            if dev.get('firstLayer'):
                firstLayer_count += 1
            if dev.get('preload_status') in [200, 205]:
                file_size = dev.get('content_length', 0)
                try:
                    float(file_size)
                except Exception:
                    file_size = 0
                if dev.get('firstLayer'):
                    firstLayer_success += 1
                count += 1
            total_count += 1
        # firstLayer_success =10
        # firstLayer_count =10
        # count =8
        # total_count = 10

        if total_count == 0:
            return {"percent": 0, "file_size": float(file_size), "total_count": total_count, "count": count}

        # first only
        if total_count - firstLayer_count == 0:
            if firstLayer_success / float(total_count) >= 0.6:
                percent = 100
        # nf only
        elif firstLayer_count == 0 and total_count > 0:
            if (count - firstLayer_success) / float(total_count) >= 0.5:
                percent = 100
        # first + nf
        elif firstLayer_count > 0 and (total_count - firstLayer_count) > 0:
            if (firstLayer_success / float(firstLayer_count)) >= 0.6 and ((count - firstLayer_success) / float(total_count - firstLayer_count)) >= 0.5:
                percent = 100
        else:
            percent = round(float(count) / (float(total_count) if total_count else 1), 2) * 100
        # if (firstLayer_count - firstLayer_success) == 0 and (True if ((total_count - firstLayer_count) == 0) else (
        #             round((float(count - firstLayer_success) / (float(total_count - firstLayer_count))), 2) >= 0.8)):
        #     percent = 100
        # else:
        #     percent = round(float(count) / (float(total_count) if total_count else 1),2) * 100
        if total_count == 0:
            percent = 0
        # print {"percent": percent, "file_size": float(file_size), "total_count":
        # total_count, "count": count}
        return {"percent": percent, "file_size": float(file_size), "total_count": total_count, "count": count}

    # send tasks of nf
    def send(self, devs, urls):
        """
        发送命令到FC
        :param devs:
        :param command:
        :return:
        """
        try:
            dev_map = {}
            [dev_map.setdefault(d.get('host'), d) for d in devs]
            # logger.info("send dev_map: %s" % (dev_map, ))
            # original code
            # results = postal.process_loop_ret(postal.doloop(devs, urls), dev_map, "pre_ret")
            # the result of success and failure
            pre_ret, pre_ret_faild = self.doloop(devs, urls)
            results, error_result = self.process_loop_ret(pre_ret, dev_map)
            # process the result of failed
            pre_results_faild_dic = self.process_loop_ret_faild(pre_ret_faild, dev_map)
            # logger.info("send devs: %s,\ndev_map: %s,\nresults: %s,\npre_ret: %s,\npre_results_faild_dic: %s,\npre_ret_faild: %s" % (devs, dev_map, results, pre_ret, pre_ret_faild, pre_results_faild_dic))
            if dev_map:
                # the failure in the successful information    join the failure of the info list
                # results += postal.retry(dev_map.values(), urls, "pre_ret", pre_results_faild_dic)
                try:
                    pre_results_faild_dic.update(error_result)
                    retry_results = self.retry(dev_map.values(), urls, pre_results_faild_dic)
                except Exception:
                    logger.error(traceback.format_exc())
                results += retry_results

            # logger.info("send devs: %s,\n dev_map: %s,\n results: %s,\n error_result: %s,\n pre_results_faild_dic: %s" % (devs, dev_map, results, error_result, pre_results_faild_dic))
            return results
        except Exception:
            logger.error("send error: %s" % traceback.format_exc())

    def doloop(self, devs, urls, connect_timeout=5, response_timeout=8):
        """
        调用asyncore，创建信道，与FC 通过socket连接,端口31108
        :param devs:
        :param command:
        :return:
        """
        clients = []
        # http_results = []
        port = 31108
        pre_ret = []
        pre_ret_faild = []
        have_first_layer = any([d.get('firstLayer') for d in devs])
        for dev in devs:
            # logger.info("doloop have_first_layer: %s|| firstLayer: %s" %
            #             (have_first_layer, dev.get('firstLayer')))
            if have_first_layer:
                if dev.get('firstLayer'):
                    clients.append(AioClient(dev.get('host'), port, '', self.get_command_json(
                        urls, urls[0].get("action"), dev, check_conn=True), connect_timeout, response_timeout))
                else:
                    clients.append(AioClient(dev.get('host'), port, '', self.get_command_json(
                        urls, urls[0].get("action"), dev), connect_timeout, response_timeout))
            else:
                clients.append(AioClient(dev.get('host'), port, '', self.get_command_json(
                    urls, urls[0].get("action"), dev, check_conn=True), connect_timeout, response_timeout))

        results = doTheLoop(clients, logger)
        # results: [{'response_body':
        # '{"pre_ret_list":[{"code":200,"id":"5bdadaf13b7750a74daa0008"}],"sessionid":"3cb13f1eddc411e8afd8000c29629177"}',
        # 'total_cost': 0.0890894599724561, 'connect_cost': 0.029646086040884256,
        # 'response_cost': 0.05891382892150432, 'response_code': 200, 'host':
        # '61.147.92.6', 'strerror': 'no_error'}]

        for r in results:
            response_body, total_cost, connect_cost, response_cost, response_code = r.get('response_body'), r.get(
                'total_cost'), r.get('connect_cost'), r.get('response_cost'), r.get('response_code')

            if response_body:
                try:
                    pre_ret.append(r.get('host') + '\r\n' + response_body + '\r\n%d\r\n%.2f\r\n%.2f\r\n%.2f' % (
                        response_code, total_cost, connect_cost, response_cost))
                    # logger.warn("devs: %s doloop response_body: %s" % (devs, response_body))
                    logger.info("doloop pre_host: %s|| response_code: %s|| response_body: %s" % (
                        r.get('host'), response_code, response_body))
                except Exception:
                    logger.error("doloop [error] pre_devs: %s|| response_body: %s|| errorMsg: %s" %
                                 (devs, response_body, traceback.format_exc()))
            else:
                pre_ret_faild.append(r.get('host') + '\r\n%d\r\n%.2f\r\n%.2f\r\n%.2f' %
                                     (response_code, total_cost, connect_cost, response_cost))

        logger.info("doloop results: %s|| pre_ret: %s|| pre_ret_faild: %s" %
                    (results, pre_ret, pre_ret_faild))
        return pre_ret, pre_ret_faild

    def process_loop_ret(self, http_results, dev_map):
        """
        处理发送给FC后，FC返回的XML结果 parse
        :param http_results:
        :param dev_map:需要执行的设备
        :return:
        """
        results = []
        error_result = {}
        # logger.info("process_loop_ret http_results: %s|| dev_map: %s" % (http_results, dev_map))
        for result in http_results:
            try:
                host, body, a_code, total_cost, connect_cost, response_cost = result.split('\r\n')
                dev = dev_map.pop(host)
            except Exception:
                logger.info("process_loop_ret result has problem: %s|| error: %s" %
                            (result, traceback.format_exc()))
                host, str_temp = result.split('\r\n', 1)
                dev = dev_map.pop(host)
                results.append(getPostStatus(dev, 0, 0, 0, 0, 0, 0))
                continue
            try:
                has_error = False
                json_obj = json.loads(body)
                for _info in json_obj['pre_ret_list']:
                    _info_code = _info['code']
                    if _info_code == '404' or _info_code == '408':
                        dev_map.setdefault(host, dev)
                        has_error = True
                        error_result[host] = getPostStatus(
                            dev, int(_info_code), total_cost, connect_cost, response_cost, int(a_code))
                        logger.error("%s response error,code: %s" % (host, _info_code))
                        break

                if not has_error:
                    results.append(
                        getPostStatus(dev, int(json_obj['pre_ret_list'][0]['code']), total_cost, connect_cost, response_cost, int(a_code)))
            except Exception:
                dev_map.setdefault(host, dev)
                error_result[host] = getPostStatus(
                    dev, STATUS_CONNECT_FAILED, total_cost, connect_cost, response_cost, int(a_code))
                logger.error("process_loop_ret[error] host: %s|| body: %s|| error: %s" % (host, body, traceback.format_exc()))
        return results, error_result

    def process_loop_ret_faild(self, ret, dev_map):
        """
        处理发送给FC后，失败的设备文档内容
        :param ret:
        :return:
        """
        results = {}
        for result in ret:
            host, a_code, total_cost, connect_cost, response_cost = result.split('\r\n')
            dev = dev_map.pop(host)
            try:
                results[host] = getPostStatus(
                    dev, STATUS_CONNECT_FAILED, total_cost, connect_cost, response_cost, int(a_code))
                dev_map.setdefault(host, dev)
            except Exception:
                dev_map.setdefault(host, dev)
                logger.error('process_loop_ret_faild[error]: %s' % traceback.format_exc())
        return results

    def retry(self, devs, urls, pre_results_faild_dic):
        """
        失败后重新下发命令
        :param devs:
        :param command:
        :param node_name:
        :return:
        """

        ret_map = {}
        connect_timeout = 6
        response_timeout = 10
        for dev in devs:
            # original code
            # ret_map.setdefault(dev.get("host"), getPostStatus(dev, STATUS_CONNECT_FAILED))
            try:
                ret_map.setdefault(dev.get("host"), pre_results_faild_dic[dev.get("host")])
            except Exception:
                logger.error('retry[error] dev.get("host"): {0}|| pre_results_faild_dic: {1}'.format(dev.get("host"), pre_results_faild_dic))
        # for retry_count in range(RETRY_COUNT):
        for retry_count in range(3):
            # time.sleep(RETRY_DELAY_TIME)
            time.sleep(2)
            ret, ret_faild = self.doloop(devs, urls, connect_timeout, response_timeout)

            for result in ret:
                try:
                    host, body, a_code, total_cost, connect_cost, response_cost = result.split('\r\n')
                except Exception:
                    logger.error("retry[error] host: %s|| result: %s" % (host, result))
                try:
                    json_obj = json.loads(body)
                    ret_map[host]["code"] = int(json_obj['pre_ret_list'][0]['code'])
                    ret_map[host]["connect_cost"] = connect_cost
                    ret_map[host]["response_cost"] = response_cost
                    ret_map[host]["total_cost"] = total_cost
                    ret_map[host]["r_code"] = int(a_code)
                    ret_map[host]["times"] = 1
                    logger.info("retry[ret] host: %s|| body: %s|| a_code: %s" % (host, body, a_code))
                except Exception:
                    logger.error("retry[error] host: %s|| body: %s|| error: %s" % (host, body, traceback.format_exc()))

            for w in ret_faild:
                try:
                    host, a_code, total_cost, connect_cost, response_cost = w.split('\r\n')
                except Exception:
                    logger.error("retry[error] host: %s|| w: %s" % (host, w))
                try:
                    ret_map[host]["connect_cost"] = connect_cost
                    ret_map[host]["response_cost"] = response_cost
                    ret_map[host]["total_cost"] = total_cost
                    ret_map[host]["r_code"] = int(a_code)
                    ret_map[host]["times"] = 1
                    logger.info("retry[ret_faild] host: %s|| a_code: %s" % (host, a_code))
                except Exception:
                    logger.error("retry[error] host: %s|| error: %s" % (host, traceback.format_exc()))

            # not asyn to send task
            # if retry_send(ret_map, urls, node_name):
            #     break
        return list(ret_map.values())

    def get_command_json(self, urls, action, dev, check_conn=False):
        '''
        组装下发json
        '''
        logger.info('get_command_json urls: %s|| dev: %s' % (urls, dev))
        json_obj = {}
        try:
            sid = uuid.uuid1().hex
            json_obj['sessionid'] = sid
            json_obj['action'] = action
            json_obj['lvs_address'] = dev.get('host')
            json_obj['report_address'] = config.get('server', 'preload_report')
            json_obj['is_override'] = 1
            json_obj['switch_m3u8'] = dev.get('parse_m3u8_f', False) if dev.get(
                'firstLayer') else dev.get('parse_m3u8_nf', False)
            for url in urls:
                if not url.get('compressed'):
                    if not 'url_list' in json_obj:
                        json_obj['url_list'] = []
                    bowl = json_obj['url_list']
                else:
                    if not 'compressed_url_list' in json_obj:
                        json_obj['compressed_url_list'] = []
                    bowl = json_obj['compressed_url_list']
                url_id = str(url.get('_id'))
                _url = url.get('url')
                md5_value = url.get("md5")
                tmp = {}
                tmp['id'] = url_id
                tmp['url'] = _url
                tmp['origin_remote_ip'] = url.get('remote_addr')
                tmp['priority'] = url.get('priority')
                tmp['nest_track_level'] = url.get('nest_track_level')
                tmp['check_type'] = url.get('check_type')
                tmp['limit_rate'] = url.get('get_url_speed')
                tmp['preload_address'] = url.get('preload_address')
                tmp['header_list'] = url.get('header_list')
                if md5_value:
                    if check_conn:
                        tmp.update({'conn': url.get('conn_num', 0) if dev.get('type') == 'FC' else 0, 'rate': url.get(
                            'single_limit_speed', 0), 'check_value': md5_value})
                    else:
                        tmp.update({'check_value': md5_value})
                else:
                    if check_conn:
                        tmp.update({'conn': url.get('conn_num', 0) if dev.get('type') ==
                                    'FC' else 0, 'rate': url.get('single_limit_speed', 0)})
                bowl.append(tmp)

            logger.info('get_command_json json_obj: %s' % json_obj)
        except Exception:
            logger.info('get_command_json except: %s' % traceback.format_exc())
        return json.dumps(json_obj)


if __name__ == "__main__":
    logger.info("router begining...")
    now = datetime.now()
    print(now)
    router = PreloadRouter()
    router.run()
    end = datetime.now()
    print(end - now)
    logger.info("router end.")
    # message = queue.get("result_task", 1000)
    # print(message)
