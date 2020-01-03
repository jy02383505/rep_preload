#!/usr/bin/env python
# coding: utf-8

import traceback
import os
import time
from datetime import datetime, timedelta
from bson.objectid import ObjectId as BObjectId
import binascii
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import aiohttp
import asyncio


LOG_FILENAME = '/Application/rep_preload/logs/preloadTools.log'
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(process)d - Line:%(lineno)d - %(message)s")
fh = logging.FileHandler(LOG_FILENAME)
fh.setFormatter(formatter)

logger = logging.getLogger('preloadTools')
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

RETRY_COUNT = 3


def db_update(collection, find, modification):
    for retry_count in range(RETRY_COUNT):
        try:
            ret = collection.update(find, modification)
            if ret.get("updatedExisting") == True and ret.get("n") > 0:
                return
        except Exception:
            logger.error("db_update error: %s" % (traceback.format_exc()))


def ObjectId(oid=None):
    if oid:
        return BObjectId(oid)
    else:
        try:
            vv = os.urandom(5)
            uid = binascii.hexlify(vv).decode()
            id = BObjectId()
            new_id = str(id)[:14] + uid
            nid = BObjectId(new_id)
            return nid
        except:
            return BObjectId()


def send(to_addrs, subject, plainText, textType='txt', attachs=[]):
    email_obj = MIMEMultipart()
    if textType == 'txt':
        msg = MIMEText(plainText)
    elif textType == 'html':
        msg = MIMEText(plainText, 'html', 'utf-8')
    from_addr = 'noreply@chinacache.com'
    email_obj.attach(msg)
    email_obj['Subject'] = subject
    email_obj['From'] = from_addr
    email_obj['To'] = ','.join(to_addrs)

    try:
        if attachs:
            for _atta in attachs:
                _, atta_name = os.path.split(_atta)
                atta = MIMEText(open(_atta, 'rb').read(), 'base64', 'utf-8')
                atta['Content-Type'] = 'application/octet-stream'
                atta['Content-Disposition'] = 'attachment;filename="%s"' % atta_name
                email_obj.attach(atta)
    except Exception:
        logger.info('sendEmail error: %s' % (traceback.format_exc()))

    e_s = EmailSender(from_addr, to_addrs, email_obj)
    r_send = e_s.sendIt()
    if r_send != {}:

        try:
            #s = smtplib.SMTP('corp.chinacache.com')
            s = smtplib.SMTP('anonymousrelay.chinacache.com')
            s.ehlo()
            s.starttls()
            s.ehlo()
        except:
            logger.info("%s STARTTLS extension not supported by server" % email_obj['To'])
            pass
        # s.login('noreply', 'SnP123!@#')
        s.sendmail(from_addr, to_addrs, email_obj.as_string())
        s.quit()


def doPost(url, data):
    headers = {'content-type': 'application/json'}
    try:
        request = urllib.request.Request(url, data=data, headers=headers)
        response = urllib.request.urlopen(request)
        logger.info("doPost code: %s|| url: %s|| data: %s" %
                     (response.getcode(), url, data))
        return response.getcode()
    except Exception:
        logger.error("doPost url: %s|| error: %s. " % (url, traceback.format_exc()))
        return 0


class EmailSender:

    def __init__(self, from_addr, to_addrs, msg, passwd=None):
        self.from_addr = from_addr if '@' in from_addr else config.get('emailinfo', from_addr)
        self.to_addrs = to_addrs if isinstance(to_addrs, list) else [
            i for i in to_addrs.split(',') if i]

        self.passwd = passwd if passwd else config.get(
            'emailinfo', '{}_pwd'.format(from_addr[:from_addr.find('@')]))
        self.mail_server = config.get('emailinfo', 'mail_server')
        self.mail_port = int(config.get('emailinfo', 'mail_port'))

        self.msg = msg
        logger.info("EmailSender from_addr: %s|| passwd: %s|| to_addrs(%s): %s" %
                    (self.from_addr, self.passwd, type(self.to_addrs), self.to_addrs))

    def sendIt(self):
        try:
            s = smtplib.SMTP_SSL(self.mail_server, self.mail_port)
            s.ehlo()
            r_login = s.login(self.from_addr, self.passwd)
            r_send = s.sendmail(self.from_addr, self.to_addrs, self.msg.as_string())
            s.quit()
            logger.info("EmailSender [done: sendIt] r_login: %s|| r_send: %s" % (r_login, r_send))
            return r_send  # {} normally
        except smtplib.SMTPException:
            logger.info("EmailSender [error]: %s" % (traceback.format_exc(), ))


class AioClient:

    def __init__(self, host=None, port=None, path=None, body=None, connect_timeout=1.5, response_timeout=1.5, logger=None, dev=None, url=None):
        self.host = host
        self.port = port
        self.path = path
        self.body = body
        self.connect_timeout = connect_timeout
        self.response_timeout = response_timeout

        self.logger = logger  # optional
        self.dev = dev  # for user: autodesk
        self.url = url  # for user: autodesk

        self.strerror = 'no_error'
        self.response_code = 200
        self.total_cost = 0
        self.connect_cost = 0
        self.response_cost = 0
        self.request_start_time = 0
        self.request_end_time = 0

    def makeUrl(self):
        if self.host and self.port:
            url = 'http://{}:{}'.format(self.host, self.port) if not self.host.startswith(
                'http') else '{}:{}'.format(self.host, self.port)
            if self.path:
                url = 'http://{}:{}{}'.format(self.host, self.port,
                                              self.path if self.path.startswith('/') else '/' + self.path)
        elif self.url:
            url = self.url if self.url.startswith('http') else 'https://' + self.url
        return url

    async def doget(self, session, url):
        async with session.get(url) as response:
            return await response.text()

    async def dohead(self, session, url):
        async with session.head(url) as response:
            return await response.text()

    async def dopost(self, session, url, body):
        async with session.post(url, data=bytes(body, encoding='utf-8')) as response:
            # async with session.post(url, data=bytes(json.dumps(body), encoding='utf-8') if isinstance(body, dict) else bytes(body)) as response:
            # async with session.post(url, json=body, headers={'greetings': 'HHHHHHHHHello.'}) as response:
            # async with session.post(url, json=body) as response:
            # async with session.post(url, data=json.dumps(body),
            # headers={'greetings': 'nihao'}) as response:
            return await response.text()

    async def on_connection_create_start(self, session, trace_config_ctx, params):
        self.connection_create_start_time = session.loop.time()

    async def on_connection_create_end(self, session, trace_config_ctx, params):
        self.connection_create_end_time = session.loop.time()
        if self.connection_create_end_time - self.connection_create_start_time >= self.connect_timeout:
            self.response_code = 503
            session.close()

    async def on_request_start(self, session, trace_config_ctx, params):
        self.request_start_time = session.loop.time()

    async def on_request_end(self, session, trace_config_ctx, params):
        self.request_end_time = session.loop.time()
        if self.request_end_time - self.connection_create_end_time >= self.response_timeout:
            self.response_code = 501
            session.close()

    async def on_request_exception(self, session, trace_config_ctx, params):
        self.response_code = 502
        # self.strerror = 'errors occured: {}'.format(self.exception)

    async def main(self, methodType='POST'):
        # amount of connections simultaneously limit default: 100;no limitation: 0
        conn = aiohttp.TCPConnector(limit=200)
        timeout = aiohttp.ClientTimeout(
            sock_connect=self.connect_timeout, sock_read=self.response_timeout)

        trace_config = aiohttp.TraceConfig()
        trace_config.on_connection_create_start.append(self.on_connection_create_start)
        trace_config.on_connection_create_end.append(self.on_connection_create_end)
        trace_config.on_request_start.append(self.on_request_start)
        trace_config.on_request_end.append(self.on_request_end)
        trace_config.on_request_exception.append(self.on_request_exception)

        async with aiohttp.ClientSession(timeout=timeout, connector=conn, headers={'greetings': 'hello_world.'}, trace_configs=[trace_config]) as session:
            # r = await fetch(session, 'http://localhost:55555/hello/lym')
            # return r

            result = {}
            if methodType.lower() == 'post':
                result['response_body'] = await self.dopost(session, self.makeUrl(), self.body)
            elif methodType.lower() == 'get':
                result['response_body'] = await self.doget(session, self.makeUrl())
            elif methodType.lower() == 'head':
                result['response_body'] = await self.dohead(session, self.makeUrl())

            result['total_cost'] = self.request_end_time - self.request_start_time
            result['connect_cost'] = self.connection_create_end_time - \
                self.connection_create_start_time
            result['response_cost'] = self.request_end_time - self.connection_create_end_time
            result['response_code'] = self.response_code
            result['host'] = self.host
            result['strerror'] = self.strerror
            result['dev'] = self.dev  # special for user: autodesk
            return result


# loop = asyncio.new_event_loop()


def doTheLoop(clients, logger):
    logger.info('doTheLoop [LOOP STARTING...] len(clients): %s' % (len(clients), ))

    # loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(asyncio.wait([c.main() for c in clients]))
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()
    logger.info('doTheLoop [LOOP CLOSED.] len(clients): %s' % (len(clients), ))

    # loop = AsyncIOMainLoop()
    # results = loop.run_until_complete(asyncio.wait([c.main() for c in clients]))
    # loop.run_until_complete(asyncio.sleep(0.250))
    # loop.close()

    logger.info('doTheLoop [LOOP DONE...] results: {}'.format(results))
    return [i.result() for i in results[0]]


def getPostStatus(dev, statusCode, total_cost=0, connect_cost=0, response_cost=0, a_code=200, r_code=200):
    return {"host": dev.get('host'), "firstLayer": dev.get('firstLayer'),
            "name": dev.get('name'), "code": statusCode, "total_cost": total_cost, "connect_cost": connect_cost,
            "response_cost": response_cost, "a_code": a_code, "r_code": r_code, "times": 0}
