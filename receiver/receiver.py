# -*- coding:utf-8 -*-
import tornado.ioloop
import tornado.web
import pika
# from pika.adapters.tornado_connection import TornadoConnection
import logging
from pika import adapters
import json
from datetime import datetime
import time
import traceback
from core.config import config

TORNADO_PORT = 11111

IOLOOP_TIMEOUT = 500

# configure my logger
# logging.config.fileConfig('log.conf')
# logger= log ipging.getLogger(__name__)

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log等级总开关
# 第二步，创建一个handler，用于写入日志文件
logfile = '/Application/rep_preload/logs/receiver.log'
fh = logging.FileHandler(logfile, mode='a')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 第三步，定义handler的输出格式
formatter = logging.Formatter(
    "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 第四步，将logger添加到handler里面
logger.addHandler(fh)

# holds channel objects
channel = None


class PikaClient(object):
    # all the following functions precede in order starting with connect

    def connect(self):
        try:
            # logger = logging.getLogger('rmq_tornado')
            # credentials = pika.PlainCredentials(RMQ_USER, RMQ_PWD)
            # param = pika.ConnectionParameters(host=RMQ_HOST, port=RMQ_PORT, credentials=credentials)
            # param = pika.URLParameters("amqp://bermuda:bermuda@223.202.203.118:5672/%2F?connection_attempts=3&heartbeat_interval=3600")
            rabbit_server = config.get('rabbitmq', 'host')
            amqp_url = "amqp://bermuda:bermuda@{0}/%2F?connection_attempts=3&heartbeat_interval=3600".format(
                rabbit_server)
            param = pika.URLParameters(amqp_url)
            # self.connection = TornadoConnection(param, on_open_callback=self.on_connected)
            self.connection = adapters.TornadoConnection(param, self.on_connected)
        except Exception:
            logger.error('PikaClient[error]: %s' % (traceback.format_exc()))

    def on_connected(self, connection):
        """When we are completely connected to rabbitmq this is called"""

        logger.info('Succesfully connected to rabbitmq')

        # open a channel
        self.connection.channel(self.on_channel_open)

    def on_channel_open(self, new_channel):
        """When the channel is open this is called"""
        logging.info('Opening channel to rabbitmq')

        global channel
        channel = new_channel


class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/content/preload_report", PreloadReport),
        ]

        tornado.web.Application.__init__(self, handlers)


class PreloadReport(tornado.web.RequestHandler):

    def get(self):
        self.write("Tornado web server. Post a message to it using 'message' as a parameter. The message will then be published to a rabbitmq queue.")

    def post(self):
        '''
        report_body
        {"download_mean_rate":"547.851562","lvs_address":"61.168.76.156","preload_status":"200","sessionid":"42f54840027f11e9b74400e081daef43","http_status":200,"response_time":"-","refresh_status":"failed","check_type":"BASIC","data":"-","last_modified":"-","url":"http://yqad.365noc.com:80/FTP/FTP_D/yx-4-zhangjiagang/1511_MaoDou_5S/1511_MaoDou_5S_jpg239/ASSETMAP","cache_status":"HIT","url_id":"5c187a9937d015dd8802734d","content_length":1683,"report_ip":"223.202.203.31","report_port":"80","origin_remote_ip":"111.200.33.194","status":"Done."}
        '''
        try:
            data = json.loads(self.request.body)
            remote_addr = self.request.headers.get('X-Real-Ip')

            logger.info('PreloadReport[post] body: {}|| remote_addr: {}'.format(data, remote_addr))

            report_body = self.get_fc_report_json(data)

            if report_body['lvs_address']:
                report_body['remote_addr'] = report_body['lvs_address']
            else:
                report_body['remote_addr'] = request.remote_addr

            r = channel.basic_publish(exchange='', routing_key='preload_report', body=json.dumps(report_body),
                                  properties=pika.BasicProperties(content_type="text/plain", delivery_mode=1))
            logger.info('PreloadReport[post|receiverDone.] r: {}'.format(r))
            self.write('ok')
        except Exception:
            logger.error('PreloadReport[post] error: {}'.format(traceback.format_exc()))
            self.set_status(500, reason='error')

    def get_fc_report_json(self, body):
        '''
        解析json
        '''
        try:
            preload_result = {}
            preload_result['sessionid'] = body.get('sessionid', '')
            preload_result['preload_status'] = int(body.get('preload_status', 0))
            preload_result['url_id'] = body.get('url_id', '')
            preload_result['download_mean_rate'] = body.get('download_mean_rate', '')
            preload_result['cache_status'] = body.get('cache_status', '')
            preload_result['http_status_code'] = body.get('http_status_code', body.get('http_status', ''))
            preload_result['response_time'] = body.get('response_time', '')
            preload_result['last_modified'] = body.get('last_modified', '')
            preload_result['content_length'] = body.get('content_length', '')
            preload_result['check_type'] = body.get('check_type', '')
            preload_result['check_result'] = body.get('check_result', '')
            preload_result['lvs_address'] = body.get('lvs_address', '')
            preload_result['finish_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            preload_result['final_status'] = body.get('status', '')
            preload_result['origin_remote_ip'] = body.get('origin_remote_ip', '')
            return preload_result

        except Exception:
            logger.error("PreloadReport[get_fc_report_json] error: {}".format(traceback.format_exc()))
            raise ValueError("PreloadReport[get_fc_report_json] error.")


application = Application()


def run(thePort):
    application.pika = PikaClient()

    # application.listen(TORNADO_PORT)
    application.listen(thePort)

    ioloop = tornado.ioloop.IOLoop.instance()

    ioloop.add_timeout(IOLOOP_TIMEOUT, application.pika.connect)

    ioloop.start()


if __name__ == "__main__":
    application.pika = PikaClient()

    application.listen(TORNADO_PORT)

    ioloop = tornado.ioloop.IOLoop.instance()

    ioloop.add_timeout(IOLOOP_TIMEOUT, application.pika.connect)

    ioloop.start()
