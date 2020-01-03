import logging, time, traceback
from core import redisfactory



LOG_FILENAME = '/Application/rep_preload/logs/rep_tools.log'
# LOG_FILENAME = '/home/rubin/logs/bermuda_tools.log'
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(process)d - Line:%(lineno)d - %(message)s")
fh = logging.FileHandler(LOG_FILENAME)
fh.setFormatter(formatter)

logger = logging.getLogger('monitor_region_devs')
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

expire_time = 5 * 24 * 60 * 60
def delete_urlid_host(url_id, host, type_t ='HPCC'):
    """
    delete urlid_id_fc host
    Args:
        url_id_fc: key of reids 5
        host: the ip of device
        type_t: the type of device

    Returns:

    """
    try:
        if url_id and host:
            RESULT_REFRESH = redisfactory.getDB(5)
            url_id_type = 'URLID_' + str(url_id) + "_" + type_t
            # delete the value of set
            RESULT_REFRESH.srem(url_id_type, host)
            hosts_members = RESULT_REFRESH.smembers(url_id_type)
            if not hosts_members:
                url_id_type_r = url_id_type + "_R"
                rid_id_type = RESULT_REFRESH.get(url_id_type_r)
                # delete rid_id_fc  value url_id_fc
                RESULT_REFRESH.srem(rid_id_type, url_id_type)
                rid_id_fc_smembers = RESULT_REFRESH.smembers(rid_id_type)
                if not rid_id_fc_smembers:
                    # _F   finish time
                    RESULT_REFRESH.set(rid_id_type + '_F', time.time())
                    RESULT_REFRESH.expire(rid_id_type + '_F', expire_time)
    except Exception as e:
        logger.error('delete_fc_urlid_host error:%s' % traceback.format_exc(e))
        #logger.info('delete_fc_urlid_host url_id_fc:%s, host:%s' % (rid_id_type, host))
def get_mongo_str(str_number, num):
    """

    Args:
        str_number: str(ObjectId)
        num: the num of mongo

    Returns:str  the number of mongo

    """
    try:
        int_10 = int('0x' + str_number[:8], 16)
        return_t = int_10 % int(num)
        return str(return_t)
        # print int_10
    except Exception:
        logger.error("get_mongo_str error :%s" % traceback.format_exc())
        return None