# -*- coding:utf-8 -*-
"""
Created on 2017-11-21

"""
import os
import logging
from core.preloadReport import PreloadRouter
#from util import log_utils
import time
import threading
import traceback


logger = logging


def run():
    logger.info("preload begining...")
    # while True:
    try:
        router = PreloadRouter()
        router.run()
    except Exception:
        logger.error('run[error] error: %s' % traceback.format_exc())
    logger.info("preload end.")


def main():
    while True:
        th = threading.Thread(target=run)
        th.start()
        time.sleep(15)


def main3():
    # while True:
    Ts = []
    for i in range(10):
        th = threading.Thread(target=run)
        Ts.append(th)
    for thr in Ts:
        thr.start()
    for th in Ts:
        threading.Thread.join(th)
# def main():
#     logger.info("refresh begining...")
#     while True:
#         router = Refresh_router()
#         router.run()
#         time.sleep(5)
#     logger.info("refresh end.")
#     os._exit(0) # fix :there are threads, not exit properly

if __name__ == "__main__":
    main()
