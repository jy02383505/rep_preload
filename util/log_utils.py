#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging.config
import os


log_dir = os.path.join("/Application/rep_preload", "logs")
if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logging.config.fileConfig("/Application/rep_preload/conf/logging.conf")
logger = logging.getLogger("root")
logger_receiver = logging.getLogger("receiver")
logger_preload_report = logging.getLogger("preload_report")


def getLogger():
    return logger


def get_receiver_Logger():
    return logger_receiver


def get_logger_preload_report():
    return logger_preload_report
