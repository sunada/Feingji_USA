#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################################
#
# File Name:  main.py
#
# Function:   
#
# Usage:  
#
# Input:  
#
# Output:	
#
# Author: panwenhai
#
# Create Time:    2017-04-25 16:17:02
#
######################################################

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import time
import json
import logging
import shutil
import pandas as pd
from datetime import datetime, timedelta


def init(conf_file):
    """ 初始化函数，根据配置文件，初始化各个目录，返回配置结构 """

    with open(conf_file, "r") as f:
        config_json = json.loads(f.read())

    log_dir = config_json["log_dir"]
    result_dir = config_json["result_dir"]

    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    if os.path.exists(result_dir):
        shutil.rmtree(result_dir)
    os.mkdir(result_dir)

    if config_json.get("debug_mode", False):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level, filename="%s/log.%s" % (log_dir, datetime.now().strftime("%Y%m%d")), filemode='a', \
                                    format='%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] [%(lineno)d] %(message)s')

    return config_json
    

def load_trade_data(data_dir):
    """ 加载一个目录下的所有详情数据，返回DataFrame """

    result = pd.DataFrame()

    for file_name in os.listdir(data_dir):
        tmp = pd.read_csv(data_dir + "/" + file_name)
        result = pd.concat([result, tmp], ignore_index=True)

    return result


def load_dividends(dividen_file):
    """ 加载dividend数据，返回DataFrame """

    return pd.read_csv(dividen_file)


def main():

    config_json = init("./conf/config.json")

    trade_data = load_trade_data(config_json["data_dir"])

    print trade_data

    dividend = load_dividends("./conf/dividends.csv")

    print dividend

    return


if __name__ == "__main__":
    main()

