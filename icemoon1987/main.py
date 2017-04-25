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


def load_sponsor(sponsor_file):
    """ 加载sponsor数据，返回DataFrame """

    return pd.read_csv(sponsor_file)


def filter_by_sponsor(trade_data, sponsor, config_json):
    """ 按照sponsor过滤数据 """

    tickers = sponsor.where(sponsor["sponsor"].isin(config_json["select_sponsors"])).dropna()["ticker"].values

    result = trade_data.where(trade_data["Ticker"].isin(tickers)).dropna()

    return result


def generate_earnings(trade_data, config_json):
    """ 计算earnings """

    print trade_data
    print config_json["earning_days_gap"]

    return




def load_data(config_json):
    """ 加载数据，合并数据 """

    trade_data = load_trade_data(config_json["data_dir"])
    dividend = load_dividends("./conf/dividends.csv")
    sponsor = load_sponsor("./conf/ticker_sponsor.csv")

    #print trade_data
    #print dividend
    #print sponsor

    #print trade_data.where(trade_data["Ticker"] == "MMV").dropna()
    #print trade_data.query("Ticker == \"MFM\"")
    #print sponsor.query("ticker == \"MMV\"")

    #result = pd.merge(trade_data, sponsor, how="left", left_on="Ticker", right_on="ticker")

    #print result
    
    return


def main():

    config_json = init("./conf/config.json")

    #load_data(config_json)

    trade_data = load_trade_data(config_json["data_dir"])
    dividend = load_dividends("./conf/dividends.csv")
    sponsor = load_sponsor("./conf/ticker_sponsor.csv")

    filtered_trade_data = filter_by_sponsor(trade_data, sponsor, config_json)

    generate_earnings(trade_data, config_json)

    return


if __name__ == "__main__":
    main()


