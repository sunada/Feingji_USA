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
from account import Account

# 设置pandas数据显示宽度
pd.set_option("display.width", 300)


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

    logging.basicConfig(level=log_level, filename="%s/log.%s" % (log_dir, datetime.now().strftime("%Y%m%d")), filemode='a', format='%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] [%(lineno)d] %(message)s')
    #logging.basicConfig(level=log_level, format='%(asctime)s [%(levelname)s] [%(filename)s] [%(funcName)s] [%(lineno)d] %(message)s')

    return config_json
    

def load_trade_data(data_dir):
    """ 加载一个目录下的所有价格详情数据，返回DataFrame """

    result = pd.DataFrame()

    for file_name in os.listdir(data_dir):
        tmp = pd.read_csv(data_dir + "/" + file_name, parse_dates=["Date"])
        result = pd.concat([result, tmp], ignore_index=True)

    return result


def load_dividends(dividen_file):
    """ 加载红利数据，返回DataFrame """

    return pd.read_csv(dividen_file, parse_dates=["Declared Date", "Payable Date", "Ex Date"])


def load_sponsor(sponsor_file):
    """ 加载基金公司数据，返回DataFrame """

    return pd.read_csv(sponsor_file)


def filter_by_sponsor(trade_data, sponsor, select_sponsors):
    """ 按照基金公司过滤数据 """

    tickers = sponsor.where(sponsor["sponsor"].isin(select_sponsors)).dropna()["ticker"].values

    result = trade_data.where(trade_data["Ticker"].isin(tickers)).dropna()

    return result


def mark_special_dividend(dividend):
    """ 将分红数据按日期排序，并标注是否为特殊分红，通过前后对比。需要输入按照Ticker、Ex Date排序 """

    result = result.ix[:, ["Ticker", "Ex Date", "Distrib Amount"]]

    result["last_amount"] = result.shift(1)["Distrib Amount"]
    result["amount_raise"] = (result["Distrib Amount"] - result["last_amount"]) / result["last_amount"]

    result["is_special"] = 0

    result.loc[result["amount_raise"] > 0.5, ["is_special"]] = 1
    result.loc[result["amount_raise"] < -0.5, ["is_special"]] = 1

    result.loc[result["Ticker"] != result["Ticker"].shift(1), ["is_special"]] = 0

    return result


def mark_special_dividend_by_mean(dividend):
    """ 将分红数据按日期排序，并标注是否为特殊分红，通过平均值对比。需要输入按照Ticker、Ex Date排序 """

    tmp = dividend.ix[:, ["Ticker", "Ex Date", "Distrib Amount"]]

    # 计算分红平均值，合并入原来的数据
    mean_result = tmp.groupby(tmp["Ticker"]).mean()
    mean_result.reset_index(level=0, inplace=True)
    mean_result.rename(columns={"Distrib Amount": "mean_amount"}, inplace=True)

    dividend = pd.merge(dividend, mean_result, how="left", left_on="Ticker", right_on="Ticker")

    # 判断是否为特殊分红
    dividend["amount_raise"] = (dividend["Distrib Amount"] - dividend["mean_amount"]) / dividend["mean_amount"]

    dividend["is_special"] = 0

    dividend.loc[dividend["amount_raise"] > 0.8, ["is_special"]] = 1
    dividend.loc[dividend["amount_raise"] < -0.8, ["is_special"]] = 1

    return dividend


def mark_dividend_down(dividend):
    """ 标注每次分红是否比前一次分红低，除去特殊分红。需要输入按照Ticker、Ex Date排序，且已标注特殊分红"""

    # 过滤特殊分红

    tmp = dividend.where(dividend["is_special"] == 0).dropna()
    tmp = tmp.ix[:, ["Ticker", "Ex Date", "Distrib Amount"]]

    # 标注是否比前一次分红低
    tmp["last_amount"] = tmp.shift(1)["Distrib Amount"]

    tmp["is_low"] = 0
    tmp.loc[tmp["Distrib Amount"] < tmp["last_amount"], ["is_low"]] = 1

    # 对每个Ticker的第一条数据进行修正
    tmp.loc[tmp["Ticker"] != tmp["Ticker"].shift(1), ["last_amount", "is_low"]] = 0

    dividend = pd.merge(dividend, tmp, how="left", left_on=["Ticker", "Ex Date", "Distrib Amount"], right_on=["Ticker", "Ex Date", "Distrib Amount"])

    # 对特殊分红数据进行修正
    dividend.loc[dividend["is_special"] == 1, ["last_amount", "is_low"]] = 0

    return dividend


def mark_period_dividend_rise(dividend, dividend_rise_days):
    """ 标注一段时间之内，每次分红是否都比上一次分红高"""

    # 分组使用移动窗口统计是否分红不降
    grouped = dividend.groupby(dividend["Ticker"])

    is_low_sum_result = pd.DataFrame()

    for ticker, data in grouped:

        is_low_sum = data.ix[:, ["Ticker", "Ex Date", "is_low"]].rolling(on="Ex Date", window=str(dividend_rise_days) + "d").sum()

        is_low_sum["is_period_rise"] = 0

        is_low_sum.loc[is_low_sum["is_low"] == 0, ["is_period_rise"]] = 1

        is_low_sum_result = pd.concat([is_low_sum_result, is_low_sum])

    dividend = pd.merge(dividend, is_low_sum_result.ix[:, ["is_period_rise"]], how="left", left_index=True, right_index=True)

    return dividend


def calcu_period_dividend_earning(dividend, earning_days):
    """ 计算一段时间之内的分红总收益 """

    # 分组使用移动窗口统计分红总收益
    grouped = dividend.groupby(dividend["Ticker"])

    dividend_earning = pd.DataFrame()

    for ticker, data in grouped:

        tmp = data.ix[:, ["Ticker", "Ex Date", "Distrib Amount"]].rolling(on="Ex Date", window=str(earning_days) + "d").sum()

        dividend_earning = pd.concat([dividend_earning, tmp])

    dividend_earning.rename(columns={"Distrib Amount": "amount_sum"}, inplace=True)

    dividend = pd.merge(dividend, dividend_earning.ix[:, ["amount_sum"]], how="left", left_index=True, right_index=True)

    return dividend


def calcu_dividend_freq(dividend):
    """ 计算非特殊分红频率，一年中分红次数。需要输入已标注特殊分红 """

    # 过滤特殊分红，将日期转为年份
    tmp = dividend.where(dividend["is_special"] == 0).dropna()
    tmp["year"] = tmp["Ex Date"].apply(lambda x: x.date().year)

    # 过滤2010年、2017年的不完整数据
    tmp = tmp.where(tmp["year"] != 2010).dropna()
    tmp = tmp.where(tmp["year"] != 2017).dropna()

    # 统计每个基金分红了几年
    tmp2 = tmp.ix[:, ["Ticker", "year"]].drop_duplicates()
    ticker_year = tmp2.groupby(tmp2["Ticker"]).count()
    ticker_year.rename(columns={"year": "year_num"}, inplace=True)
    ticker_year.reset_index(level=0, inplace=True)

    # 统计分红次数
    ticker_dividend_num = tmp.ix[:, ["Ticker"]].groupby(tmp["Ticker"]).count()
    ticker_dividend_num.rename(columns={"Ticker": "dividend_num"}, inplace=True)
    ticker_dividend_num.reset_index(level=0, inplace=True)

    # 合并，并计算年均分红次数
    merge_result = pd.merge(ticker_year, ticker_dividend_num)
    dividend = pd.merge(dividend, merge_result, how="left")
    dividend["mean_dividend_num"] = dividend["dividend_num"] / dividend["year_num"]

    return dividend


def calcu_dividend_NTM(dividend):

    dividend["NTM"] = dividend["Distrib Amount"] * dividend["mean_dividend_num"]

    return dividend


def calcu_earnings(trade_data, earning_days):
    """ 计算一段时间之内的差价回报，结束价格 - 开始价格 """

    # 分组处理每一个封基
    grouped = trade_data.groupby(trade_data["Ticker"])

    earnings = pd.DataFrame()

    for ticker, data in grouped:

        # 由于本身数据有日期“漏洞”，需要经过重采样，填补漏洞
        # 重采样需要使用日期作为index
        data.set_index("Date", inplace=True)

        tmp = data.resample("D").pad()

        # 计算差价回报
        tmp["pre_price"] = tmp.shift(earning_days)["Share Price"]
        tmp["price_earning"] = tmp["Share Price"] - tmp["pre_price"]

        earnings = pd.concat([earnings, tmp])

    earnings.fillna(0, inplace=True)
    earnings.reset_index(level=0, inplace=True)

    trade_data = pd.merge(trade_data, earnings.ix[:, ["Ticker", "Date", "pre_price", "price_earning"]], how="left", left_on=["Ticker", "Date"], right_on=["Ticker", "Date"])

    return trade_data


def calcu_premium_rate(trade_data, z_score_days):
    """ 计算溢价率，(当日价格 - 前一日净值) / 前一日净值 """

    # 将前一天的净值，合并到这一天的数据中
    trade_data["last_value"] = trade_data.shift(1)["Net Asset Value"]
    
    # 一个Ticker的第一天，无法取到前一天的净值，过滤掉
    result = trade_data.where(trade_data["Ticker"] == trade_data["Ticker"].shift(1)).dropna()

    # 计算溢价率
    result["premium_rate"] = result["Share Price"] / result["last_value"] - 1

    # 分组使用移动窗口计算溢价率均值、溢价率标准差
    grouped = result.groupby(result["Ticker"])

    premium_result = pd.DataFrame()

    for ticker, data in grouped:

        premium_rate_mean = data.ix[:, ["Ticker", "Date", "premium_rate"]].rolling(on="Date", window= str(z_score_days) + "d").mean()
        premium_rate_mean.rename(columns={"premium_rate": "premium_rate_mean"}, inplace=True)

        premium_rate_std = data.ix[:, ["Ticker", "Date", "premium_rate"]].rolling(on="Date", window= str(z_score_days) + "d").std()
        premium_rate_std.rename(columns={"premium_rate": "premium_rate_std"}, inplace=True)

        premium_result = pd.concat([premium_result, pd.merge(premium_rate_mean, premium_rate_std)])

    result = pd.merge(result, premium_result, how="left", left_on=["Ticker", "Date"], right_on=["Ticker", "Date"])

    # 计算溢价率z-score
    result["premium_rate_z_score"] = (result["premium_rate"] - result["premium_rate_mean"]) / result["premium_rate_std"]

    return result


def process_dividend(dividend, config_json):
    """ 处理分红数据 """

    # 提取需要的字段
    dividend = dividend.ix[:, ["Ticker", "Ex Date", "Payable Date", "Distrib Amount"]]

    # 按照基金、日期排序
    dividend = dividend.sort_values(by=["Ticker", "Ex Date"])

    # 标注是否为特殊分红
    dividend = mark_special_dividend_by_mean(dividend)

    # 标注每次分红是否比前一次低
    dividend = mark_dividend_down(dividend)

    # 标注一段时间之内，每次分红是否保持不降
    dividend = mark_period_dividend_rise(dividend, config_json["dividend_rise_days"])

    # 计算一段时间之内的分红总收益
    dividend = calcu_period_dividend_earning(dividend, config_json["earning_days"])

    # 计算分红频率
    dividend = calcu_dividend_freq(dividend)

    # 计算每天的NTM
    dividend = calcu_dividend_NTM(dividend)

    return dividend


def process_trade_data(trade_data, sponsor, config_json):
    """ 处理交易详情数据 """

    # 按照基金名、日期排序
    trade_data = trade_data.sort_values(by=["Ticker", "Date"])

    # 按照基金公司过滤
    trade_data = filter_by_sponsor(trade_data, sponsor, config_json["select_sponsors"])

    # 计算溢价率、溢价率z-score
    trade_data = calcu_premium_rate(trade_data, config_json["z_score_days"])

    # 计算一段时间之内的差价回报
    trade_data = calcu_earnings(trade_data, config_json["earning_days"])

    return trade_data


def get_test_dates(trade_data, dividend, config_json):

    logging.debug("dividend_rise_days: %d" % (config_json["dividend_rise_days"]))
    logging.debug("earning_days: %d" % (config_json["earning_days"]))
    logging.debug("z_score_days: %d" % (config_json["z_score_days"]))
    logging.debug("backtest_start_date: %s" % (config_json["backtest_start_date"]))
    logging.debug("backtest_end_date:   %s" % (config_json["backtest_end_date"]))

    # 取出交易数据和红利数据中的交集部分
    max_date1 = dividend["Ex Date"].max()
    max_date2 = trade_data["Date"].max()

    if max_date1 > max_date2:
        max_date = max_date2
    else:
        max_date = max_date1

    min_date1 = dividend["Ex Date"].min()
    min_date2 = trade_data["Date"].min()

    if min_date1 > min_date2:
        min_date = min_date1
    else:
        min_date = min_date2

    logging.info("date range in data:  %s   --  %s" % (min_date, max_date))

    # 计算指标时需要的数据天数
    tmp = []
    tmp.append(config_json["dividend_rise_days"])
    tmp.append(config_json["earning_days"])
    tmp.append(config_json["z_score_days"])

    max_days = max(tmp)
    logging.info("days to calculate all features:  %d" % (max_days))

    # 检查回测开始、结束时间是否合理
    # 由于计算指标需要一定天数的数据，所以只有部分数据的指标是完整的。回测起止时间必须使用完整指标的数据。
    # 如果没有配置回测起始时间，自动通过数据中的日期和计算指标所用天数推算
    if config_json.get("backtest_start_date", "") == "":
        backtest_start_date = min_date + timedelta(days = max_days)
        logging.warn("backtest_start_date not found. Using default: %s" % (backtest_start_date))
    else:
        backtest_start_date = datetime.strptime(config_json["backtest_start_date"], "%Y-%m-%d")

    if config_json.get("backtest_end_date", "") == "":
        backtest_end_date = max_date
        logging.warn("backtest_end_date not found. Using default: %s" % (backtest_end_date))
    else:
        backtest_start_date = datetime.strptime(config_json["backtest_start_date"], "%Y-%m-%d")

    if backtest_start_date + timedelta(days=max_days) > backtest_end_date:
        logging.fatal("Not enouth data to run backtest!")
        return None, None

    return backtest_start_date, backtest_end_date



def backtest(trade_data, dividend, config_json):
    """ 实现回测逻辑的函数 """

    # 检查回测日期是否满足条件
    start_date, end_date = get_test_dates(trade_data, dividend, config_json)

    if start_date == None:
        return None

    print trade_data, dividend

    # 去除特殊分红
    dividend = dividend.where(dividend["is_special"] == 0).dropna()

    # 合并交易数据和分红数据，并填充
    trade_data = pd.merge(trade_data, dividend, how="left", left_on=["Ticker", "Date"], right_on=["Ticker", "Ex Date"])

    merged_data = pd.DataFrame()
    grouped = trade_data.groupby(trade_data["Ticker"])

    for ticker, data in grouped:
        data.fillna(method="pad", inplace=True)
        merged_data = pd.concat([merged_data, data])

    # 回测开始
    now_date = start_date
    account = Account(cash=config_json["start_cash"], min_trade_share=0, trade_unit=1)

    # Ticker: {Pay Date: hold share}
    dividend_ticker_map = {}

    while now_date < end_date:

        # 判断当前持仓是否有能够获得红利的封基，将能够获得红利的封基记录在另外一张表中。在Payable Date到来时在获取红利
        holding_tickers = account.get_stock_list()

        ex_tickers = dividend.where(dividend["Ex Date"] == now_date).dropna()
        ex_tickers = extickers.where(dividend["Ticker"] in holding_tickers).dropna().ix[:, ["Ticker", "Payable Date", "Distrib Amount"]].value

        print ex_tickers

        # 判断是否是可以获得红利的封基的Payable Date，获取红利

        # 根据策略选择封基

        # 进行此次调仓

        now_date += timedelta(days=1)
    

    return



def main():

    config_json = init("./conf/config.json")

    # 加载数据
    trade_data = load_trade_data(config_json["data_dir"])
    dividend = load_dividends("./conf/dividends.csv")
    sponsor = load_sponsor("./conf/ticker_sponsor.csv")

    # 处理分红数据
    dividend = process_dividend(dividend, config_json)
    logging.debug("finish process_dividend.")

    # 处理交易详情数据
    trade_data = process_trade_data(trade_data, sponsor, config_json)
    logging.debug("finish process_trade_data.")

    # 回测
    logging.info("backtest start at: %s" % (datetime.now()))
    backtest(trade_data, dividend, config_json)
    logging.info("backtest end at: %s" % (datetime.now()))

    return


if __name__ == "__main__":
    main()


