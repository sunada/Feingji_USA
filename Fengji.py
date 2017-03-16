#!/usr/bin/env python
#coding=utf-8

__author__ = 'Administrator'

import os
from datetime import datetime
from datetime import timedelta
import csv
import pandas as pd

# 仅处理文件中指定的封基
def get_chosed_fund(file = "ticker_sponsor.csv"):
    df = pd.read_csv(file)
    return df['ticker'].tolist()

#遍历路径下，得到以prefix为前缀的文件list
def get_files(path, predix):
    files = []
    for file in os.listdir(path):
        if file.startswith(predix):
            files.append(file)
    return files

#将分红文件割裂到以ticker为名的文件中
def spit_devidends(filename):
    dir = "./data/dividend"
    if not os.path.exists(dir):
        os.makedirs(dir)
    chosed_funds = get_chosed_fund()
    with open(filename) as f:
        lines = f.readlines()
        ticker = lines[1].strip().split(",")[0]
        dividend_ticker = False
        if ticker in chosed_funds:
            dividend_ticker = open(dir + "/" + ticker + ".csv", 'w')
        for line in lines[1:]:
            seps = line.strip().split(",")
            if not seps[0] in chosed_funds:
                continue
            if ticker == seps[0]:
                if not dividend_ticker:
                    dividend_ticker = open(dir + "/" + ticker + ".csv", 'w')
                dividend_ticker.write(line)
            else:
                if dividend_ticker:
                    dividend_ticker.close()
                dividend_ticker = open(dir + "/" + seps[0] + ".csv", 'w')
                dividend_ticker.write(line)
                ticker = line.split(",")[0]

#计算折价且写入新文件
def cal_discount(file, dir):
    if not os.path.exists(file):
        return False
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(file, 'r') as f:
        lines = f.readlines()
        last_day = [0]
        new_file = False
        cnt = 0
        chosed_funds = get_chosed_fund()
        for line in lines[1:]:
            this_day = line.strip().split(",")
            ticker = this_day[0]
            if not ticker in chosed_funds:
                continue
            if this_day[0] == last_day[0]:
                tmp = 1 - float(this_day[2]) / float(last_day[3])
                this_day[4] = str(round(tmp, 4))
                last_day = this_day
                content = ",".join(this_day) + "\n"
                new_file.write(content)
            else:
                if new_file:
                    new_file.close()
                cnt += 1
                last_day = this_day
                last_day[4] = "None"
                filename = last_day[0]+".csv"
                new_file = open(os.path.join(dir, filename), 'w')
                new_file.write("Ticker,Date,Price,NAT,Discount\n")
                content = ",".join(last_day) + "\n"
                new_file.write(content)
        return cnt

#计算分红频率,并将结果写入新文件
def cal_dividend_cnt(file,date_patern):
    if not os.path.exists(file):
        return False

    data = csv.reader(open(file, 'r'), delimiter=",")
    print file
    data = sorted(data, key = lambda x:datetime.strptime(x[2], date_patern), reverse=True)
    with open(file + ".tmp", 'w') as f:
        for d in data:
            tmp = ",".join(d)
            f.write(tmp + "\n")
        f.close()

    with open(file + ".tmp", 'r') as f:
        lines = f.readlines()
        pd_map = {}
        print file
        for line in lines:
            seps = line.split(",")
            ticker = seps[0]
            pay_date = datetime.strptime(seps[2], date_patern)
            pd_map[pay_date] = pd_map[pay_date] if pay_date in pd_map else 0
            for key in pd_map:
                if within_365_days(pay_date, key):
                # if within_one_year(pd, key):
                    val1 = pd_map[key] + 1
                    pd_map[key] = val1
        write_map_to_file(file, pd_map,"./data/dividend/" + ticker + "_new_365.csv")
        f.close()
        os.remove(file + ".tmp")

#计算Z值且写入新文件
def cal_z(file, new_file):
    dateparse = pd.tseries.tools.to_datetime
    df = pd.read_csv(file,index_col="Date",parse_dates=True,date_parser=dateparse)
    with open(file, 'r') as f, open(new_file, 'w') as wf:
        wf.write("Ticker,Date,Price,NAT,Discount,ava,std,z\n")
        lines = f.readlines()
        seps = lines[1].strip().split(",")
        start_date = datetime.strptime(seps[1], "%Y-%m-%d")
        for line in lines[1:365]:
            line = line.strip()
            seps = line.split(",")
            today = datetime.strptime(seps[1], "%Y-%m-%d")
            one_year_before = today + timedelta(days = -365)
            if one_year_before < start_date:
                continue
            ava = df.loc[one_year_before : today]['Discount'].mean()
            std = df.loc[one_year_before : today]['Discount'].std()
            discount = float(seps[4])
            z = (discount - ava) / std
            line += "," + str(round(ava,4)) + "," + str(round(std,4)) + "," + str(round(z, 4)) + "\n"
            wf.write(line)

#检查三年中，封基是否每次的分红都不小于上一次（特殊分红除外）
def check_dividend_3years(file, new_file):
    data = csv.reader(open(file, 'r'), delimiter=",")
    data = sorted(data, key = lambda x:datetime.strptime(x[2], "%Y-%m-%d"))
    with open(new_file, 'w') as wf:
        for i in range(len(data)):
            item = data[i]
            amount = float(item[4])
            pay_date = datetime.strptime(item[2], "%Y-%m-%d")
            three_years_before = pay_date + timedelta(days = -365 * 3)
            tmp = pay_date
            res = True
            j = i - 1
            print item
            while tmp >= three_years_before and j >= 0:
                print "tmp:", tmp, "three_years_before:", three_years_before
                print amount, float(data[j][4]), amount * 1.2
                if amount < float(data[j][4]) and amount * 1.2 > float(data[j][4]) :
                    res = False
                    print "res:", res
                    break
                j -= 1
                tmp =  datetime.strptime(data[j][2], "%Y-%m-%d")
            data[i].append(str(res))

        for item in data:
            tmp = ','.join(item)
            tmp += "\n"
            wf.write(tmp)
        wf.close()

#已废弃 this_date是否处在target_date过往1年中
def within_one_year(this_date, target_date):
    li = []
    this_date = this_date.strftime("%Y-%m-%d")
    target_date = target_date.strftime("%Y-%m-%d")
    seps = target_date.split("-")
    year = int(seps[0])
    month = int(seps[1])
    for i in range(0, 12):
        if month - i > 0:
            join_str = ["-", "-0"][month - i < 10]
            li.append(str(year) + join_str + str(month - i))
        else:
            join_str = ["-", "-0"][month + 12 - i < 10]
            li.append(str(year - 1) + join_str + str(month + 12 - i))
    seps = this_date.split("-")
    tmp = seps[0] + "-" + seps[1]
    return tmp in li

# target_date - 365天 < this_date <= target_date
def within_365_days(this_date, target_date):
    return this_date <= target_date and this_date > target_date + timedelta(days=-365)

#将map中的数据加入到file对应的行后。map记录着某天某个ticker的一年内分红的次数
def write_map_to_file(file, map, new_file):
    if len(map) == 0:
        return
    with open(new_file, 'w') as wf, open(file, 'r') as rf:
        wf.write("Ticker,Declared Date,Payable Date,Ex Date,Distrib Amount,Income,Long Gain,Short Gain,ROC,dividend_cnt\n")
        for line in rf:
            seps = line.strip().split(",")
            key = datetime.strptime(seps[2], "%Y-%m-%d")
            tmp = ",".join(seps) + "," + str(map[key]) + "\n"
            wf.write(tmp)
    wf.close()
    rf.close()
    return

# 检查年基金每年分红的次数
def check_divident_cnt(filename):
    map = {}
    with open(filename) as f:
        lines = f.readlines()
        for line in lines[1:]:
            sep = line.strip().split(",")
            cnt = int(sep[-1])
            if not cnt in map:
                map[cnt] = 0
            map[cnt] += 1

    for key in map:
        if key < 12 and map[key] == 1:
            continue
        if key == 12:
            continue
        print filename, key, map[key]



if __name__ == "__main__":
    #计算交易历史的折价率
    # files = get_files(".", "result")
    # for f in files:
    #     print cal_discount(f, "./data/discount")

    # 切割分红数据
    # spit_devidends("dividends.csv")

    #计算一年内分红的次数
    for file in os.listdir("./data/dividend"):
        if not "_" in file:
            cal_dividend_cnt("./data/dividend/" + file, "%Y-%m-%d")
    # cal_divident_cnt("./data/dividend/AFB.csv", "%Y-%m-%d")

    # for file in os.listdir("./data/dividend"):
    #     if "_" in file:
    #         check_divident_cnt("./data/dividend/" + file)

    # filename = "BGH.csv"
    # cal_divident_cnt("./data/dividend/" + filename, "%Y-%m-%d")
    # check_divident_cnt("./data/dividend/"  + filename.split(".")[0] + "_new.csv")

    # fill_dividend_data("./data/dividend/ABE_new_365.csv")
    # cal_z("./data/discount/AFB.csv", "./data/discount/AFB_z.csv")
    # check_dividend_3years("./data/dividend/ACP_new_365.csv", "./data/dividend/ACP_new_365_3years.csv")

