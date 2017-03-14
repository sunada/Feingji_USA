#!/usr/bin/env python
#coding=utf-8

__author__ = 'Administrator'

import os
from datetime import datetime
from datetime import timedelta
import csv
import pandas as pd

def get_files(path, fredix):
    files = []
    for file in os.listdir(path):
        if file.startswith(fredix):
            files.append(file)
    return files

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
        for line in lines[1:]:
            this_day = line.strip().split(",")
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

def cal_divident_cnt(file,date_patern):
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
            pd = datetime.strptime(seps[2], date_patern)
            pd_map[pd] = pd_map[pd] if pd in pd_map else 0
            for key in pd_map:
                if within_365_days(pd, key):
                # if within_one_year(pd, key):
                    val1 = pd_map[key] + 1
                    pd_map[key] = val1
        write_map_to_file(file, pd_map,"./data/dividend/" + ticker + "_new_365.csv")
        f.close()
        os.remove(file + ".tmp")

#this_date是否处在target_date过往1年中
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

def within_365_days(this_date, target_date):
    return this_date <= target_date and this_date > target_date + timedelta(days=-365)

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

def spit_devidends(filename):
    dirs = "./data/dividend"
    with open(filename) as f:
        lines = f.readlines()
        ticker = lines[1].strip().split(",")[0]
        dividend_ticker = open(dirs + "/" + ticker + ".csv", 'w')
        for line in lines[1:]:
            seps = line.strip().split(",")
            if ticker == seps[0]:
                dividend_ticker.write(line)
            else:
                dividend_ticker.close()
                dividend_ticker = open(dirs + "/" + seps[0] + ".csv", 'w')
                dividend_ticker.write(line)
                ticker = line.split(",")[0]

def cal_z(file):
    df = pd.read_csv(file)
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines[1:7]:
            seps = line.strip().split(",")
            today = datetime.strptime(seps[1], "%Y-%m-%d")
            one_year_before = today + timedelta(days = -365)
            one_year_before_str = one_year_before.strftime("%Y-%m-%d")
            print one_year_before_str
            tmp = 'Date <= ' + seps[1]
            print tmp
            print df.query(tmp)
            print df.query('Date >= ' + one_year_before_str).query('Date <= ' + seps[1])
            ava = df.query('Date >= ' + one_year_before_str)['Discount'].mean()
            std = df.query('Date >= ' + one_year_before_str)['Discount'].std()
            # print ava, std


def fill_dividend_data(file):
    with open(file, 'a+') as f:
        lines = f.readlines()
        print len(lines)
        print lines[-1]
        seps_start = lines[0].split(",")
        start = datetime.strptime(seps_start[1], "%Y-%m-%d")
        cnt = seps_start[-1]
        for line in lines[1:]:
            print line
            seps = line.split(",")
            end = datetime.strptime(seps[1], "%Y-%m-%d")
            i = start + timedelta(days = 1)
            while i < end:
                tmp = seps[0] + "," + i.strftime("%Y-%m-%d") + "," + cnt
                f.write(tmp)
                i += timedelta(days = 1)
            start = end
            cnt = seps[-1]
        f.close()

if __name__ == "__main__":
    # files = get_files(".", "result")
    # for f in files:
    #     print cal_discount(f, "./data/discount")

    # spit_devidends("dividends.csv")

    # for file in os.listdir("./data/dividend"):
    #     if not "_" in file:
    #         cal_divident_cnt("./data/dividend/" + file, "%Y-%m-%d")
    # cal_divident_cnt("./data/dividend/AFB.csv", "%Y-%m-%d")

    # for file in os.listdir("./data/dividend"):
    #     if "_" in file:
    #         check_divident_cnt("./data/dividend/" + file)

    # filename = "BGH.csv"
    # cal_divident_cnt("./data/dividend/" + filename, "%Y-%m-%d")
    # check_divident_cnt("./data/dividend/"  + filename.split(".")[0] + "_new.csv")

    # fill_dividend_data("./data/dividend/ABE_new_365.csv")
    cal_z("./data/discount/AFB.csv")

