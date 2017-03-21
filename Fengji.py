#!/usr/bin/env python
#coding=utf-8

__author__ = 'Administrator'

import os
from datetime import datetime
from datetime import timedelta
import csv
import pandas as pd
import MySQLdb

# 仅处理文件中指定的封基
def get_chosed_fund(file = "ticker_sponsor.csv"):
    df = pd.read_csv(file)
    return df['ticker'].tolist()

#遍历路径下，得到以prefix为前缀的文件list
def get_files_prefix(path, prefix):
    files = []
    for file in os.listdir(path):
        if file.startswith(prefix):
            files.append(file)
    return files

def get_files_postfix(path, postfix):
    files = []
    for file in os.listdir(path):
        if file.endswith(postfix):
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

#将历史交易数据导入数据库
def discount_database(path="./data/z/"):
    db = MySQLdb.connect("localhost","root","lala","fengji_usa")
    for file in os.listdir(path):
    # file = "./data/discount/BXMX.csv"
    # if True:
        with open(path+file, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                seps = line.split(",")
                sql = """INSERT INTO fund_history(ticker,deal_date, price, NAT, discount,ava,fund_std,z) VALUES ("%s","%s",%s,%s,%s,%s,%s,%s)""" %(seps[0],seps[1],seps[2],seps[3],seps[4],seps[5],seps[6],seps[7])
                # print sql
                try:
                    print line
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                except Exception, e:
                    print e
                    db.rollback()
    db.close()

#将分红历史数据导入数据库
def dividend_database(files,path="./data/dividend/"):
    db = MySQLdb.connect("localhost","root","lala","fengji_usa")
    for file in files:
        # file = "./data/discount/BXMX.csv"
        # if True:
        with open(path+file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                seps = line.split(",")
                sql = """INSERT INTO dividend(ticker,declare_date, payable_date, ex_date, amount,cnt,bigger_three_year) VALUES ("%s","%s","%s","%s",%s,%s,%s)""" %(seps[0],seps[1],seps[2],seps[3],seps[4],seps[9],seps[10])
                # print sql
                try:
                    cursor = db.cursor()
                    cursor.execute(sql)
                    db.commit()
                except Exception, e:
                    print e
                    db.rollback()
    db.close()

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
                last_day[4] = "0"
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
            amount = float(seps[4])
            pay_date = datetime.strptime(seps[2], date_patern)
            # pd_map[pay_date] = pd_map[pay_date] if pay_date in pd_map else 0
            #同一天分红再次（即有一次为特殊分红），则特殊分红不计入近一年的分红次数
            # if pay_date in pd_map:
            #     continue
            #相同的pay_date只计一次
            # pd_map[pay_date] = 0
            if pay_date in pd_map:
                (cnt, div_amount) = pd_map[pay_date]
                pd_map[pay_date] = (cnt, div_amount + amount)
                continue
            else:
                pd_map[pay_date] = (0, amount)
            for key in pd_map:
                if within_365_days(pay_date, key):
                # if within_one_year(pd, key):
                #     val1 = pd_map[key] + 1
                #     pd_map[key] = val1
                    (cnt, amount) = pd_map[key]
                    pd_map[key] = (cnt + 1, amount)
        # write_map_to_file(file, pd_map,"./data/dividend/" + ticker + "_new_365.csv")
        write_map_to_file2(ticker, pd_map, "./data/dividend/" + ticker + "_new_365.csv")
        f.close()
        os.remove(file + ".tmp")

#将map中的数据加入到file对应的行后。map记录着某天某个ticker的一年内分红的次数
def write_map_to_file(file, map, new_file):
    if len(map) == 0:
        return
        with open(new_file, 'w') as wf, open(file, 'r') as rf:
            # 3年内分红不小于上次的处理函数暂无法处理此行数据
            # wf.write("Ticker,Declared Date,Payable Date,Ex Date,Distrib Amount,Income,Long Gain,Short Gain,ROC,dividend_cnt\n")
            for line in rf:
                seps = line.strip().split(",")
                pay_date = datetime.strptime(seps[2], "%Y-%m-%d")
                tmp = ",".join(seps) + "," + str(map[pay_date]) + "\n"
                wf.write(tmp)
        return

def write_map_to_file2(ticker,map, new_file):
    with open(new_file, 'w') as wf:
        for pay_date in map.keys():
            amount = map[pay_date][1]
            cnt = map[pay_date][0]
            tmp = ticker + "," + pay_date.strftime("%Y-%m-%d") + "," + str(amount) + "," + str(cnt) + "\n"
            wf.write(tmp)
    return

# target_date - 365天 < this_date <= target_date
def within_365_days(this_date, target_date):
    return this_date <= target_date and this_date > target_date + timedelta(days=-364)


#计算Z值且写入新文件
def cal_z(file, new_file):
    dateparse = pd.tseries.tools.to_datetime
    df = pd.read_csv(file,index_col="Date",parse_dates=True,date_parser=dateparse)
    with open(file, 'r') as f, open(new_file, 'w') as wf:
        wf.write("Ticker,Date,Price,NAT,Discount,ava,std,z\n")
        lines = f.readlines()
        seps = lines[1].strip().split(",")
        start_date = datetime.strptime(seps[1], "%Y-%m-%d")
        for line in lines[1:]:
            line = line.strip()
            seps = line.split(",")
            today = datetime.strptime(seps[1], "%Y-%m-%d")
            one_year_before = today + timedelta(days = -365)
            if one_year_before < start_date:
                print one_year_before, start_date
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
            j = i
            while tmp >= three_years_before and j > 0:
                # print "tmp:", tmp, "three_years_before:", three_years_before
                # print "当期分红：",amount, "上期分红", float(data[j][4]), "特殊分红标准线",amount * 1.2
                if amount < float(data[j - 1][4]) and amount * 1.2 > float(data[j - 1][4]) :
                    res = False
                    print "res:", res
                    break
                j -= 1
                tmp =  datetime.strptime(data[j][2], "%Y-%m-%d")
                amount = float(data[j][4])
            data[i].append(str(res))

        for item in data:
            tmp = ','.join(item)
            tmp += "\n"
            wf.write(tmp)
        wf.close()

#将分红数据扩充到每天后写到文件中
def fill_dividend_data(files,path="./data/dividend/"):
    for file in files[4:5]:
        # print file
        data = csv.reader(open(path+file, 'r'), delimiter=",")
        data = sorted(data, key = lambda x:datetime.strptime(x[2], "%Y-%m-%d"))
        tmp = data[0]
        tmp_iter = datetime.strptime(tmp[2], "%Y-%m-%d")
        new_file = path + file.split(".")[0] + "_fill.csv"
        last_pay_date = None
        with open(new_file, 'w') as f:
            for d in data:
                print "d:", d
                pay_date = datetime.strptime(d[2], "%Y-%m-%d")
                print "pay_date:", pay_date, " last_pay_date:",last_pay_date," tmp_iter:",tmp_iter
                if pay_date == last_pay_date:
                    f.write(",".join(d) + "\n")
                    tmp_iter = pay_date + timedelta(days = 1)
                    continue
                while tmp_iter <= pay_date:
                    tmp[2] = tmp_iter.strftime("%Y-%m-%d")
                    f.write(",".join(tmp) + "\n")
                    tmp_iter += timedelta(days = 1)
                last_pay_date = pay_date
                tmp = d

#this_date是否处在target_date过往1年中。分红按月算，未精确到天
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

# 检查年基金每年分红的次数
def check_divident_cnt(filename):
    map = {}
    with open(filename) as f:
        lines = f.readlines()
        max = 0
        for line in lines[1:]:
            sep = line.strip().split(",")
            cnt = int(sep[-1])
            max = max if max >= cnt else cnt
            if not cnt in map:
                map[cnt] = 0
            map[cnt] += 1

    for key in map:
        if key < max and map[key] == 1:
            continue
        if key == max:
            continue
        print filename, "最大分红次数：",max, "一年内异常分红的次数：", key, " 有多少个这样的次数：", map[key]

# 交易历史数据与筛选出的基金对比结果 [NIQ, NID, NHA]没有历史数据
def check_two_source_tickers(path="./data/discount", file="ticker_sponsor.csv"):
    funds = os.listdir("./data/discount")
    df = pd.read_csv("ticker_sponsor.csv")
    tickers = df['ticker'].tolist()
    tickers_from_funds = []
    for f in funds:
        ticker = f.split(".")[0]
        tickers_from_funds.append(ticker)

    for t in tickers:
        if t not in tickers_from_funds:
            print t

if __name__ == "__main__":
    #计算交易历史的折价率
    # files = get_files(".", "result")
    # for f in files:
    #     print cal_discount(f, "./data/discount")

    #计算z值
    # files = os.listdir("./data/discount")
    # for file in files:
    #     new_file = "./data/z/"+file.split(".")[0] + "_z.csv"
    #     cal_z("./data/discount/"+file, new_file)
    # cal_z("./data/discount/JHB.csv", "./data/z/JHB_z.csv")

    #将含有discount和z值的文件装入数据
    # discount_database()

    #发现数据库里比目标tikcers相关7个，分别是：EHT,JHB,JHD,JPT,NIQ,NID,NHA。
    #其中NIQ,NID,NHA这三项无历史交易数据;EHT,JHB,JHD,JPT交易数据不足一年。z值计算不出来，所以都没写入数据库。
    # tickers = pd.read_csv("mysql_test.csv")["ticker"].tolist()
    # tickers_from_sponsor = pd.read_csv("ticker_sponsor.csv")["ticker"].tolist()
    # for t in tickers_from_sponsor:
    #     if t not in tickers:
    #         print t


    # 切割分红数据
    # spit_devidends("dividends.csv")

    #计算一年内分红的次数
    # for file in os.listdir("./data/dividend"):
    #     if not "_" in file:
    #         cal_dividend_cnt("./data/dividend/" + file, "%Y-%m-%d")
    # cal_dividend_cnt("./data/dividend/EFF.csv", "%Y-%m-%d")

    # files = get_files_postfix("./data/dividend/", "_3years.csv")
    # dividend_database(files)
    # fill_dividend_data(files)

    # 检查每年基金的分红次数，输出不正常的
    # for file in os.listdir("./data/dividend"):
    #     if "_" in file:
    #         check_divident_cnt("./data/dividend/" + file)

    # filename = "BGH.csv"
    # cal_divident_cnt("./data/dividend/" + filename, "%Y-%m-%d")
    # check_divident_cnt("./data/dividend/"  + filename.split(".")[0] + "_new.csv")

    ticker = "EFF"
    check_dividend_3years("./data/dividend/" + ticker + "_new_365.csv", "./data/dividend/" + ticker + "_new_365_3years.csv")

    # chosed_funds = get_chosed_fund()
    # for ticker in chosed_funds:
    #     check_dividend_3years("./data/dividend/" + ticker + "_new_365.csv", "./data/dividend/" + ticker + "_new_365_3years.csv")



