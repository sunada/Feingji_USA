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

def dividend_original(file="dividends.csv"):
    if not os.path.exists(file):
        return False
    db = MySQLdb.connect("localhost","root","lala","fengji_usa")
    with open(file, 'r') as f:
        f.readline()
        for line in f:
            seps = line.strip().split(",")
            sql = """INSERT INTO dividend_original(ticker,declare_date, payable_date, ex_date, amount) VALUES ("%s","%s","%s","%s",%s)""" %(seps[0],seps[1],seps[2],seps[3],seps[4])
            print sql
            try:
                cur = db.cursor()
                cur.execute(sql)
                db.commit()
            except Exception, e:
                print e
                db.rollback()
        db.close
    return True

#将处理后的交易&分红数据写入数据库
def deal_dividend_database(files, path="./data/chosed_fund_data/"):
    db = MySQLdb.connect("localhost", "root", "lala", "fengji_usa")
    for file in files:
        with open(path + file, 'r') as f:
            f.readline()
            for line in f:
                seps = line.strip().split(",")
                print seps
                sql = """INSERT INTO fund_dividend(deal_date,ticker,price,nat,discount,ava,standard,z,amount,cnt,3years,3year_sum_amount,is_real_dividend_date,3years_profit) VALUES ("%s","%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""" %(seps[0],seps[1],seps[2],seps[3],seps[4],seps[5],seps[6],seps[7],seps[9],seps[10],seps[11],seps[12],seps[13],seps[14])
                print sql
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

#计算折价且写入新文件
def cal_new_discount(file, dir):
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
            if ticker != "BXMX":
                continue
            if not ticker in chosed_funds:
                continue
            if this_day[0] == last_day[0]:
                tmp = float(this_day[2]) / float(last_day[3]) - 1
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
            amount = float(seps[4])    #分红金额
            pay_date = datetime.strptime(seps[2], date_patern)
            # pd_map[pay_date] = pd_map[pay_date] if pay_date in pd_map else 0
            #同一天分红再次（即有一次为特殊分红），则特殊分红不计入近一年的分红次数
            # if pay_date in pd_map:
            #     continue
            #相同的pay_date只计一次
            # pd_map[pay_date] = 0
            #将相同的pay_date发放的分红累计起来，次数计为1
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
        new_file_name = ticker + "_new_365.csv"
        # write_map_to_file(file, pd_map,"./data/dividend/" + ticker + "_new_365.csv")
        write_map_to_file2(ticker, pd_map, "./data/dividend/" + ticker + "_new_365.csv")
        f.close()
        os.remove(file + ".tmp")
        return new_file_name

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
    sort_items = sorted(map.items(), key=lambda e:e[0], reverse=False)
    with open(new_file, 'w') as wf:
        wf.write("ticker,pay_date,dividend_amount,cnt\n")
        for key,value in sort_items:
            pay_date = key
            amount = value[1]
            cnt = value[0]
            tmp = ticker + "," + pay_date.strftime("%Y-%m-%d") + "," + str(amount) + "," + str(cnt) + "\n"
            wf.write(tmp)
    # with open(new_file, 'w') as wf:
    #     # wf.write("ticker,pay_date,dividend_amount,cnt\n")
    #     for pay_date in map:
    #         amount = map[pay_date][1]
    #         cnt = map[pay_date][0]
    #         tmp = ticker + "," + pay_date.strftime("%Y-%m-%d") + "," + str(amount) + "," + str(cnt) + "\n"
    #         wf.write(tmp)
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
    print file
    data = csv.reader(open(file, 'r'), delimiter=",")
    data.next()
    data = sorted(data, key = lambda x:datetime.strptime(x[1], "%Y-%m-%d"))
    with open(new_file, 'w') as wf:
        wf.write("ticker,pay_date,dividend_amount,cnt,3years\n")
        for i in range(len(data)):
            item = data[i]
            amount = float(item[2])
            pay_date = datetime.strptime(item[1], "%Y-%m-%d")
            three_years_before = pay_date + timedelta(days = -365 * 3)
            tmp = pay_date
            res = True
            j = i
            while tmp >= three_years_before and j > 0:
                # print "tmp:", tmp, "three_years_before:", three_years_before
                # print "当期分红：",amount, "上期分红", float(data[j][4]), "特殊分红标准线",amount * 1.2
                if amount < float(data[j - 1][2]) and amount * 1.2 > float(data[j - 1][2]) :
                    res = False
                    break
                j -= 1
                tmp =  datetime.strptime(data[j][1], "%Y-%m-%d")
                amount = float(data[j][2])
            data[i].append(str(res))

        for item in data:
            tmp = ','.join(item)
            tmp += "\n"
            wf.write(tmp)
        wf.close()

#计算三年中分红的累加值
def sum_3years_dividend(file, new_file):
    dateparse = pd.tseries.tools.to_datetime
    df = pd.read_csv(file,index_col="pay_date",parse_dates=True,date_parser=dateparse)

    data = csv.reader(open(file, 'r'), delimiter = ",")
    data.next()
    with open(new_file, 'w') as wf:
        wf.write("ticker,pay_date,dividend_amount,cnt,3year,3year_sum_dividend_amount\n")
        for d in data:
            pay_date = datetime.strptime(d[1], "%Y-%m-%d")
            pay_date_3years_before = pay_date + timedelta(days = -365 * 3)
            sum = df[pay_date_3years_before : pay_date]['dividend_amount'].sum()
            d.append(str(sum))
            wf.write(",".join(d) + "\n")

#将分红数据扩充到每天后写到文件中
def fill_dividend_data(files,path="./data/dividend/"):
    for file in files:
        print file
        data = csv.reader(open(path+file, 'r'), delimiter=",")
        data.next()
        data = sorted(data, key = lambda x:datetime.strptime(x[1], "%Y-%m-%d"))
        tmp = data[0]
        tmp_iter = datetime.strptime(tmp[1], "%Y-%m-%d")
        new_file = path + file.split(".")[0] + "_fill.csv"
        last_pay_date = None
        with open(new_file, 'w') as f:
            f.write("ticker,pay_date,dividend_amount,cnt,3year,3year_sum_dividend_amount,is_real_dividend_date\n")
            for d in data:
                # print "d:", d
                d.append("True")
                pay_date = datetime.strptime(d[1], "%Y-%m-%d")
                # print "pay_date:", pay_date, " last_pay_date:",last_pay_date," tmp_iter:",tmp_iter
                if pay_date == last_pay_date:
                    d[-1] = "True"
                    f.write(",".join(d) + "\n")
                    tmp_iter = pay_date + timedelta(days = 1)
                    continue
                while tmp_iter < pay_date:
                    tmp[1] = tmp_iter.strftime("%Y-%m-%d")
                    tmp[-1] = "False"
                    f.write(",".join(tmp) + "\n")
                    tmp_iter += timedelta(days = 1)
                d[-1] = "True"
                f.write(",".join(d) + "\n")
                tmp_iter = pay_date + timedelta(days = 1)
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

    (max_cnt_key, max_cnt) = (0,0)
    max = 0
    all_cnt = 0
    for key in map:
        max = key if map[key] > max else max
        (max_cnt_key, max_cnt) = (key, map[key]) if map[key] > max_cnt else (max_cnt_key, max_cnt)
        all_cnt += map[key]
    if (all_cnt - map[max_cnt_key]) != (max_cnt_key - 1):
        print filename, "历史中，一年最多：",max, "次分红，共发生：", map[max],"次"
        print "分红数次数出现概率最大的次数为", max_cnt_key, " 共有",map[max_cnt_key],"次数"
        print "其他分红次数共发生：",all_cnt - map[max_cnt_key]

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

#将交易数据和分红数据合起来成一个新文件
def join_z_dividend(fund,z_file, dividend_file, new_path):
    new_file = new_path + fund + "_z_dividend.csv"
    dateparse = pd.tseries.tools.to_datetime
    z_data = pd.read_csv(z_file,index_col="Date",parse_dates=True,date_parser=dateparse)
    dividend_data = pd.read_csv(dividend_file,index_col="pay_date",parse_dates=True,date_parser=dateparse)
    result = pd.concat([z_data, dividend_data], axis=1,join="inner")
    result.to_csv(new_file)
    return new_file

#加入一列，3年累计分红+（现价-3年前价格）
def cal_3years_profit(file, new_file):
    df = pd.read_csv(file,index_col=0)
    if df.empty:
        return
    df['3years_profit'] = df.apply(lambda x: 0, axis=1)
    start_date = datetime.strptime(df.index[0], "%Y-%m-%d")
    for iter in df.index:
        pay_date = datetime.strptime(iter, "%Y-%m-%d")
        key = pay_date.strftime("%Y-%m-%d")
        pay_date_3years_before = pay_date + timedelta(days = -365 * 3)
        key_before = pay_date_3years_before.strftime("%Y-%m-%d")
        if pay_date_3years_before >= start_date:
            while key_before not in df.index:
                pay_date_3years_before += timedelta(days = -1)
                key_before = pay_date_3years_before.strftime("%Y-%m-%d")
            price_before = df.loc[key_before,'Price']
            # print price_before
            profit = df.loc[key,'Price'] - price_before + df.loc[key,'3year_sum_dividend_amount']
            df.loc[key,'3years_profit'] = profit
            # print df.loc[key]
    #调整列顺序
    # cols = list(df)
    # cols.insert(1,cols.pop(cols.index('3years_profit')))
    # cols.insert(2,cols.pop(cols.index('3year_sum_dividend_amount')))
    # df = df.ix[:,cols]
    df.to_csv(new_file)

#检查最终数据里的基金是否齐全
def check_final(path, chosed_funds):
    res = []
    final_funds_files = get_files_postfix(path,"_final.csv")
    print len(final_funds_files), len(chosed_funds)
    for f in chosed_funds:
        tmp = f + "_final.csv"
        if tmp not in final_funds_files:
            res.append(f)
    return res

if __name__ == "__main__":
    dividend_original()

    #0. 封基轮动池
    # chosed_funds = get_chosed_fund()

    #1. 计算交易历史的折价率
    # files = get_files_prefix(".", "result")
    # for f in files:
        # print cal_discount(f, "./data/discount")

    #2. 计算z值
    # files = os.listdir("./data/discount")
    # for file in files:
    #     new_file = "./data/z/"+file.split(".")[0] + "_z.csv"
    #     cal_z("./data/discount/"+file, new_file)

    #cal_new_discount计算得到溢价率 = 价格/前一天的净值 -1
    #使用溢价率计算z值，发现与使用折价率计算z值相比，仅正负相反，其他一样
    # files = get_files_prefix(".", "result")
    # for f in files:
    #     print cal_new_discount(f, "./data/discountTest")
    # cal_z("./data/discountTest/BXMX.csv", "./data/z/BXMX.csv")

    #3. 将含有discount和z值的文件装入数据
    # discount_database()

    #发现数据库里比目标tikcers相关7个，分别是：EHT,JHB,JHD,JPT,NIQ,NID,NHA。
    #其中NIQ,NID,NHA这三项无历史交易数据;EHT,JHB,JHD,JPT交易数据不足一年。z值计算不出来，所以都没写入数据库。
    # tickers = pd.read_csv("mysql_test.csv")["ticker"].tolist()
    # tickers_from_sponsor = pd.read_csv("ticker_sponsor.csv")["ticker"].tolist()
    # for t in tickers_from_sponsor:
    #     if t not in tickers:
    #         print t


    # 4. 切割分红数据
    # spit_devidends("dividends.csv")

    #5. 计算一年内分红的次数
    # for file in os.listdir("./data/dividend"):
    #     if not "_" in file:
    #         cal_dividend_cnt("./data/dividend/" + file, "%Y-%m-%d")
    # dividend_cnt_per_year = cal_dividend_cnt("./data/dividend/EFR.csv", "%Y-%m-%d")
    # print dividend_cnt_per_year

    #6.检查每年基金的分红次数，输出不正常的
    # for file in os.listdir("./data/dividend"):
    #     if file.endswith("_new_365.csv"):
    #         check_divident_cnt("./data/dividend/" + file)
    # filename = "JPT.csv"
    # cal_dividend_cnt("./data/dividend/" + filename, "%Y-%m-%d")
    # check_divident_cnt("./data/dividend/"  + filename.split(".")[0] + "_new_365.csv")

    #7.1 标记是否符合三年内每次分红都不小于上一次（上次为特殊分红除外）
    #7.2 计算3年累计分红
    # path = "./data/dividend/"
    # for file in os.listdir("./data/dividend"):
    #     # if file == "BXMX_new_365.csv":
    #     if file.endswith("_new_365.csv"):
    #         new_file_name = path + file.split(".")[0] + "_3years.csv"
    #         # file = path + file
    #         # check_dividend_3years(file, new_file_name)
    #         # 计算三年累计分红
    #         dividend_with_sum = path + file.split(".")[0] + "_3years_sum.csv"
    #         sum_3years_dividend(new_file_name, dividend_with_sum)


    #8. 扩展分红数据到每一天
    # path = "./data/dividend/"
    # files = get_files_postfix(path, "_3years_sum.csv")
    # fill_dividend_data(files)

    #将分红数据写入数据库
    # dividend_database(files)

    #9.将历吏交易数据和历史分红数据合到一起
    # path_z = "./data/z/"
    # path_dividend = "./data/dividend/"
    # new_path ="./data/chosed_fund_data/"
    # chosed_funds = get_chosed_fund()
    # if not os.path.exists(new_path):
    #     os.makedirs(new_path)
    # for fund in chosed_funds:
    #     print fund
    #     z_file = path_z + fund + "_z.csv"
    #     dividend_file = path_dividend + fund + "_new_365_3years_sum_fill.csv"
    #     #NIQ NID NHA没有历史交易数据
    #     if os.path.exists(z_file) and os.path.exists(dividend_file):
    #         # new_file = join_z_dividend(fund,z_file,dividend_file,new_path)
    #         file = new_path + fund + "_z_dividend.csv"
    #         new_file = new_path + fund + "_final.csv"
    #         cal_3years_profit(file, new_file)

    # path = "./data/chosed_fund_data/"
    # ticker = "BXMX"
    # file = path + ticker + "_z_dividend.csv"
    # new_file = path + ticker + "_final.csv"
    # cal_3years_profit(file, new_file)

    #10. 检查final数据文件中，基金是数据是否齐全
    #检查结果显示：['EHT', 'JHB', 'JHD', 'NIQ', 'NID', 'NHA', 'JPT'] 未有最终数据文件。
    # 随机查看两个后发现基金是16年才开始有交易数据，不满1年。不符合轮动条件
    # res = check_final("./data/chosed_fund_data", get_chosed_fund())
    # print res

    # final_files = get_files_postfix("./data/chosed_fund_data/", "_final.csv")
    # print final_files
    # deal_dividend_database(["CEV_final.csv"])
    # deal_dividend_database(final_files)










