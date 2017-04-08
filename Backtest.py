#coding:utf-8
__author__ = 'Administrator'
from datetime import datetime,timedelta
import MySQLdb
import matplotlib.pyplot as plt
import logging



class PriceShare:
    def __init__(self, price, share):
        self.price = price
        self.share = share

    def worth(self):
        return self.price * self.share

    def to_string(self):
        return "price:" + str(self.price) + " share:" + str(self.share) + " value:" + str(self.worth())

    def get_share(self):
        return self.share

    def get_price(self):
        return self.price

class HoldFunds:

    def __init__(self, amount):
        self.amount = amount
        self.cash = amount
        #{ticker:PriceShare}
        self.hold_funds = {}
        #预期可以拿到的分红{日期：[(ticker，金额)]}
        self.expected_dividend = {}
        logging.basicConfig(level=logging.DEBUG,
                            format='%(message)s',
                            datefmt='',
                            filename=datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S") + ".csv",
                            filemode='w')

    def refresh(self, date):
        new_amount = 0.0
        ticker_cnt = len(self.hold_funds)
        if ticker_cnt == 0:
            return
        db = MySQLdb.connect("127.0.0.1", "root", "lala", "fengji_usa")
        for ticker in self.hold_funds:
            sql = "SELECT price From fund_dividend WHERE ticker = '%s' and deal_date = '%s'" %(ticker,date)
            # print sql
            cur = db.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            # print "len(data):", len(data)
            if len(data) == 1:
                self.hold_funds[ticker].price = float(data[0][0])
            new_amount += self.hold_funds[ticker].worth()
        db.close()
        if
        self.amount = new_amount + self.cash
        return

    def refresh_amount(self):
        new_amount = 0.00
        for f in self.hold_funds:
            new_amount += self.hold_funds[f].worth()
        self.amount = new_amount + self.cash

    def get_dividend(self,date):
        db = MySQLdb.connect("localhost", "root", "lala", "fengji_usa")
        for ticker in self.hold_funds:
            price_share = self.hold_funds[ticker]
            cur = db.cursor()
            sql = "SELECT payable_date,amount FROM dividend_original WHERE ticker = '%s' AND ex_date = '%s'" %(ticker,date)
            cur.execute(sql)
            data = cur.fetchall()
            for d in data:
                payable_date = d[0]
                dividend_amount = d[1]
                # print "buy_date:", buy_date.date(), " ex_date:", ex_date
                tmp = price_share * dividend_amount
                if payable_date in self.expected_dividend:
                    self.expected_dividend[payable_date] += tmp
                else:
                    self.expected_dividend[payable_date] = tmp
        db.close()
        return

    def buy_funds(self, targets,date,expense_ratio = 0):
        tickers = self.need2buy(targets)
        if len(tickers) == 0:
            return
        print "need to buy_funds:", tickers
        cnt = len(tickers)
        for t in tickers:
            price = targets[t]
            share = int(self.cash / cnt / price)
            self.hold_funds[t] = PriceShare(price, share)
            self.cash -= price * share * (1 + expense_ratio)
            self.cash = round(self.cash, 4)
        self.refresh_amount()
        return

    def buy_fund(self, ticker, price, cash, expense_ratio=0):
        share = int(cash / price)
        if share == 0:
            return
        self.cash -= price * share * (1 + expense_ratio)
        self.cash = round(self.cash, 4)
        if not ticker in self.hold_funds:
            self.hold_funds[ticker] = PriceShare(price, share)
        else:
            hold_share = self.hold_funds[ticker].get_share()
            new_share = hold_share + share
            self.hold_funds[ticker] =  PriceShare(price, new_share)
        # self.refresh_amount()

    def sell_funds(self, targets,expense_ratio = 0):
        tickers = self.need2sell(targets)
        if len(tickers) == 0:
            return
        print "need to sell_funds:",tickers
        for t in tickers:
            tmp = self.hold_funds[t].worth()
            self.cash += tmp * (1 + expense_ratio)
            self.cash = round(self.cash, 4)
            self.hold_funds.pop(t)
        return

    def need2buy(self, targets):
        target_tickers = targets.keys()
        hold_tickers = self.hold_funds.keys()
        return list(set(target_tickers).difference(set(hold_tickers)))

    def need2sell(self, targets):
        target_tickers = targets.keys()
        hold_tickers = self.hold_funds.keys()
        return list(set(hold_tickers).difference(set(target_tickers)))

    def to_string(self):
        res = "amount:" + str(self.amount) + " cash:" + str(self.cash)
        for t in self.hold_funds:
            res += " ticker:" + t + " "\
                   + " content:" + self.hold_funds[t].to_string()
        return res

    def to_csv_line(self, today):
        # contents = ['today',"value","cash","ticker","buy_date","price","share"]
        contents = [datetime.strftime(today, "%Y-%m-%d"), str(self.amount), str(self.cash)]
        tmp = ""
        for ticker in self.hold_funds:
            tmp += " " + ticker + ","
            tmp += self.hold_funds[ticker].to_string() + ","
        tmp.strip()
        contents.append(tmp)
        # print "contents:", ",".join(contents)
        return ",".join(contents)
        # return contents

    #targets: {ticker:price}
    def chagne_holds(self, date, targets):
        # print date
        tmp = self.amount
        # print "before refresh:", self.to_string()
        self.refresh(date)
        if self.amount / tmp < 0.99:
            print date,(self.amount/tmp - 1) * 100, tmp,self.to_string()
        # print date.date()," after refresh:", self.to_string()
        self.sell_funds(targets)
        # print "after sell_funds:", self.to_string()
        self.buy_funds(targets,date)
        # print "after buy_funds:", self.to_string()
        contents = self.to_csv_line(date)
        logging.info(contents)
        return self.amount

def lundong(start_date, end_date, cnt, start_money):
    date = datetime.strptime(start_date,"%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    account = HoldFunds(start_money)
    logging.info("deal_date,amount,cash,holds_ticker, buy_date, price_share_value")
    amount = []
    dates = []

    db = MySQLdb.connect("127.0.0.1","root","lala","fengji_usa")
    while date <= end:
        date_str = datetime.strftime(date, "%Y-%m-%d")
        # sql = "SELECT ticker, z*discount*cnt*amount/price AS score, price FROM fund_dividend WHERE " \
        #       "deal_date='%s' ORDER BY score DESC " \
        #       "limit %s;" %(date_str,cnt)
        # sql = "SELECT a.ticker,a.score,a.price FROM (SELECT deal_date, ticker, price, nat, discount, amount, " \
        #       "cnt, z, 3years, 3years_profit, z*discount*cnt*amount/price AS score FROM fund_dividend WHERE " \
        #       "deal_date='%s' ORDER BY 3years_profit DESC LIMIT 10) AS a ORDER BY score DESC " \
        #       "limit %s;" %(date_str,cnt)
        sql = "SELECT a.ticker,a.score,a.price FROM (SELECT deal_date, ticker, price, nat, discount, amount, " \
              "cnt, z, 3years, 3years_profit, z*discount*cnt*amount/price AS score FROM fund_dividend WHERE " \
              "deal_date='%s' AND 3years = 1 ORDER BY 3years_profit DESC LIMIT 10) AS a ORDER BY score DESC " \
              "limit %s;" %(date_str,cnt)
        # print sql
        try:
            cursor = db.cursor()
            cursor.execute(sql)
            funds = {}
            fetchall = cursor.fetchall()
            if len(fetchall) <= 0:
                date += timedelta(days = 1)
                continue
            for data in fetchall:
                # print data
                #data[0]:ticker, data[1]:综合得分z*discount*divident_ratio, data[2]:price
                #funds[ticker] = (score,price)
                # print "data[2]:", data[2]," float(data[2]):",float(data[2])
                funds[data[0]] = float(data[2])
                # print "funds:", funds
        except Exception, e:
            print e
        dates.append(date)
        amount.append(account.chagne_holds(date, funds))
        account.get_dividend(date)
        date += timedelta(days = 1)
    db.close()
    plt.plot(dates, amount)
    plt.show()
    return

if __name__ == "__main__":
    # start_date = datetime.strptime("2011-02-03","%Y-%m-%d") + timedelta(days = 365 * 3)
    # end_date = datetime.strptime("2017-03-07","%Y-%m-%d")

    #数据库中数据反馈，最早的交易日期为2011-02-03，最早可回测的日期为3年后的2014-02-03
    #最晚的交易日期为2017-03-07
    # start_date = "2014-02-03"
    # end_date = "2017-03-07"
    start_date = "2016-05-06"
    end_date = "2017-03-07"
    lundong(start_date, end_date, 1, 100000)