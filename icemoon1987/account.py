#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################################
#
# File Name:  account.py
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
# Create Time:    2017-05-11 13:49:12
#
######################################################

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import time
import logging
from datetime import datetime, timedelta


class Stock(object):

    def __init__(self, id):

        self.id = id
        self.share = 0
        self.value = 0.0

        return


    def buy(self, price, share):

        self.share += share
        self.value = price * share

        return


    def sell(self, price, share):
        
        self.share -= share
        self.value = price * share

        return

    
    def get_mean_price(self):
        return self.value / self.share


    def update_value(self, price):
        self.value = price * self.share
        


class Account(object):

    def __init__(self, cash=0, min_trade_share=0, trade_unit=1):

        self.stock_map = {}
        self.cash = cash
        self.min_trade_share = min_trade_share
        self.trade_unit = trade_unit

        return


    def buy(self, stock_id, price, share):

        max_share = int(self.cash / price)

        if share > max_share:
            logging.warn("cash=%f, price=%f, max_share=%d, share=%d, can only buy %d share." % (self.cash, price, max_share, share, max_share))
            share = max_share

        if share < self.min_trade_share:
            logging.warn("share=%d, min_trade_share=%d, can not buy any share." % (share, self.min_trade_share))
            return 0

        if share % self.trade_unit != 0:
            true_share = int(share / self.trade_unit) * self.trade_unit
            logging.warn("share=%d, trade_unit=%d, can only buy %d share." % (share, self.trade_unit, true_share))
            share = true_share

        if stock_id not in self.stock_map:
            self.stock_map[stock_id] = Stock(stock_id)

        self.stock_map[stock_id].buy(price, share)
        self.cash -= price * share
        logging.info("buy ticker %s %d share at %f" % (stock_id, share, price))

        return share


    def sell(self, stock_id, price, share):

        if stock_id not in self.stock_map:
            logging.warn("not holding this stock. share_id = %s" % (stock_id))
            return 0

        if share < self.min_trade_share:
            logging.warn("share=%d, min_trade_share=%d, can not sell any share." % (share, self.min_trade_share))
            return 0

        if share % self.trade_unit != 0:
            true_share = int(share / self.trade_unit) * self.trade_unit
            logging.warn("share=%d, trade_unit=%d, can only sell %d share." % (share, self.min_trade_share, true_share))
            share = true_share

        if share > self.stock_map[stock_id].share:
            logging.warn("share=%d, holding_share=%d, can only sell %d share." % (share, self.stock_map[stock_id].share, self.stock_map[stock_id].share))
            share = self.stock_map[stock_id].share

        self.stock_map[stock_id].sell(price, share)
        self.cash += price * share

        if self.stock_map[stock_id].share == 0:
            del self.stock_map[stock_id]

        logging.info("sell ticker %s %d share at %f" % (stock_id, share, price))
        
        return share


    def get_value(self):

        value = 0

        for stock_id in self.stock_map:
            value += self.stock_map[stock_id].value

        value += self.cash

        return value


    def update_value(self, price_map):

        for stock_id in self.stock_map:
            self.stock_map[stock_id].update_value(price_map[stock_id])

        return self.get_value()

    
    def get_stock_list(self):
        return self.stock_map.keys()


    def __str__(self):

        result = ""

        for stock_id in self.stock_map:
            result += "stock_id=%s, share=%d, value=%f\n" % ( self.stock_map[stock_id].id, self.stock_map[stock_id].share, self.stock_map[stock_id].value)

        result += "cash=%f\n" % (self.cash)
        result += "value=%f\n" % (self.get_value())

        return result


if __name__ == "__main__":

    account = Account(cash=100000, min_trade_share=100, trade_unit=100)

    print "normal: buy aaa"
    account.buy("aaa", 10.0, 500)
    print str(account)

    print "normal: buy bbb"
    account.buy("bbb", 15.0, 200)
    print str(account)

    print "normal: sell aaa"
    account.sell("aaa", 20.0, 500)
    print str(account)

    print "normal: sell bbb"
    account.sell("bbb", 30.0, 100)
    print str(account)

    print "normal: sell bbb"
    account.sell("bbb", 30.0, 100)
    print str(account)

    account = Account(cash=100000, min_trade_share=100, trade_unit=100)

    print "buy share > max share"
    account.buy("aaa", 10.0, 999999999)
    print str(account)

    print "sell > holding share"
    account.sell("aaa", 10.0, 999999999)
    print str(account)

    print "buy share < min_trade_share"
    account.buy("aaa", 10.0, 99)
    print str(account)

    print "buy share % trade_unit != 0"
    account.buy("aaa", 10.0, 113)
    print str(account)

    print "sell share % trade_unit != 0"
    account.sell("aaa", 10.0, 113)
    print str(account)

    print "buy aaa"
    account.sell("aaa", 10.0, 100)
    print str(account)

    print "sell share < min_trade_share"
    account.sell("aaa", 10.0, 99)
    print str(account)

    print "sell unhold stock"
    account.sell("bbb", 10.0, 100)
    print str(account)


