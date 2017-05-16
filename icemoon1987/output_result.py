#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################################
#
# File Name:  output_result.py
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
# Create Time:    2017-05-12 18:26:39
#
######################################################

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def draw_value_result(value_result_file):

    data = pd.read_csv(value_result_file)
    data.set_index("Date", inplace=True)

    data.plot()
    plt.grid(True)
    plt.show()

    return


def main():

    draw_value_result("./result/value_result.csv")

    return

if __name__ == "__main__":
    main()
