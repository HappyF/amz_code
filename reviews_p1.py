#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 18:53:04 2021

@author: happyf
"""

import pymysql
import pandas as pd
import re

# 从数据库中获取各自的类目
conn=pymysql.connect(host='rm-m5ex5024i8851023yqo.mysql.rds.aliyuncs.com',user='amz1',password='amz123456+',database='amz_data')
cur=conn.cursor()


# 