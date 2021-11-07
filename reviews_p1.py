#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 18:53:04 2021

@author: happyf
"""

import pymysql
import pandas as pd
from tqdm import tqdm

cls='E:learningamazonlistmonitors' # 所属产品
n=3 # 多少天

# 从数据库中获取各自的类目
def get_data_from_mysql(sql):
        conn = pymysql.connect(host='rm-m5ex5024i8851023yqo.mysql.rds.aliyuncs.com',user='amz1',password='amz123456+',database='amz_data')
        cursor = conn.cursor()

        cursor.execute(sql)
        data = cursor.fetchall()
        return data

# 先选择产品，再排重，下面SQL的逻辑，是选取每个asin中最近一次抓取时的asin
sql="""
-- select t1.date, t2.asin,t1.rank_big_mean,t1.rank_small_mean,t2.reviews
select t2.asin,t2.reviews
from
(select max(date) date,asin,round(avg(rank_big),0) rank_big_mean, round(avg(rank_small),0) rank_small_mean,count(asin) as n from listdb 
where class="{}" 
group by asin) t1
left JOIN listdb t2
on t1.date=t2.date and t1.asin=t2.asin
where t1.n>={} and t2.reviews>0
""".format(cls,n)

#
df_reviews=pd.DataFrame(get_data_from_mysql(sql),columns=['asin','reviews'])
df_reviews['reviews']=df_reviews['reviews'].apply(lambda x:int(x.replace('.0','')))

base_url='https://www.amazon.com/product-reviews/'
reviews_p1_url=[]
# 进行判断
# 如果小于10条，直接全抓，无论怎么样都是这些
url_less=df_reviews[df_reviews.reviews<=10]['asin']
for asin in url_less:
    reviews_p1_url.append(base_url+asin+'?currency=USD&language=en_US&sortBy=helpful&pageNumber=')

# 如果大于10条，分开抓，最后聚合
url_more=df_reviews[df_reviews.reviews>10]['asin']
info=[
        ('Image and video',"?currency=USD&language=en_US&sortBy=helpful&mediaType=media_reviews_only&pageNumber="), # 全部
        ('Critical','?currency=USD&language=en_US&sortBy=helpful&filterByStar=critical&pageNumber='), # 250//10
        ('Recent','?currency=USD&language=en_US&sortBy=recent&reviewerType=all_reviews&pageNumber='), # 500
        ('Positive','?currency=USD&language=en_US&sortBy=helpful&filterByStar=positive&pageNumber='), # 250
        ('Helpful','?currency=USD&language=en_US&sortBy=helpful&pageNumber=') # 500
    ]

# 输出txt
for asin in url_more:
        for i,j in info:
                reviews_p1_url.append(base_url+asin+j)

PATH_REVIEWS_URL='E:\\amz_url\\more_url.txt'
with open(PATH_REVIEWS_URL,'w') as f:
    for i in tqdm(reviews_p1_url):
        f.writelines(i+'\n')