#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 21:37:51 2021

@author: happyf
"""

import pandas as pd #导入和处理数据
import pymysql # 插入数据库
import re # 处理文本
import datetime as dt # 增加时间
import glob # 批量处理
from tqdm import tqdm


# 数据录入，确定导入哪些
PATH='/Users/happyf/Desktop/amazon/d1/' # 数据所在
SAVE_PATH='/Users/happyf/Desktop/amazon/d1/save/'
file_list=glob.glob(PATH+'*.csv')


def get_reviews(n):
    try:
        n1=n.replace(',','')
        r1=re.findall(r'[0-9]+',n1)[0]
        return int(r1)
    except:
        return 0
    
def get_stars(n):
    #n=n.replace('','0.0')
    try:
        return n[:18].replace(' out of 5 stars','')
    except:
        return 0


# 抓取数据初步处理
def clean_df(df,file):
    # 把nan数据转换成‘’
    df_filter=df.fillna('')
    # 数据处理
    df_filter['stars']=df['stars'].apply(get_stars)
    df_filter['reviews']=df['reviews'].apply(get_reviews)
    #
    df_filter['price']=df['price'].apply(lambda x:str(x).replace('$','').replace(',',''))
    df_filter['rank_big']=df['rank_big'].apply(lambda x: x.replace('#',''))
    df_filter['rank_small']=df['rank_small'].apply(lambda x: x.replace('#',''))

    # 抓取日期，类目
    df_filter['insert_date']=dt.datetime.now().strftime("%Y-%m-%d")
    # df_filter['type']=file.split('-')[0].replace(PATH,'')
    df_filter['date']=file.split('-')[1].replace('.csv','')
    df_filter['class']=file.split('-')[0]
    return df_filter
    
# 如果通过测试，放到数据库中
def put_database(df):
    conn=pymysql.connect(host='rm-m5ex5024i8851023yqo.mysql.rds.aliyuncs.com',user='amz1',password='amz123456+',database='amz_data')
    cur=conn.cursor()
    for i in tqdm(range(len(df))):
        sql="insert into listdb(rank_small,title,url,reviews_url,stars,reviews,sell,rank_big,rank_big_name,asin,brand,on_shelf,buy_limit,price,insert_date,type,date,class) values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(df.loc[i,'rank_small'],df.loc[i,'title'].replace("'","\'"),df.loc[i,'url'],df.loc[i,'reviews_url'],df.loc[i,'stars'],str(df.loc[i,'reviews']), df.loc[i,'sell'],df.loc[i,'rank_big'],df.loc[i,'rank_big_name'],df.loc[i,'asin'],df.loc[i,'brand'].replace(".","\'"),df.loc[i,'on_shelf'],df.loc[i,'buy_limit'],df.loc[i,'price'],df.loc[i,'insert_date'],df.loc[i,'type'],df.loc[i,'date'],df.loc[i,'class'])
        print(sql)
        cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()

def main():
    if file_list:
        for file in file_list:
            #try:
            df=pd.read_csv(file)
            #return df
            df_final=clean_df(df,file)
            df_final.to_csv(SAVE_PATH+file.replace(PATH,''))
            print('======存入数据库=====')
            put_database(df_final)
            
            #except:
             #   continue
    else:
        print("文件路径有问题，群里提问")
        
        
if __name__ == '__main__':
    main()