#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#Creator         :
#Create Date     :
#Modified Date   :
#Modified Content:
#Description     :create age group; calculate number of users and their conversion rate within each age group

import psycopg2
import pandas as pd
from sLM39J30948(dws_fm93_GMV) import data2sql
from sN2292W5T08(merge_table) import truncate_data

#import data
conn =psycopg2.connect(host="10.20.56.84",port = 14103,user = "stork",password = "stork",database = "fm93demo")
sql = "select * from fm93demo.dwd_fm93_user_age_gmv"
sql2 = "select * from fm93demo.dwd_fm93_user_age_conv"
data = pd.read_sql(sql,conn)
data2 = pd.read_sql(sql2,conn)

#set max age for each age group
group_max_ages = [10,20,30,40,50,60,70,80,90,100]

#create intial arrays
sums = [0] * len(group_max_ages)
counts = [0] * len(group_max_ages)
counts2 = [0] * len(group_max_ages)

#calculate number of customers and their average GMV within each age group
idx = 0
for _, row in data.iterrows():
    if row['age'] > group_max_ages[idx]:
        idx += 1

    sums[idx] += row['count_age'] * row['avg_gmv']
    counts[idx] += row['count_age']

avg = [s / c for s, c in zip(sums, counts)]

#calculate number of customers and their conversion rate within each age group
idx2 = 0
for _, row in data2.iterrows():
    if row['age'] > group_max_ages[idx2]:
        idx2 += 1

    counts2[idx2] += row['count_age_all']

conv = [c / c2 for c, c2 in zip (counts,counts2)]
# conv_perc = ["%.1f%%" % (i * 100) for i in conv]

# store into dataframe
a = ['0-10','11-20','21-30','31-40','41-50','51-60','61-70','71-80','81-90','91-100']
b = avg
c = conv
d = {"a":a,"b":b,"c":c}
df = pd.DataFrame(d)

# store into database
truncate_data(tablename='fm93demo.dws_fm93_agegroup',conn=conn)
data2sql(tablename = 'fm93demo.dws_fm93_agegroup',conn = conn, df = df, col_list_sql = ['age_group','avg_gmv','conv'])
