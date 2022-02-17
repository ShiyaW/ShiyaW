#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#Creator         :
#Create Date     :
#Modified Date   :
#Modified Content:
#Description     :Calculate the number of users of each app function


import psycopg2
import pandas as pd
# from sLM39J30948(dws_fm93_GMV) import data2sql
# from sN2292W5T08(merge_table) import truncate_data

conn =psycopg2.connect(host="xxxx",port = 14103,user = "stork",password = "stork",database = "fm93demo")
data_list = []
cursor = conn.cursor()
#Calculate the number of users of each app function
for i in range(1,9):
  get_sql1 ="select count(source_id) from fm93demo.ods_dim_fm93_userprofile_df where user_click_id like '%" + str(i) + "%';"
  cursor.execute(get_sql1)
  data = cursor.fetchall()
  data_cut = data[0]
  data_cut2 = data_cut[0]
  data_list.append(data_cut2)
  # print (data_cut2)
# print (data_list)

# write into dataframe
a = ['路况预警','高速缴罚','高速路况','市区路况','挪车服务','违章查询','寻人寻物','失物招领']
b = data_list
c = {"a":a,"b":b}
df = pd.DataFrame(c)
# print(df)

# store into database
def data2sql(tablename, conn, df, col_list_sql):
    cursor = conn.cursor()
    cols_sql = ','.join(col_list_sql)
    s_sql = ','.join(['%s'] * col_list_sql.__len__())
    for row in df.values:
        value_list = []
        sql = "insert into " + tablename + "(" + cols_sql + ")" \
              + "\nvalues(" + s_sql + ")"

        for v in row:
            value_list.append(str(v))

        cursor.execute(sql, value_list)
        conn.commit()
def truncate_data(tablename,conn):
  cursor = conn.cursor()
  sql = 'truncate table ' + tablename
  cursor.execute(sql,conn)
  conn.commit()
truncate_data(tablename='fm93demo.dws_fm93_user_app',conn=conn)
data2sql(tablename = 'fm93demo.dws_fm93_user_app',conn = conn, df = df, col_list_sql = ['app','users'])
#用户使用APP服务功能 （1路况预警，2高速缴罚，3高速路况，4市区路况，5挪车服务，6违章查询 7.寻人寻物 8.失物招领）