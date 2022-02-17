#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#Creator         :
#Create Date     :
#Modified Date   :
#Modified Content:
#Description     :Calculate monthly conversion of newly registered users (newly registered users for the month that placed order/newly registered users for the month)

import psycopg2
import pandas as pd
import re

conn =psycopg2.connect(host="10.20.56.84",port = 14103,user = "stork",password = "stork",database = "fm93demo")
def transfer_str(input_str):
  year,month = input_str.split('-')
  month = int(month)
  opt = str(month) + '/' +  year
  return opt


def get_monthly_conv(conn):
    cursor = conn.cursor()
    get_table_sql = 'select * from fm93demo.dws_fm93_newregister_user'
    table_df = pd.read_sql(sql=get_table_sql, con=conn)
    time_zones = table_df['year_month']
    m_convs = []
    for time_zone in time_zones:
      time_zone = transfer_str(time_zone)
      #calculate newly registered users for the month that placed order
      select_sql = "select count(distinct id) from (select * from fm93demo.ods_dim_fm93_userprofile_df where register_time like  '%" + time_zone + "%' ) a\
                     inner join (select * from fm93demo.ods_dim_fm93_order_df where order_create_time like '%" + time_zone + "%') b\
                     on a.phone = b.buyer_phone_number and a.phone_city_name = b.receiver_address_city"
      cursor.execute(select_sql)
      conn.commit()
      data_cell_son = cursor.fetchall()
      data_cell_son = data_cell_son[0]
      data_cell_son = data_cell_son[0]
      #calculate newly registered users for the month
      select_sql = "select count(distinct id) from fm93demo.ods_dim_fm93_userprofile_df where register_time like '%" + time_zone + "%'"
      cursor.execute(select_sql)
      conn.commit()
      data_cell_mother = cursor.fetchall()
      data_cell_mother = data_cell_mother[0]
      data_cell_mother = data_cell_mother[0]
      monthly_conv = data_cell_son / data_cell_mother
      m_convs.append(monthly_conv)
    table_df['m_conv'] = m_convs
    return table_df

df = get_monthly_conv(conn)

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
truncate_data(tablename='fm93demo.dws_fm93_newregister_user_final',conn=conn)
data2sql(tablename = 'fm93demo.dws_fm93_newregister_user_final',conn = conn, df = df, col_list_sql = ['year_month','new_user','m_conv'])

