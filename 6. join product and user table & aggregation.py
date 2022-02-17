#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# Creator         :
# Create Date     :
# Modified Date   :
# Modified Content:
# Description     :join order table with user profile table and add aggregated column of purchase frequency and total purchase amount($) for each user


# import MySQLdb
import psycopg2
import pandas as pd
import numpy as np
import re

# from sLM39J30948 import data2sql
# from sLM39J30948 import truncate_data

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# import data
conn = psycopg2.connect(host="xxxx", port=14103, user="stork", password="stork", database="fm93demo")

dwd_fm93_goodcategory = pd.read_sql('select * from fm93demo.dwd_fm93_goodcategory', conn)
dim_fm93_order_goods = pd.read_sql('select * from fm93demo.ods_dim_fm93_order_goods_df', conn)

# join product table and product category table
dim_fm93_order_goods['order_good_id'] = dim_fm93_order_goods['order_good_id'].astype(str)

merge1_df = pd.merge(dwd_fm93_goodcategory, dim_fm93_order_goods, on='order_good_id', how='inner')
print('merge part 1 is over,data length{}'.format(merge1_df.__len__()))

# join order table with above table 
dim_fm93_order = pd.read_sql('select * from fm93demo.dwd_fm93_order_cleaned', conn)

merge2_df = pd.merge(merge1_df, dim_fm93_order, on='order_id', how='inner')
# print(merge2_df.columns)

del merge1_df
del dim_fm93_order
del dim_fm93_order_goods
del dwd_fm93_goodcategory

merge2_df = merge2_df[['order_id', 'origin_order_amount', 'order_create_time', 'buyer_phone_number',
                       'receiver_address_province', 'receiver_address_city', 'receiver_address_disctrict',
                       'receiving_address', 'category']]
print('merge part 2 is over,data length{}'.format(merge2_df.__len__()))

# join user table with above table 
userprofile_len = 1421513
step = 10000
route = int(round(userprofile_len / step, 0))
mergefinal_dfs = pd.DataFrame()
for i in range(route + 1):
    if i:
        read_sql = 'select user_click_id,phone_provience_name,source_id,last_login_time,id,phone,real_name, gender,age,phone_city_name,register_time,search_label_id,user_share_id from fm93demo.dwd_fm93_userprofile_cleaned limit ' + str(
            step) + ' offset ' + str(i * step)
    else:
        read_sql = 'select user_click_id,phone_provience_name,source_id,last_login_time,id,phone,real_name,gender,age,phone_city_name,register_time,search_label_id,user_share_id from fm93demo.dwd_fm93_userprofile_cleaned limit ' + str(
            step)

    dim_fm93_userprofile = pd.read_sql(read_sql, conn)
    mergefinal_df = pd.merge(merge2_df, dim_fm93_userprofile, left_on=['buyer_phone_number', 'receiver_address_city'],
                             right_on=['phone', 'phone_city_name'], how='inner')
    print('process {} in {}'.format(i, route))
    mergefinal_dfs = pd.concat([mergefinal_dfs, mergefinal_df])
# print(mergefinal_dfs.columns)

mergefinal_dfs = mergefinal_dfs[
    ['user_click_id', 'phone_provience_name', 'source_id', 'last_login_time', 'category', 'order_id',
     'id', 'phone', 'real_name', 'gender', 'age', 'phone_city_name', 'origin_order_amount',
     'register_time', 'search_label_id', 'user_share_id']]

# print(mergefinal_dfs[['user_click_id']].groupby('user_click_id').count())
# exit()

# add column "is she/he an app user"
import re

p = re.compile('[0-9]')
mergefinal_dfs['is_app'] = mergefinal_dfs['user_click_id'].apply(lambda x: 1 if re.findall(p, x).__len__() > 0 else 0)
print(mergefinal_dfs[['is_app']].groupby('is_app').count())

# calculate purchase frequency of each user
count_df = mergefinal_dfs[['id', 'order_id']].groupby('id').count()
count_df['num_order'] = count_df['order_id']
count_df['id_count'] = count_df.index
count_df = count_df[['id_count', 'num_order']]
mergefinal_dfs = pd.merge(mergefinal_dfs, count_df, how='left', left_on='id', right_on='id_count')
del count_df

# add column "province", "city"
mergefinal_dfs['phone_provience_name'] = mergefinal_dfs['phone_provience_name'].astype(str)
mergefinal_dfs['phone_provience_name'] = mergefinal_dfs['phone_provience_name'].apply(lambda x:
                                                                                      x.replace('Null', '').replace(
                                                                                          'None', '').replace('NaN',
                                                                                                              ''))
mergefinal_dfs['phone_city_name'] = mergefinal_dfs['phone_city_name'].astype(str)
mergefinal_dfs['phone_city_name'] = mergefinal_dfs['phone_city_name'].apply(lambda x:
                                                                            x.replace('Null', '').replace('None',
                                                                                                          '').replace(
                                                                                'NaN', ''))
mergefinal_dfs['province_city'] = mergefinal_dfs['phone_provience_name'] + '-' + mergefinal_dfs['phone_city_name']

# calculate total purchase amount of each user
sum_df = mergefinal_dfs[['id', 'origin_order_amount']].groupby('id').sum()
sum_df['sum_amount'] = sum_df['origin_order_amount']
sum_df['id_sum'] = sum_df.index
sum_df = sum_df[['id_sum', 'sum_amount']]
mergefinal_dfs = pd.merge(mergefinal_dfs, sum_df, how='left', left_on='id', right_on='id_sum')

# categories into one row
category_list = []
for line in mergefinal_dfs[['id', 'category']].groupby('id'):
    c = ','.join(np.unique(line[1]['category'].to_list()))
    category_list.append((line[0], c))
cate_df = pd.DataFrame(category_list, columns=['id', 'category_sum'])
mergefinal_dfs = pd.merge(mergefinal_dfs, cate_df, how='left', on='id')

del cate_df

# delete duplicae user rows
mergefinal_dfs.drop_duplicates(subset=['id'], inplace=True, keep='first')

# mergefinal_dfs['order_number'] =
# del dim_fm93_userprofile
# del mergefinal_df
# print('userprofile is loaded,data length{}'.format(dim_fm93_userprofile.__len__()))
# print(dim_fm93_userprofile[['phone','phone_city_name']])
# dim_fm93_userprofile= dim_fm93_userprofile.dropna(axis='index', how = 'any', subset=['phone','phone_city_name'])
# merge2_df = merge2_df.dropna(axis='index', how = 'any', subset=['buyer_phone_number','receiver_address_city'])
# print('userprofile is cleaned,data length{}'.format(dim_fm93_userprofile.__len__()))
# print('merge2 is cleaned,data length{}'.format(merge2_df.__len__()))

# mergefinal_df = pd.merge(merge2_df,dim_fm93_userprofile,left_on=['buyer_phone_number','receiver_address_city'],
#                         right_on = ['phone','phone_city_name'], how='inner')

# a.buyer_phone_number = b.phone and a.receiver_address_city = b.phone_city_name
mergefinal_dfs.drop(columns=['category', 'origin_order_amount', 'order_id', 'id_sum', 'id_count'], inplace=True)

print('merge part 3 is over,data length{}'.format(mergefinal_dfs.__len__()))

mergefinal_dfs['age'] = mergefinal_dfs['age'].apply(lambda x: x if isinstance(x, int) else 0)

print(mergefinal_dfs.head(5))
print(mergefinal_dfs.columns)


def truncate_data(tablename, conn):
    cursor = conn.cursor()
    sql = 'truncate table ' + tablename
    cursor.execute(sql, conn)
    conn.commit()


def data2sql(tablename, conn, df, col_list_sql):
    cursor = conn.cursor()
    # df = df[:,col_list_df]
    # 记得df的cols何上传的cols内容一致且顺序相同
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


truncate_data(tablename='fm93demo.dws_user_table_total', conn=conn)
# exit()
# mergefinal_df.to_csv('./opttest.csv',encoding='utf-8-sig',index=False)
data2sql(tablename='fm93demo.dws_user_table_total', conn=conn, df=mergefinal_dfs,
         col_list_sql=['user_click_id', 'phone_provience_name', 'source_id', 'last_login_time',
                       'id', 'phone', 'real_name', 'gender', 'age', 'phone_city_name',
                       'register_time', 'search_label_id', 'user_share_id', 'is_app',
                       'num_order', 'province_city', 'sum_amount', 'category_sum'])

# data2sql(con=conn,)

