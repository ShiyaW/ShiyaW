#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#Creator         :
#Create Date     :
#Modified Date   :
#Modified Content:
#Description     :Word segmentation of product name; define product categories

import psycopg2
import pandas as pd
import jieba
import re
import jieba.posseg as jp
from sLM39J30948(dws_fm93_GMV) import data2sql

conn =psycopg2.connect(host="10.20.56.84",port = 14103,user = "stork",password = "stork",database = "fm93demo")
sql = "select * from fm93demo.ods_dim_fm93_order_goods_df"
data = pd.read_sql(sql, conn)

#import stopwords library
with open(r'/root/fm93demo/baidu_stopwords.txt',encoding='utf-8') as f:
    stopwords = f.readlines()

stopwords = [x.replace('\n','') for x in stopwords]
def kill_special(input_s):
    p = re.compile('[\u4E00-\u9fa5a-zA-Z]')
    return ''.join(re.findall(p,input_s))

dt = data.loc[:,['good_name','order_good_id']]

seg_lists = []

for i in range(len(dt['good_name'])):
    input = dt['good_name'][i]
    input = kill_special(input)
    # seg_list = list(jieba.cut(input, cut_all=False,HMM=True))
    seg_list = jp.lcut(input) # jieba word segmentation
    seg_list_transfered = [list(x) for x in seg_list]
    seg_list_filtered = [x[0] for x in seg_list_transfered if x[1] in ['n','ns']] #filtering part of speech (keep nouns)
    seg_list_outstop = [x for x in seg_list_filtered if x not in stopwords] #filtering stopwords
    seg_list_outstop.append(dt['order_good_id'][i])
    seg_lists.append(seg_list_outstop)

#create dictionary to define categories
category_label = {'车':'车载用品','驾驶':'车载用品','燃油':'车载用品', '车载':'车载用品','油路':'车载用品','添加剂':'车载用品',
                  '清洁剂':'车载用品','碳':'车载用品','除积':'车载用品','尾气':'车载用品','汽油':'车载用品','柴油':'车载用品','清洗剂':'车载用品',
                  '阿克苏':'公益活动', '公益活动':'公益活动',
                  '音乐节':'活动门票','门票':'活动门票',
                  '新疆':'食品饮料','冰糖':'食品饮料','丸':'食品饮料','葡萄':'食品饮料','山核桃':'食品饮料','龙井':'食品饮料',
                  '月饼':'食品饮料','水蜜桃':'食品饮料'}

#classify according to dictionary
category_list = []
for seg_list_outstop in seg_lists:
    category = ''
    for word in seg_list_outstop:
        if word in category_label:
            category = category_label[word]
    if category == '':
        category = '其他品类'
    category_list.append(category)

#generate order_good_id list
id_list = []
for seg_list_outstop in seg_lists:
  id_list.append(seg_list_outstop[-1])
# print (id_list)
    # print(seg_list_outstop[-1],category)

#combine order_good_id and cateogry_id 
output_Data = pd.DataFrame(columns=['seg','cat'])

output_Data['seg'] = id_list
output_Data['cat'] = category_list

#store into database
data2sql(tablename = 'fm93demo.dwd_fm93_goodcategory',conn = conn, df = output_Data, col_list_sql = ['order_good_id','category'])
# cursor = conn.cursor()
# cursor.execute("select * from fm93demo.ods_dim_fm93_order_goods_df limit 10000")
# data = cur.fetchall()