import requests
import json
import pandas as pd
import datetime
import calendar
import MySQLdb

conn =MySQLdb.connect(host="xx", port=3306, user="root", password="123456", database="my_test", charset='utf8')

# Get the time range list

def get_time_range_list(startdate, enddate):

    # :param startdate: month_begin_date --> str
    # :param enddate: month_end_date --> str
    # :return: date_range_list -->list

    date_range_list = []
    startdate = datetime.datetime.strptime(startdate,'%Y-%m-%d')
    enddate = datetime.datetime.strptime(enddate,'%Y-%m-%d')
    while 1:
        next_month = startdate + datetime.timedelta(days=calendar.monthrange(startdate.year, startdate.month)[1])
        month_end = next_month - datetime.timedelta(days=1)
        if month_end < enddate:
            date_range_list.append((datetime.datetime.strftime(startdate,
                                                                   '%Y-%m-%d'),
                                        datetime.datetime.strftime(month_end,
                                                                    '%Y-%m-%d')))
            startdate = next_month
        else:
            date_range_list.append((datetime.datetime.strftime(next_month - datetime.timedelta(days=calendar.monthrange(startdate.year, startdate.month)[1]),
                                                                   '%Y-%m-%d'),
                                        datetime.datetime.strftime(enddate,
                                                                   '%Y-%m-%d')))
            return date_range_list
now = datetime.datetime.now()
range_list = get_time_range_list("2001-01-01",now.strftime('%Y-%m-%d'))

# extract data from url
df_list = pd.DataFrame()
for i in range (0,len(range_list)):
    baseurl = "http://xx"
    params = {
        "beginDate" : range_list[i][0],
        "endDate" : range_list[i][1]
    }
    r=requests.get(baseurl,params=params)
    # print(type(r.text))
    # print(r.json())
    # print(json.loads(r.text))
    r_py = json.loads(r.text)
    data = r_py['data']['data']
    # print(type(r_py),type(r))
    df = pd.DataFrame.from_dict(data)
    print('session'+str(i)+'finished')
    df_list = df_list.append(df)

# store data into database
print(df_list)
def data2sql(tablename, conn, df, col_list_sql):
    cursor = conn.cursor()
    # df = df[:,col_list_df]
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

data2sql(tablename = 'test_loaduser',conn = conn, df = df_list, col_list_sql = ['id','gender','age','birthday','hasKid','phoneProvienceName','phoneCityName','addressProvience','addressCity','addressDisctrict','occupation','zodiacName','constellationName','carBrand','carBrandPattern','driveLicenseType','driveLicenseBirthday','longitude','latitude','carType','sourceId','registerTime','lastLoginTime','province','city','subscribe','subscribeTime'])