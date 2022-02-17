#Revnue and average order value by month

#reformatting order_create_time to "YYYY-MM"
data['order_create_time'] = pd.to_datetime(data['order_create_time'], format='%d/%m/%Y %H:%M:%S')
data['order-ym']=data['order_create_time'].dt.strftime("%Y-%m")

#sum origin_order_amount by month
data['origin_order_amount']=pd.to_numeric(data['origin_order_amount'])
sales_ym = data.groupby(by='order-ym')['origin_order_amount'].sum()

#number of orders and average ordr value by month
sales_no = data.groupby(by='order-ym').size()
aov_ym = sales_ym/sales_no

sales_ym = sales_ym.to_frame()
sales_ym['year_month'] = sales_ym.index
# print(sales_ym)

aov_ym = aov_ym.to_frame()
aov_ym['year_month'] = aov_ym.index

conn =psycopg2.connect(host="xxxx",port = 14103,user = "stork",password = "stork",database = "fm93demo")

#Revenue month over month change
sql = "select * from fm93demo.dwd_fm93_gmv"
data = pd.read_sql(sql,conn)
rec_row = data[-2:-1]
rec_row2 = data[-3:-2]

gmv = rec_row.iat[0,1]
gmv2 = rec_row2.iat[0,1]
rate = gmv/gmv2 -1

data_input = [[rec_row.iat[0,0],rate]]
df = pd.DataFrame(data_input,columns=['year_month','rate'])