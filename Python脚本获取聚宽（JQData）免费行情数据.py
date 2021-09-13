#!/usr/bin/python
# -*- coding: UTF-8 -*-
'''
get_option_daily_price

作者：汪sir
日期：2020年1月29日
参数列表说明
argv[1]：传入标的代码，如510050.XSHG，该参数不能缺失；
argv[2]：传入交易日期，如2019-12-02，该参数可选；
功能说明：
从聚宽数据API 接口中查询下载opt_daily_price表中的数据，筛选条件为标的代码+交易日期
'''
""" 
四章： Python脚本获取聚宽（JQData）免费行情数据

https://zhuanlan.zhihu.com/p/266164156

"""

from jqdatasdk import *
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import sys
import os
import datetime

JQAPI_USERNAME = '聚宽用户名'
JQAPI_PASSWORD = '聚宽密码'
TABLE_NAME = 'opt_daily_price'
DATABASE_SCHEMA = 'options_data'
DEFAULT_ERROR_MESAGE = 'have_no_error_message'
DATABASE_CONNECT_STR = 'mysql+pymysql://数据库用户名:数据库密码@云数据库内网地址/数据库名?charset=utf8'


#获取聚宽API连接
def get_connect_from_api():
	auth(JQAPI_USERNAME, JQAPI_PASSWORD)
	print('login sucessed:' + str(is_auth()))

#判断是否为交易日，不是交易日则关闭连接并退出程序
def is_trade_day(data_date):
	if len(get_trade_days(start_date=data_date, end_date=data_date)) == 0:
		print('not trade day')
		logout()
		os._exit(0)

#关闭聚宽API连接
def close_connect_to_api():
	print(logout()) 

#从聚宽API获取数据
def get_data_from_api():
	data = None

	try:
		#先根据标的代码查询对应的合约代码列表
		code_array = opt.run_query(query(opt.OPT_CONTRACT_INFO).filter(opt.OPT_CONTRACT_INFO.underlying_symbol==underlying_symbol,opt.OPT_CONTRACT_INFO.contract_status=='LIST'))['code']
		#遍历合约代码列表，从API中获取对应数据
		for i in range(0, len(code_array)):
			records = opt.run_query(query(opt.OPT_DAILY_PRICE).filter(opt.OPT_DAILY_PRICE.code==code_array[i],opt.OPT_DAILY_PRICE.date==current_date.strftime('%Y-%m-%d')))
			if i == 0:
				data = records
			else:
				data =data.append(records, ignore_index=True)

	except Exception as e:
		print('get data failed from API')
	print(len(data))
	return data

#将API返回的数据写入数据库
def write_data_to_db(data_array):
	engine=create_engine(DATABASE_CONNECT_STR)
	#engine=create_engine('mysql+pymysql://jackwang:jack@2019@rm-wz9gc93fekg4d0fed.mysql.rds.aliyuncs.com/options_data?charset=utf8')
	
	try:
		data_array.to_sql(TABLE_NAME,engine,schema=DATABASE_SCHEMA,if_exists='append',index=False,index_label=False)
	except Exception as e:
		print('data insert into db occurs exception')

#主函数入口
if __name__ == '__main__':
	#获取外部传入数据
	current_file_name = sys.argv[0]
	underlying_symbol = sys.argv[1]
	current_date = datetime.datetime.today()
	#如果外部传入日期则使用外部日期，否则使用当天日期
	if len(sys.argv) == 3:
		current_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d')

	start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

	#获取聚宽数据API接口
	get_connect_from_api()
	#判断是否为交易日
	is_trade_day(current_date)
	#从数据API中拿到目标数据
	data_array = get_data_from_api()
	#关闭数据API接口
	close_connect_to_api()
	#将数据写入到数据库中
	write_data_to_db(data_array)