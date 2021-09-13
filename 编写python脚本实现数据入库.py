
""" 
互帮互助 经验分享 编写python脚本实现数据入库

https://www.vnpy.com/forum/topic/3203-bian-xie-pythonjiao-ben-shi-xian-shu-ju-ru-ku?page=1#pid11498

"""

""" 
配置数据库

vn.py 中默认使用 SQLite 数据库。 因此，如果需要使用 MongoDB 数据库，则需要修改 vn.py 的全局配置。具体流程如下：
 
在C:\Users\你的用户名\.vntrader 目录下找到 vt_setting.json 文件

对vt_setting.json 文件中的database.driver,database.database,database.host,database.port 进行修改。

配置 MongoDB 数据库和设置好 MongoDB 分表读取
"""


""" 
vn.py 提供了很多工具使数据入库这个过程变得简单，快捷。下面是数据入库的基本流程：
 
1、先确定要入库的数据是 Tick 还是 Bar (K线) 类型数据

2、将需要入库的数据转化成 vn.py 定义的 TickData 或 BarData 数据类型

3、使用 vn.py 提供的数据入库工具函数 database_manager.save_tick_data 或 database_manager.save_bar_data 将相应的 TickData 或 BarData 入库

"""


""" 
如果数据的质量较差，比如数据的分隔符设置存在问题，会使得pd.read_csv函数没办法正确的读取.csv文件。这时则需要使用python的 csv 库。本文的数据入库过程统一使用 pandas 来完成

"""
from vnpy.trader.constant import (Exchange, Interval)
import pandas as pd
# 读取需要入库的csv文件，该文件是用gbk编码
imported_data = pd.read_csv('需要入库的数据的绝对路径',encoding='gbk')

# 将csv文件中 `市场代码`的 SC 替换成 Exchange.SHFE SHFE
imported_data['市场代码'] = Exchange.SHFE

# 增加一列数据 `inteval`，且该列数据的所有值都是 Interval.MINUTE
imported_data['interval'] = Interval.MINUTE


# 明确需要是float数据类型的列
float_columns = ['开', '高', '低', '收', '成交量', '持仓量']

for col in float_columns:
  imported_data[col] = imported_data[col].astype('float')


# 明确时间戳的格式
# %Y/%m/%d %H:%M:%S 代表着你的csv数据中的时间戳必须是 2020/05/01 08:32:30 格式
datetime_format = '%Y%m%d %H:%M:%S'

imported_data['时间'] = pd.to_datetime(imported_data['时间'],format=datetime_format)
 
 
 
# 因为没有用到 成交额 这一列的数据，所以该列列名不变
imported_data.columns = ['exchange','symbol','datetime','open','high','low','close','volume','成交额','open_interest','interval']
 
# 因为该csv文件储存的是ag的主力连续数据，即多张ag合约的拼接。因此，symbol列中有多个不同到期日的ag合约代码，这里需要将合约代码统一为ag88
imported_data['symbol'] ='ag88'



# 使用 vn.py 封装好的 database_manager.save_bar_data 将数据入库
# 导入 database_manager 模块
from vnpy.trader.database import database_manager
from vnpy.trader.object import (BarData,TickData)
# 封装函数
def move_df_to_mongodb(imported_data:pd.DataFrame,collection_name:str):
    bars = []
    start = None
    count = 0

    for row in imported_data.itertuples():

        bar = BarData(

              symbol=row.symbol,
              exchange=row.exchange,
              datetime=row.datetime,
              interval=row.interval,
              volume=row.volume,
              open_price=row.open,
              high_price=row.high,
              low_price=row.low,
              close_price=row.close,
              open_interest=row.open_interest,
              gateway_name="DB",

        )


        bars.append(bar)

        # do some statistics
        count += 1
        if not start:
            start = bar.datetime
    end = bar.datetime

    # insert into database
    database_manager.save_bar_data(bars, collection_name)
    print(f"Insert Bar: {count} from {start} - {end}")

# 如果，没有设置分表储存不同类型的数据。则需要先将move_df_to_mongodb函数中的collection_name参数删除，同时将上面代码的倒数第二行修改为
    database_manager.save_bar_data(bars)

# 1、创建一个sqlite数据库连接对象：
    from vnpy.trader.database.initialize import init_sql
    from vnpy.trader.database.database import Driver

    settings={

        "database": "database.db",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "",
        "authentication_source": "admin"
    }
    sqlite_manager = init_sql(driver=Driver.SQLITE, settings=settings)


#2、 替换函数 move_df_to_mongodb 的倒数第二行
    sqlite_manager.save_bar_data(bars)

""" 
Debug
 

使用上述代码进行Sqlite数据入库的时候，会出现peewee.InterfaceError: Error binding parameter 2 - probably unsupported type错误，解决方法:

找到imported_data['时间'] = pd.to_datetime(imported_data['时间'],format=datetime_format)代码所在行
在该行代码下键入imported_data['时间'] = imported_data['时间'].dt.strftime('%Y%m%d %H:%M:%S')

"""

# 完整代码
from vnpy.trader.constant import (Exchange, Interval)
import pandas as pd
from vnpy.trader.database import database_manager
from vnpy.trader.object import (BarData,TickData)

# 封装函数
def move_df_to_mongodb(imported_data:pd.DataFrame,collection_name:str):
    bars = []
    start = None
    count = 0

    for row in imported_data.itertuples():

        bar = BarData(

              symbol=row.symbol,
              exchange=row.exchange,
              datetime=row.datetime,
              interval=row.interval,
              volume=row.volume,
              open_price=row.open,
              high_price=row.high,
              low_price=row.low,
              close_price=row.close,
              open_interest=row.open_interest,
              gateway_name="DB",

        )


        bars.append(bar)

        # do some statistics
        count += 1
        if not start:
            start = bar.datetime
    end = bar.datetime

    # insert into database
    database_manager.save_bar_data(bars, collection_name)
    print(f'Insert Bar: {count} from {start} - {end}')


if __name__ == "__main__":

    # 读取需要入库的csv文件，该文件是用gbk编码
    imported_data = pd.read_csv('D:/1分钟数据压缩包/FutAC_Min1_Std_2016/ag主力连续.csv',encoding='gbk')
    # 将csv文件中 `市场代码`的 SC 替换成 Exchange.SHFE SHFE
    imported_data['市场代码'] = Exchange.SHFE
    # 增加一列数据 `inteval`，且该列数据的所有值都是 Interval.MINUTE
    imported_data['interval'] = Interval.MINUTE
    # 明确需要是float数据类型的列
    float_columns = ['开', '高', '低', '收', '成交量', '持仓量']
    for col in float_columns:
      imported_data[col] = imported_data[col].astype('float')
    # 明确时间戳的格式
    # %Y/%m/%d %H:%M:%S 代表着你的csv数据中的时间戳必须是 2020/05/01 08:32:30 格式
    datetime_format = '%Y%m%d %H:%M:%S'
    imported_data['时间'] = pd.to_datetime(imported_data['时间'],format=datetime_format)
    # 因为没有用到 成交额 这一列的数据，所以该列列名不变
    imported_data.columns = ['exchange','symbol','datetime','open','high','low','close','volume','成交额','open_interest','interval']
    imported_data['symbol'] ='ag88'
    move_df_to_mongodb(imported_data,'ag88')



""" 
运行会报错：

AttributeError: 'str' object has no attribute 'astimezone'

原因是新版本vnpy支持了时区数据，所以datetime=row.datetime需要加上时区信息

"""
from datetime import datetime, timedelta, timezone

# 中国时区是+8，对应参数hours=8
# 日本时区是+9，hours=9
utc_8 = timezone(timedelta(hours=8))
datetime=row.datetime.replace(tzinfo=utc_8)


# 一个小技巧，把bar里面的数据打印出来，看看都是什么格式。
sql_manager.get_oldest_bar_data()


# 用localize函数处理一下你的datetime吧





