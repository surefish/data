import os 
import csv
from datetime import datetime, time

from vnpy.trader.constant import Exchange
from vnpy.trader.database import database_manager
from vnpy.trader.object import TickData


""" 
策略应用 数据相关 载入Tick数据（csv格式）到数据库中

https://www.vnpy.com/forum/topic/1421-zai-ru-tickshu-ju-csvge-shi-dao-shu-ju-ku-zhong

"""


def run_load_csv():
    """
    遍历同一文件夹内所有csv文件，并且载入到数据库中
    """
    for file in os.listdir("."): 
        if not file.endswith(".csv"): 
            continue

        print("载入文件：", file)
        csv_load(file)

""" 

自己找到问题点了， 需要把第三方库peewee.py文件里的 DATETIME='DATETIME', 修改位 DATETIME='DATETIME(3)', 如下：
FIELD = attrdict(
AUTO='INTEGER',
BIGAUTO='BIGINT',
BIGINT='BIGINT',
BLOB='BLOB',
BOOL='SMALLINT',
CHAR='CHAR',
DATE='DATE',
DATETIME='DATETIME(3)',
DECIMAL='DECIMAL',
DEFAULT='',
DOUBLE='REAL',
FLOAT='REAL',
INT='INTEGER',
SMALLINT='SMALLINT',
TEXT='TEXT',
TIME='TIME',
UUID='TEXT',
UUIDB='BLOB',
VARCHAR='VARCHAR')

"""
def csv_load(file):
    """
    读取csv文件内容，并写入到数据库中    
    """
    with open(file, "r") as f:
        reader = csv.DictReader(f)

        ticks = []
        start = None
        count = 0

        for item in reader:

            # generate datetime
            date = item["交易日"]
            second = item["最后修改时间"]
            millisecond = item["最后修改毫秒"]

            standard_time = date + " " + second + "." + millisecond
            dt = datetime.strptime(standard_time, "%Y%m%d %H:%M:%S.%f")

            # filter
            if dt.time() > time(15, 1) and dt.time() < time(20, 59):
                continue

            tick = TickData(
                symbol="RU88",
                datetime=dt,
                exchange=Exchange.SHFE,
                last_price=float(item["最新价"]),
                volume=float(item["数量"]),
                bid_price_1=float(item["申买价一"]),
                bid_volume_1=float(item["申买量一"]),
                ask_price_1=float(item["申卖价一"]),
                ask_volume_1=float(item["申卖量一"]), 
                gateway_name="DB",       
            )
            ticks.append(tick)

            # do some statistics
            count += 1
            if not start:
                start = tick.datetime

        end = tick.datetime
        database_manager.save_tick_data(ticks)

        print("插入数据", start, "-", end, "总数量：", count)      


if __name__ == "__main__":
    run_load_csv()