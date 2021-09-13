""" 
论坛 策略应用 数据相关 全市场期货数据的批量$和更新
https://www.vnpy.com/forum/topic/4578-quan-shi-chang-qi-huo-shu-ju-de-pi-liang-he-geng-xin

1 key是交易所，value是列表，里面包含各种期货品种，这样，只要在遍历一下这个字典，就可以得到所有，如RB99.SHFE这样结构的字符串 
"""

symbols = {
    "SHFE": ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB", "WR", "HC", "SS", "BU", "RU", "NR", "SP", "SC", "LU", "FU"],
    "DCE": ["C", "CS", "A", "B", "M", "Y", "P", "FB","BB", "JD", "RR", "L", "V", "PP", "J", "JM", "I", "EG", "EB", "PG"],
    "CZCE": ["SR", "CF", "CY", "PM","WH", "RI", "LR", "AP","JR","OI", "RS", "RM", "TA", "MA", "FG", "SF", "ZC", "SM", "UR", "SA", "CL"],
    "CFFEX": ["IH","IC","IF", "TF","T", "TS"]
}
​
symbol_type = "99"

""" 
增加了生猪-LH，短纤-PF
czce的CL应为为CJ红枣
同时按字母顺序排列了下，方便以后增删查找
"""
symbols={
"SHFE": ["AG","AL","AU","BU","CU","FU","HC","LU","NI","NR","PB","RB","RU","SC","SN","SP","SS","WR","ZN"],
"DCE": ["A","B","BB","C","CS","EG","EB","FB","I","J","JD","JM","L","LH","M","P","PG","PP","RR","V","Y"],
"CZCE": ["AP","CF","CJ","CY","FG","JR","LR","MA","OI","PF","PM","RI","RM","RS","SA","SF","SM","SR","TA","UR","WH","ZC"],
"CFFEX": ["IC","IF","IH","T","TF","TS"]
}



""" 
2 只需要设置下载的开始和结束时间即可，需要注意的是，vnpy数据下载模块的入参是datetime.datetime格式 
"""
from datetime import datetime
start_date = datetime(2005,1,1)
end_date = datetime(2020,9,10)


""" 
3 批量下载数据，并不难，其运作步骤如下：

遍历symbols字典，
生成不同的HistoryRequest，
调用数据下载模块rqdata_client.query_history，得到数据data
调用数据保存模块database_manager.save_bar_data，把下载好的数据data写入数据库 
"""
from vnpy.trader.rqdata import rqdata_client
from vnpy.trader.database import database_manager
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
​
def load_data(req):
 data = rqdata_client.query_history(req)
 database_manager.save_bar_data(data)
 print(f"{req.symbol}历史数据下载完成")
​
for exchange, symbols_list in symbols.items():
 for s in symbols_list:
     req = HistoryRequest(
     symbol=s+symbol_type,
     exchange=Exchange(exchange),
     start=start_date,
     interval=Interval.DAILY,
     end=end_date,
     )
     load_data(req=req)



""" 
1 设置定时器

我们希望在收盘后，某个时间点如下午5点启动脚本，来自动下载数据。这本质上是包含了一个父进程和一个子进程。
 
父进程可以是一个永远在运行的python程序，如while循环，然后设置触发条件，如当时间刚好到下午5点就启动子进程下载更新数据，其他时间则睡觉等待。
"""
from datetime import datetime, time
from time import sleep
​
current_time = datetime.now().time()
start_time = time(17,0)
​
while True:
  sleep(10)

  if current_time == start_time:
    download_data()


####2 获取数据库数据
 
# 1）调用database_manager.get_bar_data_statistics来得到字典格式的数据数据库所有信息

    data = database_manager.get_bar_data_statistics()

# 2）获取各品种最新数据的时间信息，并且插入到data字典中
    for d in data:
        newest_bar = database_manager.get_newest_bar_data(
            d["symbol"], Exchange(d["exchange"]), Interval(d["interval"])
        )
        d["end"] = newest_bar.datetime

# 3）基于交易所和K线周期筛选品种，得到新的字典symbols，其中key包含合约代码，交易所，value就是数据库的结束时间
    symbols = {}
    for i in data:
        if i["interval"] == "d" and  i["exchange"] in Exchanges:
            vt_symbol = f"{i['symbol']}.{i['exchange']}"
            end = i["end"].date()
            symbols[vt_symbol] = end

# 4）设置下载结束时间为当天，基于symbols字典的信息，遍历组合得到HistoryRequest，然后再调用上面定义好的load_data函数下载数据并写入数据库中
    end_date = datetime.now().date()
    ​
    for vt_symbol, start_date in symbols.items():
        symbol = vt_symbol.split(".")[0]
        exchange = vt_symbol.split(".")[1]
        req = HistoryRequest(
        symbol=symbol,
        exchange=Exchange(exchange),
        start=start_date,
        interval=Interval.DAILY,
        end=end_date,
        )
        load_data(req=req)


######完整代码

    from vnpy.trader.rqdata import rqdata_client
    from vnpy.trader.database import database_manager
    from vnpy.trader.constant import Exchange, Interval
    from vnpy.trader.object import HistoryRequest
    from datetime import datetime

    start_date = datetime(2018,1,1)
    end_date = datetime(2021,1,19)

    rqdata_client.init('18650816356','long5204559')
    print("init结束")

    symbols={
    "SHFE": ["AG","AL","AU","BU","CU","FU","HC","LU","NI","NR","PB","RB","RU","SC","SN","SP","SS","WR","ZN"],
    "DCE": ["A","B","BB","C","CS","EG","EB","FB","I","J","JD","JM","L","LH","M","P","PG","PP","RR","V","Y"],
    "CZCE": ["AP","CF","CJ","CY","FG","JR","LR","MA","OI","PF","PM","RI","RM","RS","SA","SF","SM","SR","TA","UR","WH","ZC"],
    "CFFEX": ["IC","IF","IH","T","TF","TS"]
    }
    symbol_type = "99"
    print("symbols字典创建成功")

    def load_data(req):

    #3、调用数据下载模块rqdata_client.query_history，得到数据data。
    data = rqdata_client.query_history(req)
    #4、调用数据保存模块database_manager.save_bar_data，把下载好的数据data写入数据库。
    database_manager.save_bar_data(data)
    print(f"{req.symbol}历史数据下载完成")

    for exchange, symbols_list in symbols.items():
    for s in symbols_list:
    req = HistoryRequest(
    symbol=s+symbol_type,
    exchange=Exchange(exchange),
    start=start_date,
    interval=Interval.MINUTE,
    end=end_date
    )
    load_data(req=req)

    print("运行结束!")




# VNPY升级到最新版之后，需要修改部分代码，代码思路同楼主


    from datetime import datetime

    from vnpy.trader.rqdata import RqdataClient
    from vnpy.trader.object import HistoryRequest
    from vnpy.trader.database import database_manager

    rqdataClient=RqdataClient()
    rqdataClient.init()

    updateInfo=[]
    barOverViews=database_manager.get_bar_overview()
    for barOverView in barOverViews:
        #print(f"{barOverView.symbol} start:{barOverView.start} end:{barOverView.end} count:{barOverView.count}")
        updateInfo.append((barOverView.symbol,barOverView.exchange,barOverView.end,barOverView.interval))


    endDt=datetime.now()

    for info in updateInfo:
        print(f'更新:{info[0]}，Interval:{info[3]}')
        req=HistoryRequest(
            symbol=info[0],
            exchange=info[1],
            start=info[2],
            end=endDt,
            interval=info[3],
        )
        rData=rqdataClient.query_history(req)
        database_manager.save_bar_data(rData)



