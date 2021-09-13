""" 
量化学习：聚宽jqdatasdk对接vnpy的数据服务
https://zhuanlan.zhihu.com/p/54371682

"""

#----------------------------------------------------------------------
# 当日数据下载，定时任务使用
def downloadAllMinuteBar():
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print(u'开始下载合约分钟线数据')
    print('-' * 50)

    today = datetime.today().date()

    trade_date_list = jqdatasdk.get_trade_days(end_date=today, count=2)

    symbols_df = jqdatasdk.get_all_securities(types=['futures'], date=today)
    
    for index, row in symbols_df.iterrows():
        downMinuteBarBySymbol(index, row, str(today), str(trade_date_list[-2]))

    print('-' * 50)
    print(u'合约分钟线数据下载完成')
    print('-' * 50)
    return


#----------------------------------------------------------------------
# 按日期一次性补全数据
def downloadMinuteBarByDate(start_date, end_date=datetime.today().date()):
    jqdatasdk.auth(JQDATA_USER, JQDATA_PASSWORD)
    """下载所有配置中的合约的分钟线数据"""
    print('-' * 50)
    print(u'开始下载合约分钟线数据')
    print('-' * 50)

    trade_date_list = jqdatasdk.get_trade_days(start_date=start_date, end_date=end_date)

    i = 0
    for trade_date in trade_date_list:
        if i == 0:
            i = 1
            continue

        symbols_df = jqdatasdk.get_all_securities(types=['futures'], date=trade_date)

        for index, row in symbols_df.iterrows():
            downMinuteBarBySymbol(index, row, str(trade_date_list[i]), str(trade_date_list[i-1]))

        i += 1

    print('-' * 50)
    print(u'合约分钟线数据下载完成')
    print('-' * 50)
    return




# 具体合约当日的数据下载函数与vnpy的Bar类型数据的生成插入数据库的过程：

#----------------------------------------------------------------------
def generateVtBar(symbol, time, d):
    """生成K线"""
    bar = VtBarData()
    bar.vtSymbol = symbol
    bar.symbol = symbol
    bar.open = float(d['open'])
    bar.high = float(d['high'])
    bar.low = float(d['low'])
    bar.close = float(d['close'])
    bar.date = datetime.strptime(time[0:10], '%Y-%m-%d').strftime('%Y%m%d')
    bar.time = time[11:]
    bar.datetime = datetime.strptime(bar.date + ' ' + bar.time, '%Y%m%d %H:%M:%S')
    bar.volume = d['volume']
    
    return bar

#----------------------------------------------------------------------
def downMinuteBarBySymbol(symbol, info, today, pre_trade_day):
    start = time()

    symbol_name = info['name']
    cl = db[symbol_name]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引

    # 在此时间段内可以获取期货夜盘数据
    minute_df = jqdatasdk.get_price(symbol, start_date=pre_trade_day + " 20:30:00",end_date=today + " 20:30:00", frequency='minute')

    # 将数据传入到数据队列当中
    for index, row in minute_df.iterrows():
        bar = generateVtBar(symbol_name, str(index), row)
        d = bar.__dict__
        flt = {'datetime': bar.datetime}
        cl.replace_one(flt, d, True)

    e = time()
    cost = (e - start) * 1000

    print(u'合约%s数据下载完成%s - %s，耗时%s毫秒' % (symbol_name, pre_trade_day, today,  cost))








