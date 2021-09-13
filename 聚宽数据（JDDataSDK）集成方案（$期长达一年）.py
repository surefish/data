'''
https://www.vnpy.com/forum/topic/1472-ju-kuan-shu-ju-jddatasdk-ji-cheng-fang-an-qi-chang-da-yi-nian

聚宽数据（JDDataSDK）集成方案（$期长达一年）.py
'''
# 第二步：抽象了行情数据接口，所有文件集中在 trader/mddata 文件夹中：
# dataapi.py文件是抽象类：

from abc import ABC, abstractmethod

from vnpy.trader.object import HistoryRequest


class MdDataApi(ABC):
    """
    抽象数据接口
    """

    @abstractmethod
    def init(self, username="", password=""):
        """
        初始化行情数据接口
        :param username: 用户名
        :param password: 密码
        :return:
        """
        pass

    @abstractmethod
    def query_history(self, req: HistoryRequest):
        """
        查询历史数据接口
        :param req:
        :return:
        """
        pass



# jqdata.py是聚宽数据接口的具体实现

import jqdatasdk as jq

from datetime import timedelta, datetime
from typing import List

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.mddata.dataapi import MdDataApi
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.setting import SETTINGS

INTERVAL_VT2JQ = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP_JQ = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()  # no need to adjust for daily bar
}


class JqdataClient(MdDataApi):
    """聚宽JQData客户端封装类"""

    def __init__(self):
        """"""
        self.username = SETTINGS["jqdata.username"]
        self.password = SETTINGS["jqdata.password"]

        self.inited = False

    def init(self, username="", password=""):
        """"""
        if self.inited:
            return True

        if username and password:
            self.username = username
            self.password = password

        if not self.username or not self.password:
            return False

        try:
            jq.auth(self.username, self.password)
        except Exception as ex:
            print("jq auth fail:" + repr(ex))
            return False

        self.inited = True
        return True

    def to_jq_symbol(self, symbol: str, exchange: Exchange):
        """
        CZCE product of RQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        if exchange in [Exchange.SSE, Exchange.SZSE]:
            if exchange == Exchange.SSE:
                jq_symbol = f"{symbol}.XSHG"  # 上海证券交易所
            else:
                jq_symbol = f"{symbol}.XSHE"  # 深圳证券交易所
        elif exchange == Exchange.SHFE:
            jq_symbol = f"{symbol}.XSGE"  # 上期所
        elif exchange == Exchange.CFFEX:
            jq_symbol = f"{symbol}.CCFX"  # 中金所
        elif exchange == Exchange.DCE:
            jq_symbol = f"{symbol}.XDCE"  # 大商所
        elif exchange == Exchange.INE:
            jq_symbol = f"{symbol}.XINE"  # 上海国际能源期货交易所
        elif exchange == Exchange.CZCE:
            # 郑商所 的合约代码年份只有三位 需要特殊处理
            for count, word in enumerate(symbol):
                if word.isdigit():
                    break

            # Check for index symbol
            time_str = symbol[count:]
            if time_str in ["88", "888", "99", "8888"]:
                return symbol

            # noinspection PyUnboundLocalVariable
            product = symbol[:count]
            year = symbol[count]
            month = symbol[count + 1:]

            if year == "9":
                year = "1" + year
            else:
                year = "2" + year

            jq_symbol = f"{product}{year}{month}.XZCE"

        return jq_symbol.upper()

    def query_history(self, req: HistoryRequest):
        """
        Query history bar data from JQData.
        """
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        jq_symbol = self.to_jq_symbol(symbol, exchange)
        # if jq_symbol not in self.symbols:
        #     return None

        jq_interval = INTERVAL_VT2JQ.get(interval)
        if not jq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP_JQ.get(interval)

        # For querying night trading period data
        # end += timedelta(1)
        now = datetime.now()
        if end >= now:
            end = now
        elif end.year == now.year and end.month == now.month and end.day == now.day:
            end = now

        df = jq.get_price(
            jq_symbol,
            frequency=jq_interval,
            fields=["open", "high", "low", "close", "volume"],
            start_date=start,
            end_date=end,
            skip_paused=True
        )

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=row.name.to_pydatetime() - adjustment,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    gateway_name="JQ"
                )
                data.append(bar)

        return data


jqdata_client = JqdataClient()

""" 
rqdata.py文件是VNPY已经提供的米筐数据接口实现，这里就改了一句，增加对标准接口MdDataApi的继承


"""


class RqdataClient(MdDataApi):


# init.py文件会根据setting.py的配置文件加载具体的行情数据接口

from vnpy.trader.mddata.dataapi import MdDataApi
from vnpy.trader.mddata.jqdata import jqdata_client
from vnpy.trader.mddata.rqdata import rqdata_client
from vnpy.trader.setting import SETTINGS

if SETTINGS["mddata.api"] == "jqdata":
    mddata_client: MdDataApi = jqdata_client
else:
    mddata_client: MdDataApi = rqdata_client


""" 
第三步：使用mddata_client替换rqdata_client即可，例如cta_strategy/engine.py文件的改造如下：
注意：script_trader/engine.py，cta_backtester/engine.py，cta_strategy/engine.py 这三个文件都要改造

"""
from vnpy.trader.mddata import mddata_client

    def init_rqdata(self):
        """
        Init RQData client.
        """
        result = mddata_client.init()
        md_data_api = SETTINGS["mddata.api"]
        if result:
            self.write_log(f"{md_data_api}数据接口初始化成功")

    def query_bar_from_rq(
        self, symbol: str, exchange: Exchange, interval: Interval, start: datetime, end: datetime
    ):
        """
        Query bar data from RQData.
        """
        req = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            start=start,
            end=end
        )

        try:
            data = mddata_client.query_history(req)
        except Exception as ex:
            self.write_log(f"{symbol}.{exchange.value}合约下载失败：{ex.args}")
            return None

        return data












