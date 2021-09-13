from tqsdk import TqApi,TqSim,TqBacktest,BacktestFinished,TqAuth
import time
from datetime import date
#acc = TqSim();

api=TqApi(auth=TqAuth("18858280757", "xiuli1"));


hangq=api.get_kline_serial("SHFE.rb2201",60,10);

print(hangq);

api.close();