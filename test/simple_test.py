# import app.utils.MyTT as tt
# import pandas as pd
# from app.config.db import dbpool


# data = pd.read_sql("select * from cn_stock_data where symbol = '600478' order by date asc", dbpool.getconn())

# hight = data["high"].values
# low = data["low"].values
# open = data["open"].values
# close = data["close"].values
# vol = data["trade_vol"].values
# date = data["date"].values


# emv, maemv = tt.EMV(hight, low, vol)
# rsi = tt.RSI(close)
# DIF,DEA,macd = tt.MACD(close)

# s_date = pd.Series(date, name="date")
# s_emv = pd.Series(emv, name="EMV")
# s_maemv = pd.Series(maemv, name="MAEMV")
# s_rsi = pd.Series(rsi, name="RSI")
# s_macd = pd.Series(macd, name="MACD")

# df = pd.concat([s_date, s_rsi, s_emv, s_maemv, s_macd], axis=1)

# # 修改 Pandas 显示设置
# pd.set_option('display.max_rows', None)   # 显示所有行
# pd.set_option('display.max_columns', None)   # 显示所有列
# pd.set_option('display.width', None)   # 显示内容不折行

# print(df)

from sqlalchemy import select
from app.config.db import *
from app.model.CnStockModel import *

exist_symbol_set = set(select(CnStockInfo.symbol))

print(exist_symbol_set)
