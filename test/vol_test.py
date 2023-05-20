import pandas as pd
# from app.config.db import dbpool

# df = pd.read_sql("select symbol from cn_stock_info",dbpool.getconn())
# symbol_list = list(df['symbol'])

# for symbol in symbol_list:
#     pd.read_sql(f"select * from cn_stock_data where ")

import akshare as ak

df = ak.stock_rank_ljqs_ths()

# 修改 Pandas 显示设置
pd.set_option('display.max_rows', None)   # 显示所有行
pd.set_option('display.max_columns', None)   # 显示所有列
pd.set_option('display.width', None)   # 显示内容不折行

df.to_excel("vol.xlsx",index=False)