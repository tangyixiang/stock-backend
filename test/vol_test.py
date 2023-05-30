# pd.set_option('display.max_rows', None)   # 显示所有行
# pd.set_option('display.max_columns', None)   # 显示所有列
# pd.set_option('display.width', None)   # 显示内容不折行

import pandas as pd
from app.config.db import *

df = pd.read_sql("select * from (select * from cn_stock_data where symbol = '300587' order by date desc limit 300) t order by date asc", engine.connect())

df['trade_vol_pct'] = df['trade_vol'].pct_change()
# df['trade_vol_pct_change'] = df['trade_vol_pct'] / df['trade_vol_pct'].shift(1)
# df_15_pct_diff = df[df['trade_vol_pct_change'].abs() > 0.15]
df = df[df['trade_vol_pct'] > 0.15]

down_per = df[(df['diff_per'] < 0)]

up_per = df[(df['diff_per'] > 3.7)]

print("===========================")
print(down_per)
print("===========================")
print(up_per)