import akshare as ak

today_table = ak.stock_zh_a_spot_em()

print(today_table[today_table['总市值'].notna()])