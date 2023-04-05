import akshare as ak
import db.mysql as db


def save_symbol_his_data(symbol: str, start_date: str, end_date: str, period: str):
    sql = """ 
      replace into cn_stock_data (symbol,date,open,close,high,low,trade_vol,trade_quota,amplitude,diff_per,diff_quota,exchange_rate) \
      VALUES (%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s); 
    """
    df = ak.stock_zh_a_hist(symbol, period, start_date, end_date)
    # 获取行数
    data_list = []
    for i in range(len(df)):
        row_data = list(tuple(df.iloc[i, :]))
        row_data.insert(0, symbol)
        data_list.append(row_data)
    # 分割长度
    chunk_size = 1000
    divided_list = [
        data_list[i : i + chunk_size] for i in range(0, len(data_list), chunk_size)
    ]
    for chunk in divided_list:
        print(chunk)
        db.batchInsert(sql, chunk)
