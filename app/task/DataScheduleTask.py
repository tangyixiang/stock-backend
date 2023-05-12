import akshare as ak
from rocketry import Rocketry
from rocketry.conds import cron
from app.config.log import logger as log
from app.config.db import *
from datetime import datetime

import pandas as pd

app = Rocketry()


# 同步symbol
@app.task(cron("10 15 * * 1-5"))
def all_symbol():
    log.info("开始同步symbol")
    today_table = ak.stock_zh_a_spot_em()
    table = today_table[["代码", "名称", "总市值"]]

    # 获取所有股票代码列表、已存在的代码列表和新的代码列表
    all_symbol_list = list(table["代码"])
    exist_symbol_list = list(
        pd.read_sql("select symbol from cn_stock_info", dbpool.getconn())["symbol"]
    )
    new_symbol_list = [
        symbol for symbol in all_symbol_list if symbol not in exist_symbol_list
    ]

    if len(new_symbol_list) == 0:
        return

    # 获取新代码对应的股票信息
    new_symbols_df = pd.DataFrame(data=new_symbol_list, columns=["代码"])
    table = pd.merge(table, new_symbols_df, on="代码", how="right")
    table_row = table.values.tolist()
    sql = "insert into cn_stock_info(symbol,name,market_value) values (%s,%s,%s)"
    batchInsert(sql, table_row)

    # 更新新代码对应的经营范围
    for symbol in exist_symbol_list:
        try:
            table2 = ak.stock_zyjs_ths(symbol)
            desc = table2["经营范围"][0]
            execute(
                f"update cn_stock_info set description = '{desc}' where symbol = '{symbol}'"
            )
        except Exception as e:
            log.error(str(e))
            log.error("异常,symbol:{}", symbol)

    log.info("同步symbol 结束")


@app.task(cron("30 15 * * 1-5"))
def open_day_data():
    today = datetime.now().strftime("%Y%m%d")
    log.info("开始同步数据,日期:{}", today)
    symbol_list = list(
        pd.read_sql("select symbol from cn_stock_info", dbpool.getconn())["symbol"]
    )
    for symbol in symbol_list:
        log.info("保存数据:{}", symbol)
        data = ak.stock_zh_a_hist(symbol=symbol, start_date=today, end_date=today)
        data.insert(loc=0, column="symbol", value=symbol)
        batchInsert(
            "insert into cn_stock_data(symbol,date,open,close,high,low,trade_vol,trade_quota,amplitude,diff_per,diff_quota,exchange_rate) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            data.values.tolist(),
        )
        log.info("同步完成:{}", symbol)

