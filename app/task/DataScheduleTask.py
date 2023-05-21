import akshare as ak
from rocketry import Rocketry
from rocketry.conds import cron
from app.config.log import logger as log
from app.config.db import *
from datetime import datetime
from sqlalchemy import text, func, select
from app.model.CnStockModel import *

import pandas as pd

app = Rocketry()


# 同步symbol
@app.task(cron("10 15 * * 1-5"))
def all_symbol():
    log.info("开始同步symbol")
    db = getSesion()
    today_table = ak.stock_zh_a_spot_em()
    # 过滤掉退市的
    today_table = today_table[today_table["总市值"].notna()]
    table = today_table[["代码", "名称", "总市值"]]
    # 获取所有股票代码列表、已存在的代码列表和新的代码列表
    all_symbol_set = set(table["代码"])
    exist_symbol_set = set(db.query(CnStockInfo.symbol).all())
    new_symbol_set = all_symbol_set - exist_symbol_set

    if len(new_symbol_set) == 0:
        return

    # 获取新代码对应的股票信息
    new_symbols_df = pd.DataFrame(data=new_symbol_set, columns=["代码"])
    table = pd.merge(table, new_symbols_df, on="代码", how="right")
    table_row = table.values.tolist()
    sql = text("insert into cn_stock_info(symbol,name,market_value) values(?,?,?)")
    batch_insert(sql, table_row)

    # 更新新代码对应的经营范围
    for symbol in new_symbol_set:
        try:
            table2 = ak.stock_zyjs_ths(symbol)
            desc = table2["经营范围"][0]
            sql2 = text(f"update cn_stock_info set description = '{desc}' where symbol = '{symbol}'")
            with engine.connect() as conn:
                conn.execute(sql2)
        except Exception as e:
            log.error("异常,symbol:{}", symbol)
    log.info("同步symbol 结束")


@app.task(cron("30 15 * * 1-5"))
def open_day_data():
    today = datetime.now().strftime("%Y%m%d")
    log.info("开始同步数据,日期:{}", today)
    symbol_list = list(pd.read_sql("select symbol from cn_stock_info", engine.connect())["symbol"])
    for symbol in symbol_list:
        log.info("保存数据:{}", symbol)
        data = ak.stock_zh_a_hist(symbol=symbol, start_date=today, end_date=today)
        data.insert(loc=0, column="symbol", value=symbol)
        sql = text("insert into cn_stock_data(symbol,date,open,close,high,low,trade_vol,trade_quota,amplitude,diff_per,diff_quota,exchange_rate) values (?,?,?,?,?,?,?,?,?,?,?,?)")
        batch_insert(sql, data.values.tolist())
        log.info("同步完成:{}", symbol)


@app.task(cron("40 15 * * 1-5"))
def vol_up():
    today = datetime.now().strftime("%Y-%m-%d")
    CnStockData()
    stmt = func.max("date").select().where("cn_stock_data")
    with engine.connect() as conn:
        result = conn.execute(stmt)
        db_max_date = result.scalar().strftime("%Y-%m-%d")
    if today != db_max_date:
        log.info(f"{today} 不是open day")
        return
    log.info("同步量价齐升")
    df = ak.stock_rank_ljqs_ths()
    table = df.loc[:, ["股票代码", "股票简称", "量价齐升天数", "阶段涨幅", "所属行业"]]
    table.insert(0, column="date", value=today)

    table_row = table.values.tolist()
    sql = text("insert into cn_stock_vol_up(date,symbol,name,up_days,up_quota,industry) values (?,?,?,?,?,?)")
    batch_insert(sql, table_row)
    log.info("同步完成")
