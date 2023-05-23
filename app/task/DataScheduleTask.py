import akshare as ak
from rocketry import Rocketry
from rocketry.conds import cron
from app.config.log import logger as log
from app.config.db import *
from datetime import datetime
from sqlalchemy import text, func, update
from app.model.CnStockModel import *

import pandas as pd

app = Rocketry()


# 同步symbol
@app.task(cron("10 15 * * 1-5"))
def all_symbol():
    log.info("开始同步symbol")
    db = singleSession()
    today_table = ak.stock_zh_a_spot_em()
    # 过滤掉退市的
    today_table = today_table[today_table["总市值"].notna()]
    table = today_table[["代码", "名称", "总市值"]]
    # 更新市值
    for index, row in table.iterrows():
        code = row["代码"]
        market_value = row["总市值"]
        with engine.begin() as conn:
            stmt = update(CnStockInfo).where(CnStockInfo.symbol == code).values(market_value=market_value)
            conn.execute(stmt)

    # 获取所有股票代码列表、已存在的代码列表和新的代码列表
    all_symbol_set = set(table["代码"])
    db_data = db.query(CnStockInfo.symbol).all()
    exist_symbol_set = set([row[0] for row in db_data])
    new_symbol_set = all_symbol_set - exist_symbol_set

    if len(new_symbol_set) == 0:
        return

    # 获取新代码对应的股票信息
    new_symbols_df = pd.DataFrame(data=new_symbol_set, columns=["代码"])
    table = pd.merge(table, new_symbols_df, on="代码", how="right")
    table_rows = table.values.tolist()
    rows = []
    for row in table_rows:
        rows.append(
            {
                "symbol": row[0],
                "name": row[1],
                "market_value": row[2],
            }
        )

    sql = text("insert into cn_stock_info(symbol,name,market_value) values(:symbol,:name,:market_value)")
    batch_insert(sql, rows)

    # 更新新代码对应的经营范围
    for symbol in new_symbol_set:
        try:
            table2 = ak.stock_zyjs_ths(symbol)
            desc = table2["经营范围"][0]
            sql2 = text(f"update cn_stock_info set description = '{desc}' where symbol = '{symbol}'")
            with engine.connect() as conn:
                conn.execute(sql2)
                conn.commit()
        except Exception as e:
            log.error("异常,symbol:{}", symbol)
    log.info("同步symbol 结束")


@app.task(cron("30 15 * * 1-5"))
def open_day_data():
    today = datetime.now().strftime("%Y%m%d")
    log.info("开始同步数据,日期:{}", today)
    symbol_list = list(pd.read_sql("select symbol from cn_stock_info", engine.connect())["symbol"])
    rows = []
    for symbol in symbol_list:
        log.info("保存数据:{}", symbol)
        data = ak.stock_zh_a_hist(symbol=symbol, start_date=today, end_date=today)
        data.insert(loc=0, column="symbol", value=symbol)
        if len(data) > 0:
            for row in data.values.tolist():
                rows.append({"symbol": row[0], "date": row[1], "open": row[2], "close": row[3], "high": row[4], "low": row[5], "trade_vol": row[6], "trade_quota": row[7], "amplitude": row[8], "diff_per": row[9], "diff_quota": row[10], "exchange_rate": row[11]})
    sql = text("insert into cn_stock_data(symbol,date,open,close,high,low,trade_vol,trade_quota,amplitude,diff_per,diff_quota,exchange_rate) values(:symbol,:date,:open,:close,:high,:low,:trade_vol,:trade_quota,:amplitude,:diff_per,:diff_quota,:exchange_rate)")
    batch_insert(sql, rows)
    log.info("同步完成:{}", symbol)


@app.task(cron("40 15 * * 1-5"))
def vol_up():
    today = datetime.now().strftime("%Y-%m-%d")
    db = singleSession()
    db_max_date = db.query(func.max(CnStockData.date)).scalar().strftime("%Y-%m-%d")
    if today != db_max_date:
        log.info(f"{today} 不是open day")
        return
    log.info("同步量价齐升")
    df = ak.stock_rank_ljqs_ths()
    table = df.loc[:, ["股票代码", "股票简称", "量价齐升天数", "阶段涨幅", "所属行业"]]
    table.insert(0, column="date", value=today)
    rows = []
    for row in table.values.tolist():
        rows.append({"date": row[0], "symbol": row[1], "name": row[2], "up_days": row[3], "up_quota": row[4], "industry": row[5]})
    sql = text("insert into cn_stock_vol_up(date,symbol,name,up_days,up_quota,industry) values (:date,:symbol,:name,:up_days,:up_quota,:industry)")
    batch_insert(sql, rows)
    log.info("同步完成")
