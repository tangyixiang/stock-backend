from app.config.db import *
import pandas as pd
from app.model.CnStockModel import *
from app.config.log import logger as log


db = singleSession()
all_symbol = db.query(CnStockInfo).all()


def filter_data(df: pd.DataFrame):
    df["trade_vol_pct"] = df["trade_vol"].pct_change()
    df = df[df["trade_vol_pct"] > 0.15]
    down_per = df[(df["diff_per"] < 0)].loc[:, ["date", "symbol", "diff_per"]].assign(vol_type=1)
    up_per = df[(df["diff_per"] > 3.7)].loc[:, ["date", "symbol", "diff_per"]].assign(vol_type=2)

    return down_per, up_per


for item in all_symbol:
    log.info(f"{item.symbol}开始运行")
    sql = f"select * from cn_stock_data where symbol = '{item.symbol}' order by date asc"
    df = pd.read_sql(sql, engine.connect())
    down_per, up_per = filter_data(df)
    down_per.to_sql(name="cn_stock_vol_analysis", con=engine.connect(), index=False, if_exists="append")
    up_per.to_sql(name="cn_stock_vol_analysis", con=engine.connect(), index=False, if_exists="append")
    log.info(f"{item.symbol}结束")
