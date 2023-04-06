import datetime
import calendar
from fastapi import APIRouter
import db.mysql as db
import akshare as ak
import pandas as pd
import pandas_ta as ta
import json
from app.utils.cn_stock_utils import save_symbol_his_data

router = APIRouter(prefix="/cn")


@router.get("/all/symbol")
async def cn_all_symbol():
    query_sql = "select symbol from cn_stock_data group by symbol"
    result = db.query(query_sql)
    insert_sql = "insert into cn_stock_info(symbol) values (%s)"
    db.batchInsert(insert_sql, result)
    return {"message": "ok"}


@router.get("/history/data")
async def cn_history_data(
    start_date: str, end_date: str, period: str = "daily", code: str = None
):
    print("working")
    if code:
        save_symbol_his_data(code, start_date, end_date, period)
    else:
        df = ak.stock_zh_a_spot_em()
        symbol_list = df["代码"].tolist()
        for symbol in symbol_list:
            try:
                save_symbol_his_data(symbol, start_date, end_date, period)
            except Exception as e:
                print("未知错误：", e)


@router.get("/today/analysis")
async def cn_today_analysis(code: str = None):
    today = datetime.date.today()
    weekday = today.weekday()
    if weekday >= 5:  # 如果是周六或周日
        days_to_friday = (calendar.FRIDAY - weekday) % 7
        today = today - datetime.timedelta(days=days_to_friday)
    two_weeks_ago = today - datetime.timedelta(weeks=4)
    start_date = two_weeks_ago.strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    if code:
        sql = "SELECT * FROM cn_stock_data WHERE symbol = '{}'".format(code)
    else:
        sql = "SELECT * FROM cn_stock_data WHERE DATE BETWEEN '{0}' AND '{1}'".format(
            start_date, end_date
        )
    df = pd.read_sql(sql, db.pool.connection())
    df_ta = ta.cdl_pattern(
        df["open"],
        df["high"],
        df["low"],
        df["close"],
        [
            "darkcloudcover",
            "dojistar",
            "engulfing",
            "hangingman",
            "hammer",
            "belthold",
            "3starsinsouth",
        ],
    )
    df_ta["date"] = df["date"]
    df_ta = df_ta.loc[(df_ta.iloc[:, :-1] != 0.0).any(axis=1)]
    cols = list(df_ta.columns)
    cols.remove("date")
    df_ta = df_ta[["date"] + cols]
    new_columns = ["date"] + [
        col.replace("CDL_", "").lower() for col in df_ta.columns if col != "date"
    ]
    df_ta.columns = new_columns
    merged_df = pd.merge(df, df_ta, on="date", how="left")
    return {
        "length": len(merged_df),
        "data": json.loads(merged_df.to_json(orient="records")),
    }


@router.get("/data")
async def cn_symbol_data_daily(symbol: str, start_date: str, end_date: str):
    sql = "select * from cn_stock_data where symbol = '{0}' and date BETWEEN '{1}' AND '{2}'".format(
        symbol, start_date, end_date
    )
    df = pd.read_sql_query(sql, db.pool.connection())
    return json.loads(df.to_json(orient="records"))
