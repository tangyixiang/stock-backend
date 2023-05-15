import json
import pandas as pd
import akshare as ak
from fastapi import APIRouter
from app.config.db import *
import app.task.DataScheduleTask as task

router = APIRouter(prefix="/cn")


@router.get("/symbol/list")
async def list_symbol(pageSize: int = 10, pageNo: int = 1):
    offset = (pageNo - 1) * pageSize
    total = pd.read_sql("select count(*) as total from cn_stock_info", dbpool.getconn()).iloc[0, 0]
    data = pd.read_sql(f"select * from cn_stock_info order by market_value desc offset {offset} limit {pageSize}", dbpool.getconn())
    return {"total": int(total), "data": json.loads(data.to_json(orient="records", force_ascii=False))}


@router.get("/indicator/calc")
async def indicator_calc(name: str, symbol: str):
    df = pd.read_sql(
        f"select * from cn_stock_data where symbol = '{symbol}' order by date asc",
        dbpool.getconn(),
    )
    print(df)


@router.get("/vol/up")
async def vol_up(date: str, pageSize: int = 20, pageNo: int = 1):
    offset = (pageNo - 1) * pageSize
    df = pd.read_sql(f"select * from cn_stock_vol_up where date = '{date}' offset {offset} limit {pageSize}", dbpool.getconn())
    # data = df.to_json(orient="records", force_ascii=False)
    symbol_list = df["symbol"].values.tolist()
    data_list = []
    for symbol in symbol_list:
        sql = f"select * from (select * from cn_stock_data where symbol = '{symbol}' order by date desc limit 90) t order by date asc"
        data = query_for_obj(sql)
        data_list.append({"symbol": symbol, "data": data})
    return {"total": len(df), "list": data_list}

