from fastapi import APIRouter
import pandas as pd
import db.mysql as db
import json

router = APIRouter(prefix="/open")


def convert_tojson(df: pd.DataFrame):
    """
    df转换为json
    """
    return json.loads(df.to_json(orient="records"))


@router.get("/all/symbol/detail")
async def all_symbol_detail():
    sql = "select * from cn_stock_info where market_value <> '0' order by market_value desc"
    df = pd.read_sql(sql, db.pool.connection())
    return convert_tojson(df)


@router.get("/kline")
async def kline_symbol(symbol: str):
    sql = "select * from cn_stock_data where symbol = '{0}' order by date limit 180".format(
        symbol
    )
    df = pd.read_sql(sql, db.pool.connection())
    return convert_tojson(df)











