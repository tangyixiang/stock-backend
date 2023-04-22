from fastapi import APIRouter
import pandas as pd
import db.mysql as db
import json
import pandas_ta as ta

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


@router.get("/create/analysis/data")
async def create_analysis_data():
    new_placeholder = "%s" + ",%s" * 63
    insert_sql = "INSERT INTO stock_analysis values( " + new_placeholder + ")"

    data_sql = "select * from cn_stock_data where symbol = '{0}'"
    query_sql = "select t.symbol from (select symbol from cn_stock_data group by symbol) t where t.symbol not in (SELECT symbol FROM stock_analysis GROUP BY symbol ) "
    result = db.query(query_sql)
    for data in result:
        try:
            sql = data_sql.format(data[0])
            df = pd.read_sql(sql, db.pool.connection())
            df2 = ta.cdl_pattern(df["open"], df["high"], df["low"], df["close"], name="all")
            df = df.loc[:, ["date", "symbol"]]
            df2["date"] = df["date"]
            df_analysis = pd.merge(df, df2, on="date")
            df_analysis = df_analysis.astype("str")
            db.batchInsert(insert_sql, df_analysis.values.tolist())
            print("插入完成")
        except Exception as e:
            print('异常',data[0])     
