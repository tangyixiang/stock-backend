import pandas as pd
from fastapi import APIRouter
from app.config.db import dbpool

router = APIRouter(prefix="/cn")


@router.get("/indicator/calc")
async def indicator_calc(name: str, symbol: str):
    df = pd.read_sql(
        f"select * from cn_stock_data where symbol = '{symbol}' order by date asc",
        dbpool.getconn(),
    )
    print(df)
