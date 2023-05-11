from fastapi import FastAPI
import pandas as pd
from app.config.db import dbpool

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/indicator/calc")
async def indicator_calc(name: str, symbol: str):
    df = pd.read_sql(
        "select * from cn_stock_data where symbol = '600478' order by date asc",
        dbpool.getconn(),
    )
    print(df)
