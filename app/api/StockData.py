import pandas as pd
from fastapi import APIRouter, Depends
from app.config.db import *
from app.model.CnStockModel import *
from sqlalchemy.orm import Session
from operator import attrgetter

router = APIRouter(prefix="/cn")


@router.get("/symbol/list")
async def list_symbol(pageSize: int = 10, pageNo: int = 1, db: Session = Depends(getSesion)):
    offset = (pageNo - 1) * pageSize
    total = db.query(CnStockInfo).count()
    data = db.query(CnStockInfo).order_by(CnStockInfo.market_value.desc()).offset(offset).limit(pageSize).all()
    return {"total": total, "list": data}


# @router.get("/indicator/calc")
# async def indicator_calc(name: str, symbol: str):
#     df = pd.read_sql(
#         f"select * from cn_stock_data where symbol = '{symbol}' order by date asc",
#         dbpool.getconn(),
#     )
#     print(df)


@router.get("/vol/category")
def vol_category(date: str, db: Session = Depends(getSesion)):
    data = db.query(CnStockVolUp).where(CnStockVolUp.date == date).all()
    return data


@router.get("/vol/up")
async def vol_up(date: str, industry: str, db: Session = Depends(getSesion)):
    df = pd.read_sql(f"select a.*, b.market_value from cn_stock_vol_up a left join cn_stock_info b on a.symbol = b.symbol where a.date = '{date}' and a.industry = '{industry}' order by b.market_value asc ", engine.connect())
    # data = df.to_json(orient="records", force_ascii=False)
    symbol_list = df["symbol"].values.tolist()
    data_list = []
    for symbol in symbol_list:
        data = db.query(CnStockData).where(CnStockData.symbol == symbol).order_by(CnStockData.date.desc()).limit(90).all()
        data.sort(key=attrgetter('date'))
        row_data = list(df.loc[df["symbol"] == symbol].squeeze())
        name = row_data[2]
        market_value = row_data[len(row_data) - 1]
        data_list.append({"symbol": symbol, "name": name, "market_value": market_value, "data": data})
    return {"total": len(df), "list": data_list}
