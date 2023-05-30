import pandas as pd
from fastapi import APIRouter, Depends
from app.config.db import *
from app.model.CnStockModel import *
from sqlalchemy.orm import Session
from operator import attrgetter
from app.task.DataScheduleTask import *
from app.model.CnStockQueryModel import *


router = APIRouter(prefix="/cn")


@router.post("/symbol/list")
async def list_symbol(query: StockInfoQuery, db: Session = Depends(getSesion)):
    offset = (query.pageNo - 1) * query.pageSize
    query_list = []

    if query.max:
        enlarge_max = query.max * 10000 * 10000
        enlarge_min = query.min * 10000 * 10000
        query_list.append(CnStockInfo.market_value > enlarge_min)
        query_list.append(CnStockInfo.market_value < enlarge_max)

    if query.symbol:
        query_list.append(CnStockInfo.symbol == query.symbol)

    total = db.query(CnStockInfo).filter(*query_list).count()
    data = db.query(CnStockInfo).filter(*query_list).order_by(CnStockInfo.market_value.desc()).offset(offset).limit(query.pageSize).all()
    return {"total": total, "list": data}


@router.get("/symbol/info")
async def symbol_info(symbol: str, db: Session = Depends(getSesion)):
    return db.query(CnStockInfo).where(CnStockInfo.symbol == symbol).one()


@router.get("/symbol/history/data")
async def symbol_history_data(symbol: str, period: int = 300, db: Session = Depends(getSesion)):
    data = db.query(CnStockData).where(CnStockData.symbol == symbol).order_by(CnStockData.date.desc()).limit(period).all()
    data.sort(key=lambda x: x.date)
    return data


@router.get("/vol/category")
def vol_category(date: str, db: Session = Depends(getSesion)):
    data = db.query(CnStockVolUp).where(CnStockVolUp.date == date).all()
    industry_set = set()
    for x in data:
        industry_set.add(x.industry)
    return industry_set


@router.get("/vol/up")
async def vol_up(date: str, industry: str, db: Session = Depends(getSesion)):
    df = pd.read_sql(f"select a.*, b.market_value from cn_stock_vol_up a left join cn_stock_info b on a.symbol = b.symbol where a.date = '{date}' and a.industry = '{industry}' order by b.market_value asc ", engine.connect())
    # data = df.to_json(orient="records", force_ascii=False)
    symbol_list = df["symbol"].values.tolist()
    data_list = []
    for symbol in symbol_list:
        data = db.query(CnStockData).where(CnStockData.symbol == symbol).order_by(CnStockData.date.desc()).limit(90).all()
        data.sort(key=attrgetter("date"))
        row_data = list(df.loc[df["symbol"] == symbol].squeeze())
        name = row_data[2]
        market_value = row_data[len(row_data) - 1]
        data_list.append({"symbol": symbol, "name": name, "market_value": market_value, "data": data})
    return {"total": len(df), "list": data_list}


@router.get("/analysis/vol")
async def analysis_vol(date: str, vol_type: int, db: Session = Depends(getSesion)):
    date_symbols = db.query(CNStockVolAnalysis).where(CNStockVolAnalysis.date == date).filter(CNStockVolAnalysis.vol_type == vol_type).order_by(CNStockVolAnalysis.trade_vol_pct.desc()).all()
    data_list = []
    for data in date_symbols:
        temp_data = db.query(CnStockData).where(CnStockData.symbol == data.symbol).order_by(CnStockData.date.desc()).limit(100).all()
        temp_data.sort(key=lambda x: x.date)
        data_list.append({"symbol": data.symbol, "data": temp_data})
    return data_list
