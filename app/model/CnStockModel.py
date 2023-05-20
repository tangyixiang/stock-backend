from sqlalchemy import Column, Index, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import VARCHAR, TEXT, DATE, DOUBLE_PRECISION, INTEGER
from sqlalchemy import MetaData

# from app.config.db import *

metadata = MetaData(schema="cn_stock")

Base = declarative_base()


class CnStockData(Base):
    __tablename__ = "cn_stock_data"

    symbol = Column(VARCHAR(32))
    date = Column(DATE, primary_key=True)
    open = Column(DOUBLE_PRECISION)
    close = Column(DOUBLE_PRECISION)
    high = Column(DOUBLE_PRECISION)
    low = Column(DOUBLE_PRECISION)
    trade_vol = Column(DOUBLE_PRECISION)
    trade_quota = Column(DOUBLE_PRECISION)
    amplitude = Column(DOUBLE_PRECISION)
    diff_per = Column(DOUBLE_PRECISION)
    diff_quota = Column(DOUBLE_PRECISION)
    exchange_rate = Column(DOUBLE_PRECISION)

    __table_args__ = (Index("cn_stock_data_date_idx", date), UniqueConstraint("symbol", "date", name="idx_symbol_date"))


# CnStockInfo
class CnStockInfo(Base):
    __tablename__ = "cn_stock_info"

    symbol = Column(VARCHAR(32), primary_key=True)
    name = Column(TEXT)
    description = Column(TEXT)
    market_value = Column(DOUBLE_PRECISION)


# CnStockVolUp
class CnStockVolUp(Base):
    __tablename__ = "cn_stock_vol_up"

    date = Column(DATE, primary_key=True)
    symbol = Column(VARCHAR(10), primary_key=True)
    name = Column(VARCHAR(255))
    up_days = Column(INTEGER)
    up_quota = Column(DOUBLE_PRECISION)
    industry = Column(VARCHAR(50))

    __table_args__ = (
        Index("idx_date", date),
        PrimaryKeyConstraint("date", "symbol"),
    )


# StockBuyWatch
class StockBuyWatch(Base):
    __tablename__ = "stock_buy_watch"

    date = Column(DATE, primary_key=True)
    symbol = Column(VARCHAR(50), primary_key=True)
    emv = Column(DOUBLE_PRECISION)
    indicator = Column(TEXT)

    __table_args__ = (Index("stock_buy_watch_date_idx", date),)
