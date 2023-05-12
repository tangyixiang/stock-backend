import os
import akshare as ak
import pandas as pd
from fastapi import APIRouter
from app.config.db import dbpool
from app.config.log import logger as log
from app.task.DataScheduleTask import *

router = APIRouter(prefix="/init")

csv_path = os.path.join(os.getcwd(), "csv")
if not os.path.exists(csv_path):
    os.mkdir(csv_path)


@router.get("/cn/symbol")
async def init_all_symbol():
    all_symbol()
    return "ok"


@router.get("/cn/data/tocsv")
async def init_data_tocsv():
    symbol_list = list(
        pd.read_sql("select symbol from cn_stock_info", dbpool.getconn())["symbol"]
    )
    for symbol in symbol_list:
        log.info("同步数据:{}", symbol)
        today = datetime.now().strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(symbol=symbol, start_date="20150101", end_date=today)
        df.insert(loc=0, column="symbol", value=symbol)
        file_name = csv_path + "/" + symbol + ".csv"
        df.to_csv(file_name, index=False, header=False, encoding="UTF-8")
        log.info("同步完成:{}", symbol)
    return "ok"


@router.get("/cn/data/import")
async def import_csv_data():
    file_names = os.listdir(csv_path)
    for file_name in file_names:
        log.info("开始导入:{}", file_name)
        file = os.path.join(csv_path, file_name)
        with open(file, "r", encoding="UTF-8") as f:
            conn = dbpool.getconn()
            cursor = conn.cursor()
            cursor.copy_from(
                f,
                "cn_stock_data",
                sep=",",
                null="",
                columns=(
                    "symbol",
                    "date",
                    "open",
                    "close",
                    "high",
                    "low",
                    "trade_vol",
                    "trade_quota",
                    "amplitude",
                    "diff_per",
                    "diff_quota",
                    "exchange_rate",
                ),
            )
            conn.commit()
            cursor.close()
        log.info("导入完成")
