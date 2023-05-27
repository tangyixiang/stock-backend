from datetime import datetime
from app.config.db import *
from sqlalchemy import func
from app.model.CnStockModel import *
from app.config.log import logger as log


def is_openday():
    today = datetime.now().strftime("%Y-%m-%d")
    db = singleSession()
    db_max_date = db.query(func.max(CnStockData.date)).scalar().strftime("%Y-%m-%d")
    isopen = today == db_max_date
    log.info(f"open day == f{isopen}")
    return isopen
