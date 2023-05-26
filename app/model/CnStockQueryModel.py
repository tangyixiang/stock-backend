from pydantic import BaseModel
from typing import Optional


class StockInfoQuery(BaseModel):
    min: Optional[int]
    max: Optional[int]
    symbol: Optional[str]
    pageSize: int = 20
    pageNo: int = 1
