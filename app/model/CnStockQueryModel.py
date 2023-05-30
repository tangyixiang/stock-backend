from pydantic import BaseModel
from typing import Optional, List


class StockInfoQuery(BaseModel):
    min: Optional[int]
    max: Optional[int]
    symbol: Optional[str]
    pageSize: int = 20
    pageNo: int = 1


class StockSymbolListQuery(BaseModel):
    symbol_list: List[str]
