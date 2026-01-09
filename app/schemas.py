from pydantic import BaseModel, Field
from typing import Literal, Optional
from datetime import datetime

class RunRequest(BaseModel):
    job_name: str = Field(default="coingecko_top_coins")
    top_n: int = Field(default=20, ge=1, le=250)

class RunResponse(BaseModel):
    run_id: int
    status: str

class RunStatusResponse(BaseModel):
    run_id: int
    job_name: str
    status: Literal["PENDING", "RUNNING", "SUCCESS", "FAILED"]
    message: Optional[str] = None

class CryptoPriceOut(BaseModel):
    coin_id: str
    symbol: str
    name: str
    price_usd: float
    market_cap_usd: Optional[float] = None
    updated_at_iso: str

class CryptoPriceHistoryOut(BaseModel):
    run_id: int
    coin_id: str
    price_usd: float
    market_cap_usd: Optional[float] = None
    captured_at: datetime