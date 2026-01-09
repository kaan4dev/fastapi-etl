from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Literal, Optional
from datetime import datetime

class RunRequest(BaseModel):
    job_name: str = Field(min_length=2, max_length=100, examples=["coingecko_test"])
    top_n: int = Field(default=50, ge=1, le=250, examples=[50])

class RunResponse(BaseModel):
    run_id: int
    status: str = Field(examples=["PENDING"])

class RunStatusResponse(BaseModel):
    run_id: int
    job_name: str
    status: str   
    message: Optional[str] = None

class CryptoPriceOut(BaseModel):
    model_config = ConfigDict(from_attributes = True)
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
