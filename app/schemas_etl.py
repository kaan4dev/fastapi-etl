from pydantic import BaseModel, Field, ConfigDict
from typing import Optional

class CryptoPriceIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    coin_id: str = Field(min_length=1, max_length=50)
    symbol: str = Field(min_length=1, max_length=20)
    name: str = Field(min_length=1, max_length=100)
    price_usd: float = Field(gt=0)
    market_cap_usd: Optional[float] = Field(default=None, ge=0)
    updated_at_iso: str
