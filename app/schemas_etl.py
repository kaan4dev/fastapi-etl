from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Optional
from datetime import datetime

class CryptoPriceIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    coin_id: str = Field(min_length=1, max_length=50)
    symbol: str = Field(min_length=1, max_length=20)
    name: str = Field(min_length=1, max_length=100)
    price_usd: float = Field(gt=0)
    market_cap_usd: Optional[float] = Field(default=None, ge=0)
    updated_at_iso: str

    @field_validator("symbol")
    @classmethod
    def symbol_must_be_upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("updated_at_iso")
    @classmethod
    def must_be_iso8601(cls, v: str) -> str:
        # Accept "Z" by converting to "+00:00"
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            datetime.fromisoformat(v)
        except ValueError:
            raise ValueError("updated_at_iso must be ISO-8601")
        return v
