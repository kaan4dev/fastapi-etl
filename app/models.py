from sqlalchemy import String, DateTime, Integer, Float, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base

class ETLRun(Base):
    __tablename__ = "etl-runs"

    id: Mapped[int] = mapped_column(Integer, primary_key = True, index = True)
    job_name: Mapped[str] = mapped_column(String(100), index = True)
    status: Mapped[str] = mapped_column(String(30), index = True)
    message: Mapped[str | None] = mapped_column(String(500), index = True)

    started_at = Mapped[DateTime | None] = mapped_column(DateTime(timezone = True), nullable = True)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class CryptoPrice(Base):
    __tablename__ = "crypto_prices"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    coin_id: Mapped[str] = mapped_column(String(50), index=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    name: Mapped[str] = mapped_column(String(100))
    price_usd: Mapped[float] = mapped_column(Float)
    market_cap_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    updated_at_iso: Mapped[str] = mapped_column(String(40))