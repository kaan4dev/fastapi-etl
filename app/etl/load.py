import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from app.models import CryptoPrice, CryptoPriceHistory
from datetime import datetime

def load_crypto_prices(db: Session, df: pd.DataFrame) -> int:
    db.execute(delete(CryptoPrice))
    rows = df.to_dict(orient="records")
    db.bulk_insert_mappings(CryptoPrice, rows)
    db.commit()
    return len(rows)

def upsert_crypto_prices(db:Session, df: pd.DataFrame) -> int:
    rows = df.to_dict(orient = "records")

    stmt = insert(CryptoPrice).values(rows)

    update_cols = {
        "symbol": stmt.excluded.symbol,
        "name": stmt.excluded.name,
        "price_usd": stmt.excluded.price_usd,
        "market_cap_usd": stmt.excluded.market_cap_usd,
        "updated_at_iso": stmt.excluded.updated_at_iso,
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["coin_id"],
        set_=update_cols,
    )

    result = db.execute(stmt)
    db.commit()
    return result.rowcount

def insert_crypto_price_history(
    db: Session,
    run_id: int,
    df: pd.DataFrame,
    captured_at: datetime
) -> int:
    hist_df = df[["coin_id", "price_usd", "market_cap_usd"]].copy()
    hist_df["run_id"] = run_id
    hist_df["captured_at"] = captured_at

    rows = hist_df.to_dict(orient="records")

    db.bulk_insert_mappings(CryptoPriceHistory, rows)
    db.commit()
    return len(rows)