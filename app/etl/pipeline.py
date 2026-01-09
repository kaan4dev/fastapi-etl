from sqlalchemy.orm import Session
from app.etl.extract import extract_top_coins
from app.etl.transform import transform_coins, validate_with_pydantic, validate_dataset_level
from app.etl.load import upsert_crypto_prices, insert_crypto_price_history
from datetime import datetime, timezone

def run_pipeline(db: Session, top_n: int, run_id: int) -> tuple[int, int]:
    raw = extract_top_coins(top_n=top_n)
    df = transform_coins(raw)
    validate_with_pydantic(df)
    validate_dataset_level(df)
    upserted = upsert_crypto_prices(db, df)
    captured_at = datetime.now(timezone.utc)
    history_inserted = insert_crypto_price_history(db, run_id, df, captured_at)
    return upserted, history_inserted
