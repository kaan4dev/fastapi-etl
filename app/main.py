from sqlalchemy.orm import Session
from app.etl.extract import extract_top_coins
from app.etl.transform import transform_coins
from app.etl.load import load_crypto_prices

def run_pipeline(db: Session, top_n: int) -> int:
    raw = extract_top_coins(top_n=top_n)
    df = transform_coins(raw)
    inserted = load_crypto_prices(db, df)
    return inserted
