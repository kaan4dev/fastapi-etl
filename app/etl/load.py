import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import delete
from app.models import CryptoPrice

def load_crypto_prices(db: Session, df: pd.DataFrame) -> int:
    db.execute(delete(CryptoPrice))
    rows = df.to_dict(orient="records")
    db.bulk_insert_mappings(CryptoPrice, rows)
    db.commit()
    return len(rows)
