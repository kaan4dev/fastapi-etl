import pandas as pd
from app.schemas import CryptoPriceDQ
from pydantic import ValidationError

def transform_coins(raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    keep = df[["id", "symbol", "name", "current_price", "market_cap", "last_updated"]].copy()
    keep.rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "market_cap": "market_cap_usd",
            "last_updated": "updated_at_iso",
        },
        inplace=True,
    )
    keep["symbol"] = keep["symbol"].str.upper()
    keep["price_usd"] = keep["price_usd"].astype(float)
    return keep

def validate_with_pydantic(df):
    errors = []

    for i, row in df.iterrows():
        try:
            CryptoPriceDQ(**row.to_dict())
        except ValidationError as e:
            errors.append(f"Row {i}: {e.errors()}")

    if errors:
        raise ValueError(" | ".join(errors))

def validate_dataset_level(df):
    if df.empty:
        raise ValueError("Dataset is empty")

    if df["coin_id"].duplicated().any():
        raise ValueError("Duplicate coin_id values found")
