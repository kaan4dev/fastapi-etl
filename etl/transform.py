import pandas as pd

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
