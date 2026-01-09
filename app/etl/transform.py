from pydantic import ValidationError
import pandas as pd
from app.schemas_etl import CryptoPriceIn

def transform_coins(raw: list[dict]) -> pd.DataFrame:
    rows = []
    errors = []

    for item in raw:
        try:
            row = CryptoPriceIn(
                coin_id=item["id"],
                symbol=item["symbol"].upper(),
                name=item["name"],
                price_usd=float(item["current_price"]),
                market_cap_usd=item.get("market_cap"),
                updated_at_iso=item.get("last_updated") or "",
            )
            rows.append(row.model_dump())
        except ValidationError as e:
            errors.append(e.errors())

    if errors:
        # In real projects: log this, or raise a custom error with context.
        raise ValueError(f"Validation failed for {len(errors)} rows")

    return pd.DataFrame(rows)
