from pydantic import ValidationError
import pandas as pd
from app.schemas_etl import CryptoPriceIn
import json

def transform_coins(raw: list[dict]) -> pd.DataFrame:
    rows = []
    errors = []
    max_samples = 3

    for i, item in enumerate(raw):
        try:
            row = CryptoPriceIn(
                coin_id=item["id"],
                symbol=item["symbol"],
                name=item["name"],
                price_usd=float(item["current_price"]),
                market_cap_usd=item.get("market_cap"),
                updated_at_iso=item.get("last_updated") or "",
            )
            rows.append(row.model_dump())
        except ValidationError as e:
            if len(errors) < max_samples:
                errors.append(
                    {
                        "index": i,
                        "coin_id": item.get("id"),
                        "errors": e.errors(),
                    }
                )

    if errors:
        error_report = {
            "error_type": "VALIDATION_ERROR",
            "error_count": len(errors),
            "error_samples": errors,
        }
        raise ValueError(json.dumps(error_report, ensure_ascii=True))

    return pd.DataFrame(rows)
