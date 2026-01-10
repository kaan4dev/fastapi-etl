from pydantic import ValidationError
import pandas as pd
from app.schemas_etl import CryptoPriceIn
import json


def run_dq_checks(df: pd.DataFrame) -> dict:
    report = {"issues": [], "summary": {}}

    required_cols = ["coin_id", "symbol", "name", "price_usd", "updated_at_iso"]
    missing_required = {
        col: int(df[col].isna().sum())
        for col in required_cols
        if col in df.columns
    }

    if any(v > 0 for v in missing_required.values()):
        report["issues"].append(
            {"type": "null_required", "detail": missing_required}
        )

    if "price_usd" in df.columns:
        bad_price = int((df["price_usd"] <= 0).sum())
        if bad_price:
            report["issues"].append(
                {"type": "range_price_usd", "detail": {"bad_count": bad_price}}
            )

    if "market_cap_usd" in df.columns:
        bad_mcap = int((df["market_cap_usd"] < 0).sum())
        if bad_mcap:
            report["issues"].append(
                {"type": "range_market_cap_usd", "detail": {"bad_count": bad_mcap}}
            )

    if "coin_id" in df.columns:
        dup_count = int(df["coin_id"].duplicated().sum())
        if dup_count:
            report["issues"].append(
                {"type": "duplicate_coin_id", "detail": {"dup_count": dup_count}}
            )

    report["summary"] = {
        "rows": int(len(df)),
        "issues_count": len(report["issues"]),
    }
    return report


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

    df = pd.DataFrame(rows)
    dq_report = run_dq_checks(df)
    if dq_report["issues"]:
        raise ValueError(str(dq_report))

    return df
