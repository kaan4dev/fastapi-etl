# FastAPI Crypto ETL Pipeline

A production-style **ETL data service** built with **FastAPI, SQLAlchemy, PostgreSQL, and Docker**.  
The project ingests cryptocurrency market data, enforces **data quality rules**, stores both **latest snapshots** and **historical time-series**, and exposes clean **REST APIs** for data access.

---

## What This Project Does

- Extracts cryptocurrency market data from a public API  
- Validates data using **Pydantic-based data quality checks**  
- Loads data into PostgreSQL using **idempotent UPSERT logic**  
- Maintains:
  - a **latest snapshot table**
  - an **append-only history table**
- Tracks every ETL run with status, timestamps, and messages  
- Exposes REST APIs to:
  - trigger ETL runs
  - inspect run status
  - query latest prices
  - query historical price data  

This project is designed to reflect **real-world data engineering practices**, not just a demo script.

---

## Architecture Overview

```
External Crypto API
        |
        v
FastAPI (ETL Orchestration)
        |
        |-- Data Quality Checks (Pydantic + Pandas)
        |
        |-- UPSERT → crypto_prices (latest snapshot)
        |
        |-- INSERT → crypto_prices_history (time series)
        |
        v
PostgreSQL
        |
        v
REST APIs (/prices, /history)
```
