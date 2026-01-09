from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import select

from app.db import Base, engine, get_db, SessionLocal
from app.models import ETLRun, CryptoPrice
from app.schemas import RunRequest, RunResponse, RunStatusResponse, CryptoPriceOut
from app.etl.pipeline import run_pipeline

app = FastAPI(title = "FastAPI ETL Service", version = "1.0.0") 

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

@app.get("/health")
def health():
    return {"status":"ok"}

def _execute_run(run_id: int, top_n: int):
    db = SessionLocal()
    try:
        run = db.get(ETLRun, run_id)
        if not run:
            return
        run.status = "RUNNING"
        run.started_at = datetime.now(timezone.utc)
        db.commit()

        upserted, history_inserted = run_pipeline(db=db, top_n=top_n, run_id=run_id)

        run.status = "SUCCESS"
        run.message = f"Upserted {upserted} rows into crypto_prices. Inserted {history_inserted} rows into crypto_prices_history."
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
    except Exception as e:
        run = db.get(ETLRun, run_id)
        if run:
            run.status = "FAILED"
            run.message = str(e)[:480]
            run.finished_at = datetime.now(timezone.utc)
            db.commit()
    finally:
        db.close()

@app.post("/runs", response_model = RunResponse)
def create_run(payload: RunRequest, bg: BackgroundTasks, db: Session = Depends(get_db)):
    run = ETLRun(job_name = payload.job_name, status = "PENDING")
    db.add(run)
    db.commit()
    db.refresh(run)

    bg.add_task(_execute_run, run.id, payload.top_n)
    return RunResponse(run_id = run.id, status = run.status)

@app.get("/runs/{run_id}", response_model = RunStatusResponse)
def get_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(ETLRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail = "Run not found.")
    return RunStatusResponse(
        run_id = run.id,
        job_name = run.job_name,
        status = run.status,
        message = run.message,
    )

@app.get("/prices", response_model=list[CryptoPriceOut])
def list_prices(
    limit: int = 50,
    coin_id: Optional[str] = None,
    symbol: Optional[str] = None,
    db: Session = Depends(get_db),
):
    limit = max(1, min(limit, 250))
    stmt = select(CryptoPrice)

    if coin_id:
        stmt = stmt.where(CryptoPrice.coin_id == coin_id)
    if symbol:
        stmt = stmt.where(CryptoPrice.symbol == symbol.upper())

    stmt = stmt.order_by(CryptoPrice.market_cap_usd.desc().nullslast()).limit(limit)
    return db.execute(stmt).scalars().all()
