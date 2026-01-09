from fastapi import FastAPI, Depends, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import datetime, timezone

from app.db import SessionLocal
from app.db import Base, engine, get_db
from app.models import ETLRun
from app.schemas import RunRequest, RunResponse, RunStatusResponse
from app.etl.pipeline import run_pipeline

def run_pipeline(db: Session, top_n: int) -> int:
    raw = extract_top_coins(top_n=top_n)
    df = transform_coins(raw)
    inserted = load_crypto_prices(db, df)
    return inserted

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
        run.status = "RUNNING"
        run_started_at = datetime.now(timezone.utc)
        db.commit()

        inserted = run_pipeline(db=db, top_n = top_n)

        run.status = "SUCCESS"
        run.message = f"Loaded {inserted} rows into cryto.prices."
        run.finished_at = datetime.now(timezone.utc)
    except Exception as e:
        run = db.get(ETLRun, run_id)
        if run:
            run.status = "FAILED"
            run.message = str(e)[:480]
            run_finished_at = datetime.now(timezone.utc)
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