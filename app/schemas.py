from pydantic import BaseModel, Field
from typing import Literal, Optional

class RunRequest(BaseModel):
    job_name: str = Field(default="coingecko_top_coins")
    top_n: int = Field(default=20, ge=1, le=250)

class RunResponse(BaseModel):
    run_id: int
    status: str

class RunStatusResponse(BaseModel):
    run_id: int
    job_name: str
    status: Literal["PENDING", "RUNNING", "SUCCESS", "FAILED"]
    message: Optional[str] = None