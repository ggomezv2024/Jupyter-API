import os, time
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, Dict, Any
from processor import run_notebook_sync

API_TOKEN = os.getenv("API_TOKEN", "cambia_este_token")
RUN_MODE = os.getenv("RUN_MODE", "sync")  # "sync" | "queue" (futuro)
app = FastAPI(title="PA → Render → Jupyter API")

class EmailBody(BaseModel):
    request_id: str
    url: Optional[str] = None
    comments: Optional[str] = None
    email: Optional[Dict[str, Any]] = None
    source: Optional[str] = "power_automate"
    token: Optional[str] = None

def _auth_ok(x_api_token: Optional[str], body_token: Optional[str]) -> bool:
    return (x_api_token == API_TOKEN) or (body_token == API_TOKEN)

@app.get("/")
def health():
    return {"ok": True, "service": "render-fastapi", "time": int(time.time())}

@app.post("/ingest")
async def ingest(payload: EmailBody, request: Request, x_api_token: Optional[str] = Header(None)):
    if not _auth_ok(x_api_token, payload.token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    job = {
        "request_id": payload.request_id.strip(),
        "url": (payload.url or "").strip(),
        "comments": payload.comments or "",
        "email": payload.email or {},
        "source": payload.source or "power_automate",
        "received_ts": int(time.time())
    }
    if RUN_MODE == "sync":
        nb_result = run_notebook_sync(job)
        return {"status": "processed", "request_id": job["request_id"], "result": nb_result}
    return {"status": "noop"}
