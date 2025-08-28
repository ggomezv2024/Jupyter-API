import os, json, time
from pathlib import Path
import papermill as pm

ROOT = Path(__file__).parent.resolve()
NB_PATH = ROOT / "notebooks" / "beeline_processor.ipynb"
DATA_DIR = ROOT / "data"
OUT_DIR = DATA_DIR / "out"
LOGS_DIR = DATA_DIR / "logs"

for d in [DATA_DIR, OUT_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

def run_notebook_sync(job: dict):
    start = time.time()
    executed_nb = OUT_DIR / f"run_{job['request_id']}_{int(start)}.ipynb"

    params = {
        "PARAM_REQUEST_ID": job["request_id"],
        "PARAM_URL": job.get("url", ""),
        "PARAM_COMMENTS": job.get("comments", ""),
        "PARAM_EMAIL": job.get("email", {}),
        "PARAM_OUT_DIR": str(OUT_DIR)
    }

    pm.execute_notebook(
        input_path=str(NB_PATH),
        output_path=str(executed_nb),
        parameters=params
    )

    result_json = OUT_DIR / f"result_{job['request_id']}.json"
    result = {}
    if result_json.exists():
        result = json.loads(result_json.read_text(encoding="utf-8"))

    return {
        "request_id": job["request_id"],
        "executed_notebook": str(executed_nb),
        "run_ms": int((time.time() - start) * 1000),
        "notebook_result": result
    }
