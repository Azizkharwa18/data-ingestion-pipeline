from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from celery.result import AsyncResult
from sqlalchemy import create_engine, text
from typing import Optional, List

# Imports from your existing modules
from celery_app import celery_app, generate_report_task
from vault_util import get_db_credentials

app = FastAPI(title="Data Ingestion Pipeline API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models ---
class TaskRequest(BaseModel):
    filename: Optional[str] = 'large_sales_data.csv'

class TaskResponse(BaseModel):
    task_id: str

class MetricsResponse(BaseModel):
    bucket: str  # TimescaleDB returns timestamps as strings/datetime
    category: str
    avg_value: float

# --- Endpoints ---

@app.post("/api/start-task", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
def start_task(request: TaskRequest):
    """
    Triggers the Celery task to process the CSV file.
    """
    print(f"🚀 Triggering task for file: {request.filename}")
    # .delay() is the standard way to call Celery tasks asynchronously
    task = generate_report_task.delay(request.filename)
    return {"task_id": task.id}

@app.get("/api/status/{task_id}")
def get_status(task_id: str):
    """
    Checks the status of a specific Celery task.
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        result_data = None
        if task_result.ready():
            result_data = task_result.result
            
        return {
            "task_id": task_id,
            "status": task_result.status,
            "result": result_data
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/metrics")
def get_metrics():
    """
    Returns time-series data aggregated by 1-hour buckets.
    """
    try:
        conn_string = get_db_credentials()
        if not conn_string:
            raise HTTPException(status_code=500, detail="Could not fetch DB credentials")
            
        engine = create_engine(conn_string)
        
        query = """
            SELECT 
                time_bucket('1 hour', timestamp) AS bucket,
                category,
                AVG(adjusted_amount) as avg_value
            FROM sales_metrics
            GROUP BY bucket, category
            ORDER BY bucket ASC;
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            # Convert SQLAlchemy rows to list of dicts
            rows = [dict(row) for row in result.mappings()]
            
        return rows
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)