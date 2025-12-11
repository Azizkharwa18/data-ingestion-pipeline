# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from celery.result import AsyncResult
# FIX: Import the correct task name
from celery_app import celery_app, generate_report_task
# Add these imports
from sqlalchemy import create_engine, text

app = Flask(__name__)
CORS(app) 

@app.route('/api/start-task', methods=['POST'])
def start_task():
    # You can pass the filename from the frontend if needed
    filename = request.json.get('filename', 'large_sales_data.csv')
    
    # FIX: Call the correct task
    task = generate_report_task.delay(filename) 
    
    return jsonify({'task_id': task.id}), 202

@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        response = {
            'task_id': task_id,
            'status': task_result.status, 
            'result': task_result.result if task_result.ready() else None
        }
        return jsonify(response)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, port=5000)

@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """
    Returns time-series data for Angular Graph.
    Aggregates data by 1-hour buckets (TimescaleDB feature).
    """
    engine = create_engine("postgresql://admin:admin_password@localhost:5432/analytics_db")
    
    # TimescaleDB 'time_bucket' is amazing for graphs!
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
        rows = [dict(row) for row in result.mappings()]
        
    return jsonify(rows)