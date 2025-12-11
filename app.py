# app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
from celery.result import AsyncResult
# FIX: Import the correct task name
from celery_app import generate_report_task 

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
        task_result = AsyncResult(task_id)
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