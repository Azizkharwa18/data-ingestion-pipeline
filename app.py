from flask import Flask, jsonify, request
from flask_cors import CORS
from celery.result import AsyncResult
# Import the actual task from your celery_app file
from celery_app import encrypt_data_task 

app = Flask(__name__)
CORS(app) # Allow Angular to connect

@app.route('/api/start-task', methods=['POST'])
def start_task():
    payload = request.json.get('payload')
    
    # 1. Trigger the real Celery task
    # .delay() sends the task to Redis queue
    task = encrypt_data_task.delay(payload) 
    
    # 2. Return the Task ID immediately to Angular
    return jsonify({'task_id': task.id}), 202

@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    # 3. Check Redis for the task status using Celery's AsyncResult
    task_result = AsyncResult(task_id)
    
    response = {
        'task_id': task_id,
        'status': task_result.status, # PENDING, STARTED, SUCCESS, FAILURE
        'result': task_result.result if task_result.ready() else None
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=5000)