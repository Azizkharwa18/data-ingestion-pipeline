from celery import Celery
import os
import gnupg  # <--- Import GPG
from dask_worker import process_heavy_data # <--- Import the new module
import json

# 1. Define the Broker (RabbitMQ) and Backend (Redis) URLs
# Note: 'localhost' works because we are running this script on your Host machine,
# while the services run in Docker mapped to localhost ports.
RABBITMQ_URL = 'amqp://guest:guest@localhost:5672//'
REDIS_URL = 'redis://localhost:6379/0'

# 2. Initialize the Celery Application
celery_app = Celery(
    'lam_research_worker',
    broker=RABBITMQ_URL,
    backend=REDIS_URL
)

# 3. Configure Celery settings
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    # This ensures that if a worker crashes, the task is acknowledged as failed
    task_acks_late=True,
)

# --- ENCRYPTION HELPER ---
def encrypt_data(data_dict):
    """
    Encrypts a dictionary using the stored Public Key.
    """
    gpg_home = os.path.join(os.getcwd(), 'keys')
    gpg = gnupg.GPG(gnupghome=gpg_home)
    
    # Load Public Key
    with open('keys/public.asc', 'r') as f:
        key_data = f.read()
        gpg.import_keys(key_data)
        
    # Convert data to string for encryption
    json_data = json.dumps(data_dict)
    
    # Encrypt
    encrypted_data = gpg.encrypt(json_data, recipients=['admin@lamresearch.com'], always_trust=True)
    
    if not encrypted_data.ok:
        raise Exception(f"Encryption failed: {encrypted_data.status}")
        
    return str(encrypted_data)  

# 4. A Simple Test Task
@celery_app.task(name='tasks.test_connection')
def test_connection_task(x, y):
    import time
    # Simulate a "heavy" process by sleeping for 5 seconds
    time.sleep(5)
    return f"Processed Result: {x + y} (Powered by Celery & Redis)"

# --- NEW DASK TASK ---
@celery_app.task(name='tasks.generate_report')
def generate_report_task(filename="large_sales_data.csv"):
    
    # Ensure file exists (absolute path is safer in production)
    if not os.path.exists(filename):
        return {"error": "File not found"}

    # Run the Dask Logic
    try:
        # 1. Process Data (Dask)
        raw_report = process_heavy_data(filename)
        
        # 2. Encrypt Data (PGP)
        print("🔒 Encrypting sensitive report...")
        secure_blob = encrypt_data(raw_report)

        # 3. Return ONLY the encrypted blob
        return {"status": "success", "secure_payload": secure_blob}
    
    except Exception as e:
        return {"status": "failed", "error": str(e)}