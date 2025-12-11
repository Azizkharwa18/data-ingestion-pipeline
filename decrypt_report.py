import gnupg
import os
import json
from celery_app import generate_report_task
import json

def get_and_decrypt_report():
    # 1. Fetch the encrypted result from Celery
    print("📡 Fetching data from the cloud...")
    task = generate_report_task.delay("large_sales_data.csv")
    result = task.get()
    
    if result.get('status') != 'success':
        print("Task failed!")
        return

    encrypted_blob = result['secure_payload']
    print("\n🔒 Received Encrypted Payload (PGP Message)")
    
    # 2. Initialize GPG
    gpg_home = os.path.join(os.getcwd(), 'keys')
    gpg = gnupg.GPG(gnupghome=gpg_home)
    
    # 3. Decrypt
    print("🔓 Decrypting...")
    decrypted_data = gpg.decrypt(encrypted_blob, passphrase="my_secret_password")
    
    if decrypted_data.ok:
        # Convert string back to JSON
        final_json = json.loads(str(decrypted_data))
        print("\n✅ Access Granted! Here is a sample of the data:")
        print(json.dumps(final_json[:2], indent=2)) # Show first 2 records
    else:
        print("❌ Decryption failed. Wrong password or key?")

if __name__ == "__main__":
    get_and_decrypt_report()