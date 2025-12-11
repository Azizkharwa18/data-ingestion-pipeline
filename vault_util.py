import hvac
import os

# Configuration (In production, these would be env variables, not hardcoded)
VAULT_URL = 'http://localhost:8200'
VAULT_TOKEN = 'root_token_123' 

def get_db_credentials():
    """
    Connects to HashiCorp Vault and retrieves the database credentials.
    """
    try:
        # 1. Initialize the Client
        client = hvac.Client(
            url=VAULT_URL,
            token=VAULT_TOKEN
        )

        # 2. Check if we are authenticated
        if not client.is_authenticated():
            raise Exception("Vault authentication failed! Check your token.")

        # 3. Read the secret
        # Note: In Vault KV Version 2, the path usually requires 'mount_point' handling
        # standard dev server mounts KV v2 at 'secret/'
        read_response = client.secrets.kv.v2.read_secret_version(
            path='database',
            mount_point='secret'
        )

        # 4. Extract the data
        # The actual data is nested deep in the JSON response
        credentials = read_response['data']['data']
        
        return credentials

    except Exception as e:
        print(f"❌ Error fetching secrets from Vault: {e}")
        return None

# --- Quick Test ---
if __name__ == "__main__":
    print("Attempting to fetch secrets from Vault...")
    creds = get_db_credentials()
    
    if creds:
        print("\n✅ Success! Credentials retrieved:")
        print(f"   User: {creds['username']}")
        print(f"   Pass: {creds['password']}") # In real app, never print this!
        print(f"   Host: {creds['host']}")
    else:
        print("Failed to retrieve credentials.")