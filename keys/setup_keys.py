import gnupg
import os

# 1. Point to the "keys" folder for storage
gpg_home = os.path.join(os.getcwd(), 'keys')
if not os.path.exists(gpg_home):
    os.makedirs(gpg_home)

# Initialize GPG
# Note: On Windows, sometimes you need to point to the binary explicitly if it's not found.
# gpg = gnupg.GPG(gnupghome=gpg_home, gpgbinary='C:\\Program Files (x86)\\GnuPG\\bin\\gpg.exe')
gpg = gnupg.GPG(gnupghome=gpg_home)

def generate_keys():
    print("🔐 Generating PGP Key Pair... (This may take a moment)")
    
    input_data = gpg.gen_key_input(
        key_type="RSA",
        key_length=2048,
        name_real="LAM Research Admin",
        name_email="admin@lamresearch.com",
        passphrase="my_secret_password" 
    )
    
    key = gpg.gen_key(input_data)
    
    print(f"✅ Key Generated!")
    print(f"   Fingerprint: {key.fingerprint}")
    
    # Export Public Key (for the Worker to use)
    public_key = gpg.export_keys(key.fingerprint)
    with open("keys/public.asc", "w") as f:
        f.write(public_key)
        
    print("✅ Public key saved to 'keys/public.asc'")

if __name__ == "__main__":
    generate_keys()