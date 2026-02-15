import os
from dotenv import load_dotenv
import pyotp
from SmartApi import SmartConnect
import time

def test_login():
    print("Loading .env...")
    load_dotenv()
    
    api_key = os.getenv('ANGELONE_API_KEY')
    client_id = os.getenv('ANGELONE_CLIENT_ID')
    password = os.getenv('ANGELONE_PASSWORD') or os.getenv('MPIN')
    totp_secret = os.getenv('ANGELONE_TOTP_SECRET')
    
    print(f"Client ID: {client_id}")
    print(f"TOTP Secret present: {'Yes' if totp_secret else 'No'}")
    
    try:
        print("Initializing SmartConnect...")
        smart_api = SmartConnect(api_key=api_key)
        
        print("Generating TOTP...")
        totp = pyotp.TOTP(totp_secret).now()
        print(f"TOTP generated: {totp}")
        
        print("Attempting generateSession...")
        data = smart_api.generateSession(client_id, password, totp)
        print(f"Session Response: {data}")
        
        if data.get('status'):
            print("Login SUCCESS!")
            return True
        else:
            print(f"Login FAILED: {data.get('message')}")
            return False
            
    except Exception as e:
        print(f"Exception during login: {e}")
        return False

if __name__ == "__main__":
    test_login()
