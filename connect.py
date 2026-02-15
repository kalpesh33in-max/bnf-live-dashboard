import pyotp
from SmartApi import SmartConnect
import config
from logzero import logger
import time

class AngelOneSession:
    def __init__(self):
        self.api_key = config.API_KEY
        self.client_id = config.CLIENT_ID
        self.password = config.PASSWORD
        self.totp_secret = config.TOTP_SECRET
        self.smart_api = None
        self.auth_token = None
        self.feed_token = None
        self.refresh_token = None
        self.last_login_time = None
        
    def login(self):
        try:
            self.smart_api = SmartConnect(api_key=self.api_key)
            totp = pyotp.TOTP(self.totp_secret).now()
            data = self.smart_api.generateSession(self.client_id, self.password, totp)
            
            if data.get('status'):
                self.auth_token = data['data']['jwtToken']
                self.refresh_token = data['data']['refreshToken']
                self.feed_token = self.smart_api.getfeedToken()
                self.last_login_time = time.time()
                logger.info(f"Login Successful for {self.client_id}")
                return True
            else:
                logger.error(f"Login Failed: {data.get('message')}")
                return False
        except Exception as e:
            logger.error(f"Login Exception: {e}")
            return False
    
    def refresh_if_needed(self):
        if not self.last_login_time or (time.time() - self.last_login_time > 12 * 3600):
            return self.login()
        return True

    def get_api_instance(self):
        return self.smart_api

session_manager = AngelOneSession()
