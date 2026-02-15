from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import config
from logzero import logger
import threading
import time
import json

class WebSocketManager:
    def __init__(self, session):
        self.session = session
        self.sws = None
        self.latest_data = {}  # Map token -> {'ltp': x, 'oi': y, 'ts': t}
        self.pending_tokens_nfo = []
        self.pending_tokens_nse = []
        self.connected = False
        self.subscription_mode = 3  # 3 = Full data with OI
        
    def connect(self, tokens_nfo, tokens_nse=[]):
        """
        Connects to WebSocket and subscribes to provided tokens.
        """
        if not self.session.auth_token:
            logger.error("No Auth Token for WebSocket")
            return False

        self.pending_tokens_nfo = tokens_nfo
        self.pending_tokens_nse = tokens_nse
        
        try:
            self.sws = SmartWebSocketV2(
                self.session.auth_token,
                self.session.api_key,
                self.session.client_id,
                self.session.feed_token
            )
            
            self.sws.on_open = self.on_open
            self.sws.on_data = self.on_data
            self.sws.on_error = self.on_error
            self.sws.on_close = self.on_close
            
            logger.info(f"Connecting WebSocket (NFO: {len(tokens_nfo)}, NSE: {len(tokens_nse)})...")
            self.sws.connect()
            return True
            
        except Exception as e:
            logger.error(f"WebSocket Connection Error: {e}")
            return False

    def subscribe_tokens(self):
        """Subscribe to tokens in batches"""
        try:
            # Subscribe NFO tokens (Exchange 2)
            if self.pending_tokens_nfo:
                batch_size = 100
                for i in range(0, len(self.pending_tokens_nfo), batch_size):
                    batch = self.pending_tokens_nfo[i:i+batch_size]
                    token_list = [{"exchangeType": 2, "tokens": batch}]
                    self.sws.subscribe(f"sub_nfo_{i}", self.subscription_mode, token_list)
                    time.sleep(0.5)
            
            # Subscribe NSE tokens (Exchange 1) for Spot Price
            if self.pending_tokens_nse:
                token_list = [{"exchangeType": 1, "tokens": self.pending_tokens_nse}]
                self.sws.subscribe("sub_nse_spot", self.subscription_mode, token_list)
                
            self.connected = True
            
        except Exception as e:
            logger.error(f"Subscription error: {e}")

    def on_data(self, wsapp, message):
        try:
            if isinstance(message, dict):
                token = message.get('token')
                if token:
                    self.latest_data[token] = {
                        'ltp': message.get('last_traded_price', message.get('ltp', 0)),
                        'oi': message.get('open_interest', message.get('oi', 0)),
                        'timestamp': time.time()
                    }
        except Exception as e:
            pass

    def on_open(self, wsapp):
        logger.info("WebSocket Connected")
        self.subscribe_tokens()

    def on_error(self, wsapp, error):
        logger.error(f"WebSocket Error: {error}")
        self.connected = False

    def on_close(self, wsapp):
        logger.info("WebSocket Closed")
        self.connected = False
        
    def get_oi(self, token):
        return self.latest_data.get(token, {}).get('oi')
    
    def get_ltp(self, token):
        return self.latest_data.get(token, {}).get('ltp', 0)
