import requests
import pandas as pd
import datetime
import config
import time
from logzero import logger

class DataManager:
    def __init__(self, session):
        self.session = session # AngelOneSession instance
        self.headers = None
        self.token_map = {} # {'NIFTY': {'expiry': '...', 'tokens': [...]}}
        
    def setup_headers(self):
        if not self.session.auth_token:
            logger.error("No Auth Token available for DataManager")
            return
            
        self.headers = {
            'Authorization': f'Bearer {self.session.auth_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-ClientLocalIP': '127.0.0.1',
            'X-ClientPublicIP': '127.0.0.1',
            'X-MACAddress': '00:00:00:00:00:00',
            'X-PrivateKey': self.session.api_key,
            'X-UserType': 'USER',
            'X-SourceID': 'WEB'
        }

    def _rate_limit(self):
        time.sleep(0.5) # Basic rate limiting

    def fetch_instrument_master(self):
        """
        Downloads and parses the Scrip Master to find Option tokens for current expiry.
        """
        logger.info("Fetching Instrument Master (Downloading ~50MB file)...")
        try:
            self._rate_limit()
            start_time = time.time()
            response = requests.get(config.INSTRUMENT_URL, timeout=60)
            logger.info(f"Download complete in {round(time.time() - start_time, 2)}s. Parsing JSON...")
            data = response.json()
            logger.info(f"JSON parsed. Processing {len(data)} instruments...")
            df = pd.DataFrame(data)
            
            # Filter for NSE_FO and our Symbols
            df_nfo = df[df['exch_seg'] == 'NFO']
            df_nse = df[df['exch_seg'] == 'NSE']
            
            # Index Token Map
            index_tokens = {'NIFTY': '26000', 'BANKNIFTY': '26009', 'FINNIFTY': '26037'}

            for symbol in config.SYMBOLS:
                # 1. Find Spot Token
                spot_token = None
                if symbol in index_tokens:
                    spot_token = index_tokens[symbol]
                else:
                    # For stocks, find the NSE token
                    stock_spot = df_nse[(df_nse['name'] == symbol) & (df_nse['symbol'].str.endswith('-EQ'))]
                    if not stock_spot.empty:
                        spot_token = stock_spot.iloc[0]['token']

                # 2. Find Future and Option tokens (NFO)
                symbol_df = df_nfo[df_nfo['name'] == symbol].copy()
                if symbol_df.empty: continue
                
                symbol_df['expiry_dt'] = pd.to_datetime(symbol_df['expiry'], format='%d%b%Y')
                today = pd.Timestamp.now().normalize()
                future_expiries = symbol_df[symbol_df['expiry_dt'] >= today]['expiry_dt'].unique()
                if len(future_expiries) == 0: continue
                    
                nearest_expiry = sorted(future_expiries)[0]
                nearest_expiry_str = nearest_expiry.strftime('%d%b%Y').upper()
                expiry_df = symbol_df[symbol_df['expiry_dt'] == nearest_expiry]
                
                tokens_list = []
                fut_token = None
                for _, row in expiry_df.iterrows():
                    t_info = {
                        'token': row['token'],
                        'symbol': row['symbol'],
                        'instrumenttype': row['instrumenttype'],
                        'strike': float(row['strike']) if row['strike'] else 0,
                        'lotsize': int(row['lotsize']) if row['lotsize'] else 0,
                        'expiry': nearest_expiry_str
                    }
                    tokens_list.append(t_info)
                    if "FUT" in row['instrumenttype']:
                        fut_token = row['token']
                
                self.token_map[symbol] = {
                    'expiry': nearest_expiry_str,
                    'tokens': tokens_list,
                    'spot_token': spot_token,
                    'fut_token': fut_token
                }
                logger.info(f"Loaded {symbol}: Spot={spot_token}, Fut={fut_token}, Options={len(tokens_list)}")
                
            return True
                
        except Exception as e:
            logger.error(f"Error fetching instrument master: {e}")
            return False

    def fetch_iv_greeks(self, symbol, expiry_date):
        """
        Fetches Option Chain with Greeks (IV) via REST API.
        """
        if not self.headers:
            self.setup_headers()
            
        payload = {
            "name": symbol,
            "expirydate": expiry_date
        }
        
        try:
            self._rate_limit()
            response = requests.post(config.OPTION_GREEKS_URL, headers=self.headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') and data.get('data'):
                    return pd.DataFrame(data['data'])
            else:
                logger.error(f"IV Fetch Failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Exception fetching IV: {e}")
            
        return None
