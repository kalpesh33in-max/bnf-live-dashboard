import pandas as pd
import numpy as np
import datetime
import config
import time
from logzero import logger

class Strategy:
    def __init__(self, data_mgr, ws_mgr):
        self.data_mgr = data_mgr
        self.ws_mgr = ws_mgr
        self.history = {} # {'NIFTY': [{'timestamp': t, 'iv': df, 'oi': {token: oi}, 'spot': price, 'future': price}]}
        self.last_signals = {} # To prevent spam

    def add_snapshot(self, symbol, iv_df, oi_map, spot_price, future_price):
        if symbol not in self.history:
            self.history[symbol] = []
            
        snapshot = {
            'timestamp': datetime.datetime.now(),
            'iv': iv_df,
            'oi': oi_map,
            'spot': spot_price,
            'future': future_price
        }
        
        self.history[symbol].append(snapshot)
        cutoff = datetime.datetime.now() - datetime.timedelta(minutes=60)
        self.history[symbol] = [s for s in self.history[symbol] if s['timestamp'] > cutoff]

    def calculate_roc(self, symbol, minutes_back=5):
        if symbol not in self.history or len(self.history[symbol]) < 2:
            return None, None
            
        current = self.history[symbol][-1]
        target_time = current['timestamp'] - datetime.timedelta(minutes=minutes_back)
        past = min(self.history[symbol], key=lambda x: abs((x['timestamp'] - target_time).total_seconds()))
        
        if (current['timestamp'] - past['timestamp']).total_seconds() < 60:
            return None, None

        # IV ROC
        iv_curr = current['iv'][['strikePrice', 'optionType', 'impliedVolatility', 'token']].copy()
        iv_prev = past['iv'][['strikePrice', 'optionType', 'impliedVolatility']].copy()
        merged = pd.merge(iv_curr, iv_prev, on=['strikePrice', 'optionType'], suffixes=('_curr', '_prev'))
        merged['iv_roc'] = ((merged['impliedVolatility_curr'] - merged['impliedVolatility_prev']) / merged['impliedVolatility_prev'].replace(0, np.nan)) * 100
        
        # OI ROC
        merged['oi_curr'] = merged['token'].map(current['oi'])
        merged['oi_prev'] = merged['token'].map(past['oi'])
        merged['oi_roc'] = ((merged['oi_curr'] - merged['oi_prev']) / merged['oi_prev'].replace(0, np.nan)) * 100
        
        # Future ROC
        fut_roc = ((current['future'] - past['future']) / past['future']) * 100 if past['future'] else 0
        
        return merged, fut_roc

    def check_signals(self, symbol):
        df, fut_roc = self.calculate_roc(symbol)
        if df is None or df.empty:
            return []
            
        current = self.history[symbol][-1]
        spot = current['spot']
        future = current['future']
        
        signals = []
        token_lookup = {t['token']: t for t in self.data_mgr.token_map.get(symbol, {}).get('tokens', [])}
        
        # ATM range: +/- 0.5% of spot
        atm_buffer = spot * 0.005 

        for _, row in df.iterrows():
            strike = row['strikePrice']
            otype = row['optionType']
            token = row['token']
            
            # Get Token Info
            token_info = token_lookup.get(token, {})
            lot_size = token_info.get('lotsize', 1)
            instrument_type = token_info.get('instrumenttype', '')

            oi_curr = row['oi_curr']
            oi_prev = row['oi_prev']
            if pd.isna(oi_curr) or pd.isna(oi_prev): continue
            
            oi_change_abs = abs(oi_curr - oi_prev)
            lots_impacted = oi_change_abs / lot_size
            
            # --- SHARED THRESHOLD: 300 LOTS ---
            if lots_impacted < 300:
                continue

            # --- FUTURE LOGIC ---
            if "FUT" in instrument_type:
                regime = "FUT Movement"
                action = "Aggressive Future Build-up" if oi_curr > oi_prev else "Aggressive Future Exit"
                self._add_signal(signals, symbol, "FUT", "FUT", regime, action, row, spot, future, fut_roc, lot_size, oi_curr, oi_prev, lots_impacted)
                continue

            # --- OPTION LOGIC (ATM/ITM check) ---
            is_atm = abs(strike - spot) <= atm_buffer
            is_itm_ce = (otype == 'CE' and strike < spot)
            is_itm_pe = (otype == 'PE' and strike > spot)

            # Skip OTM
            if not (is_atm or is_itm_ce or is_itm_pe):
                continue
            
            status = "ATM" if is_atm else "ITM"

            # Regime Mapping from logic.pdf
            oi_roc = row['oi_roc']
            iv_roc = row['iv_roc']
            regime, action = "Volume Spike", "Significant Activity"
            
            if fut_roc > 0 and oi_roc > 0 and iv_roc > 0:
                regime, action = "Long Buildup", "BUY Call (aggressive)"
            elif fut_roc < 0 and oi_roc > 0 and iv_roc > 0:
                regime, action = "Short Buildup", "BUY Put (aggressive)"
            elif fut_roc < 0 and oi_roc < 0 and iv_roc < 0:
                regime, action = "Long Unwinding", "EXIT longs / AVOID buying"
            elif fut_roc > 0 and oi_roc < 0 and iv_roc < 0:
                regime, action = "Short Covering", "EXIT shorts / AVOID selling"

            self._add_signal(signals, symbol, f"{strike} {otype} ({status})", otype, regime, action, row, spot, future, fut_roc, lot_size, oi_curr, oi_prev, lots_impacted)
        
        return signals

    def _add_signal(self, signals, symbol, strike, otype, regime, action, row, spot, future, fut_roc, lot_size, oi_curr, oi_prev, lots_impacted):
        signal_key = f"{symbol}_{strike}_{otype}_{regime}"
        if signal_key not in self.last_signals or (datetime.datetime.now() - self.last_signals[signal_key]).total_seconds() > 1800:
            signals.append({
                'symbol': symbol,
                'strike': strike,
                'type': otype,
                'regime': regime,
                'action': action,
                'oi_roc': round(row['oi_roc'], 2),
                'iv_roc': round(row['iv_roc'], 2) if otype != 'FUT' else 0,
                'iv': round(row['impliedVolatility_curr'], 2) if otype != 'FUT' else 0,
                'spot': spot,
                'future': future,
                'fut_roc': round(fut_roc, 2),
                'lot_size': lot_size,
                'lots_impact': int(lots_impacted),
                'oi_change': int(oi_curr - oi_prev),
                'oi_existing': int(oi_curr)
            })
            self.last_signals[signal_key] = datetime.datetime.now()
