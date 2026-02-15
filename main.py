import time
import schedule
import threading
import config
from logzero import logger
from connect import session_manager
from data_manager import DataManager
from websocket_manager import WebSocketManager
from strategy import Strategy
from telegram_alert import send_alert
import keep_alive

def job():
    logger.info("=" * 50)
    logger.info("Starting Analysis Cycle...")
    
    for symbol in config.SYMBOLS:
        expiry = data_mgr.token_map.get(symbol, {}).get('expiry')
        if not expiry: continue
            
        iv_df = data_mgr.fetch_iv_greeks(symbol, expiry)
        if iv_df is None: continue

        # Get Spot and Future prices
        symbol_info = data_mgr.token_map.get(symbol, {})
        spot_token = symbol_info.get('spot_token')
        fut_token = symbol_info.get('fut_token')

        spot_price = ws_mgr.get_ltp(spot_token) or 0 
        future_price = ws_mgr.get_ltp(fut_token) or 0
        
        if spot_price == 0:
            logger.warning(f"Waiting for Spot Price for {symbol}...")
            continue
            
        # FIX: Get tokens for the current symbol
        tokens = symbol_info.get('tokens', [])
        
        oi_snapshot = {}
        for t in tokens:
            token_str = t['token']
            oi = ws_mgr.get_oi(token_str)
            if oi is not None:
                oi_snapshot[token_str] = oi
        
        strat.add_snapshot(symbol, iv_df, oi_snapshot, spot_price, future_price)
        signals = strat.check_signals(symbol)
        
        for s in signals:
            # Special header for Future vs Option
            header = f"🚀 *{s['regime']}*" if s['type'] == 'FUT' else f"🔥 *{s['regime']}*"
            impact_label = f"Lots: `{s['lots_impact']}`" if s['type'] == 'FUT' else f"Lot Size: `{s['lot_size']}`"
            
            msg = (f"{header}\n"
                   f"*{s['symbol']} {s['strike']} {s['type']}*\n"
                   f"Action: `{s['action']}`\n"
                   f"{impact_label}\n\n"
                   f"Spot: `{s['spot']}` | Fut: `{s['future']}`\n"
                   f"Fut ROC: `{s['fut_roc']}%`\n"
                   f"OI Chg: `{s['oi_change']}` ({s['oi_roc']}%) \n"
                   f"Exist OI: `{s['oi_existing']}`\n")
            
            # Add IV details only for Options
            if s['type'] != 'FUT':
                msg += f"IV ROC: `{s['iv_roc']}%` | IV: `{s['iv']}`"
            
            send_alert(msg)
            logger.info(f"Alert Sent: {s['regime']} for {s['symbol']}")
        
    logger.info("Cycle Complete.")
    logger.info("=" * 50)

# --- Initialization ---
logger.info("Starting Angel One Scanner Initialization...")

if not send_alert("🤖 *Scanner Initializing...*"):
    logger.error("Telegram Error")

logger.info("Attempting Login...")
if not session_manager.login():
    logger.error("Login Failed! Exiting...")
    exit(1)
logger.info("Login Successful.")

logger.info("Initializing Data, WebSocket, and Strategy Managers...")
data_mgr = DataManager(session_manager)
ws_mgr = WebSocketManager(session_manager)
strat = Strategy(data_mgr, ws_mgr)

logger.info("Fetching Instrument Master (this may take a minute)...")
if not data_mgr.fetch_instrument_master():
    logger.error("Failed to fetch Instrument Master! Exiting...")
    exit(1)
logger.info("Instrument Master Fetched.")

all_tokens_nfo = []
all_tokens_nse = []
for symbol in config.SYMBOLS:
    if symbol in data_mgr.token_map:
        t_list = [t['token'] for t in data_mgr.token_map[symbol]['tokens']]
        all_tokens_nfo.extend(t_list)
        spot_token = data_mgr.token_map[symbol].get('spot_token')
        if spot_token:
            all_tokens_nse.append(spot_token)

if all_tokens_nfo:
    logger.info(f"Connecting WebSocket for {len(all_tokens_nfo)} NFO and {len(all_tokens_nse)} NSE tokens...")
    ws_thread = threading.Thread(target=ws_mgr.connect, args=(all_tokens_nfo, all_tokens_nse))
    ws_thread.daemon = True
    ws_thread.start()
    logger.info("WebSocket thread started.")

logger.info("Starting Keep-Alive thread...")
keep_alive_thread = threading.Thread(target=keep_alive.run)
keep_alive_thread.daemon = True
keep_alive_thread.start()

logger.info(f"Scheduling analysis job every {config.IV_FETCH_INTERVAL} minute(s)...")
schedule.every(config.IV_FETCH_INTERVAL).minutes.do(job)
send_alert("🤖 Scanner Started Successfully!")
logger.info("Scanner Initialization Complete. Entering main loop...")
while True:
    schedule.run_pending()
    time.sleep(1)
