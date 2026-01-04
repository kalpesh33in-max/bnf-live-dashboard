import streamlit as st
import pandas as pd
import asyncio
import websockets
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import re
import threading

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

st.set_page_config(page_title="Bank Nifty OI Dashboard", layout="wide")

st.markdown("""
    <style>
    .stDataFrame th, .stDataFrame td {
        max-width: 100px;
        min-width: 75px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🚀 Bank Nifty Interactive OI Dashboard")

API_KEY = os.environ.get("API_KEY", "YOUR_API_KEY") 
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"

STRIKE_RANGE = range(59000, 61001, 100)
EXPIRY_PREFIX = "BANKNIFTY27JAN26"

ALL_OPTION_SYMBOLS = [f"{EXPIRY_PREFIX}{strike}{opt_type}" for strike in STRIKE_RANGE for opt_type in ["CE", "PE"]]
SYMBOLS_TO_MONITOR = ALL_OPTION_SYMBOLS + [f"{EXPIRY_PREFIX}FUT"]

# ==============================================================================
# ============================ SESSION STATE INIT ==============================
# ==============================================================================

if 'live_data' not in st.session_state:
    st.session_state.live_data = {symbol: {"oi": 0} for symbol in SYMBOLS_TO_MONITOR}
if 'past_data' not in st.session_state:
    st.session_state.past_data = st.session_state.live_data.copy()
if 'future_price' not in st.session_state:
    st.session_state.future_price = 0.0
if 'history_df' not in st.session_state:
    all_cols = sorted([f"{s} {t.lower()}" for s in STRIKE_RANGE for t in ["ce", "pe"]])
    st.session_state.history_df = pd.DataFrame(columns=all_cols)
if 'atm_strike' not in st.session_state:
    st.session_state.atm_strike = 60100
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = "N/A"

# ==============================================================================
# ============================ HELPER FUNCTIONS ================================
# ==============================================================================

def get_current_time():
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

def extract_strike_and_type(symbol):
    match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)$', symbol)
    if match:
        return f"{match.group(1)} {match.group(2).lower()}"
    return None

def style_dashboard(df, selected_atm):
    def moneyness_styler(df_to_style: pd.DataFrame):
        df_style = pd.DataFrame('', index=df_to_style.index, columns=df_to_style.columns)
        for col_name in df_to_style.columns:
            try:
                strike = float(col_name.split()[0])
                opt_type = col_name.split()[1]
            except (ValueError, IndexError):
                continue
            style = 'color: black; font-weight: bold;'
            if strike == selected_atm:
                style += 'background-color: khaki;'
            elif opt_type == 'ce' and strike < selected_atm:
                style += 'background-color: palegreen;'
            elif opt_type == 'pe' and strike > selected_atm:
                style += 'background-color: lightsalmon;'
            df_style[col_name] = style
        return df_style
    return df.style.apply(moneyness_styler, axis=None)

# ==============================================================================
# ======================= BACKGROUND DATA UPDATER ==============================
# ==============================================================================

async def data_updater_task():
    """BACKGROUND TASK: Updates the master history DataFrame every 60 seconds."""
    await asyncio.sleep(10) # Initial delay
    while True:
        st.session_state.past_data = st.session_state.live_data.copy()
        await asyncio.sleep(60)
        
        new_row = {}
        for symbol in ALL_OPTION_SYMBOLS:
            live_oi = st.session_state.live_data.get(symbol, {}).get("oi", 0)
            past_oi = st.session_state.past_data.get(symbol, {}).get("oi", 0)
            
            oi_roc = 0.0
            if past_oi > 0:
                oi_roc = ((live_oi - past_oi) / past_oi) * 100
            
            strike_col_name = extract_strike_and_type(symbol)
            if strike_col_name:
                new_row[strike_col_name] = f"{oi_roc:.2f}%"

        if new_row:
            new_df_row = pd.DataFrame([new_row], index=[get_current_time()])
            st.session_state.history_df = pd.concat([st.session_state.history_df, new_df_row])
            st.session_state.last_update_time = get_current_time()

async def listen_to_gdfl():
    """BACKGROUND TASK: Connects to GDFL WebSocket and processes live data."""
    try:
        async with websockets.connect(WSS_URL) as websocket:
            await websocket.send(json.dumps({"MessageType": "Authenticate", "Password": API_KEY}))
            auth_response = await websocket.recv()
            if not json.loads(auth_response).get("Complete"): return

            for symbol in SYMBOLS_TO_MONITOR:
                await websocket.send(json.dumps({"MessageType": "SubscribeRealtime", "Exchange": "NFO", "Unsubscribe": "false", "InstrumentIdentifier": symbol}))

            async for message in websocket:
                data = json.loads(message)
                if data.get("MessageType") == "RealtimeResult":
                    symbol = data.get("InstrumentIdentifier")
                    if symbol and symbol in st.session_state.live_data:
                        new_oi = data.get("OpenInterest")
                        if new_oi is not None:
                            st.session_state.live_data[symbol]["oi"] = new_oi
                        
                        if "FUT" in symbol:
                            new_price = data.get("LastTradePrice")
                            if new_price is not None:
                                st.session_state.future_price = new_price
    except Exception as e:
        # In a real app, you'd want more robust error handling/logging
        print(f"WebSocket Error: {e}")

def run_background_tasks():
    """Starts the asyncio event loop in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncio.gather(listen_to_gdfl(), data_updater_task()))

# ==============================================================================
# ============================ MAIN UI DRAWING =================================
# ==============================================================================

def draw_dashboard():
    """Draws the entire Streamlit UI. Runs on every interaction."""
    st.session_state.atm_strike = st.selectbox(
        'Select Central ATM Strike',
        options=list(STRIKE_RANGE),
        index=list(STRIKE_RANGE).index(st.session_state.get('atm_strike', 60100))
    )

    future_price_col, atm_col, last_update_col = st.columns(3)
    future_price_col.metric("BNF Future Price", f"{st.session_state.future_price:.2f}")
    atm_col.metric("Selected ATM", st.session_state.atm_strike)
    last_update_col.info(f"Last updated: {st.session_state.last_update_time}")

    center_strike = st.session_state.atm_strike
    ce_strikes = [f"{center_strike - i*100} ce" for i in range(5, 0, -1)]
    atm_cols = [f"{center_strike} ce", f"{center_strike} pe"]
    pe_strikes = [f"{center_strike + i*100} pe" for i in range(1, 6)]
    
    display_columns = ce_strikes + atm_cols + pe_strikes
    
    valid_display_columns = [col for col in display_columns if col in st.session_state.history_df.columns]
    
    if not valid_display_columns:
        st.info("Waiting for data to generate table...")
        return
        
    df_display = st.session_state.history_df[valid_display_columns]
    df_display = df_display.sort_index(ascending=False).head(20)

    styled_table = style_dashboard(df_display, center_strike)
    st.dataframe(styled_table)

# ==============================================================================
# ============================ MAIN EXECUTION ==================================
# ==============================================================================

if 'background_tasks_started' not in st.session_state:
    if API_KEY and API_KEY != "YOUR_API_KEY":
        # Run the asyncio event loop in a separate thread
        thread = threading.Thread(target=run_background_tasks, daemon=True)
        thread.start()
        st.session_state.background_tasks_started = True
    else:
        st.warning("Please set the `API_KEY` environment variable for your GDFL feed.")

draw_dashboard()
