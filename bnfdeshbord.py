import streamlit as st
import pandas as pd
import asyncio
import websockets
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import re

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

# --- Streamlit Page Configuration ---
st.set_page_config(
    page_title="Bank Nifty OI Dashboard",
    layout="wide"
)

# --- App Title ---
st.title("🚀 Bank Nifty Live OI RoC% Dashboard")

# --- GDFL Configuration ---
# Load credentials securely from environment variables
# Make sure to set these in your Railway deployment environment
API_KEY = os.environ.get("API_KEY", "YOUR_API_KEY") 
WSS_URL = "wss://nimblewebstream.lisuns.com:4576/"

# --- Symbols to Monitor (Copied from gfdl_scanner.py for BANKNIFTY only) ---
SYMBOLS_TO_MONITOR = [
    # BANKNIFTY Options
    "BANKNIFTY27JAN2660100CE", "BANKNIFTY27JAN2660100PE", "BANKNIFTY27JAN2660000CE", "BANKNIFTY27JAN2660000PE",
    "BANKNIFTY27JAN2659900CE", "BANKNIFTY27JAN2659900PE", "BANKNIFTY27JAN2659800CE", "BANKNIFTY27JAN2659800PE",
    "BANKNIFTY27JAN2659700CE", "BANKNIFTY27JAN2659700PE", "BANKNIFTY27JAN2659600CE", "BANKNIFTY27JAN2659600PE",
    "BANKNIFTY27JAN2660200CE", "BANKNIFTY27JAN2660200PE", "BANKNIFTY27JAN2660300CE", "BANKNIFTY27JAN2660300PE",
    "BANKNIFTY27JAN2660400CE", "BANKNIFTY27JAN2660400PE", "BANKNIFTY27JAN2660500CE", "BANKNIFTY27JAN2660500PE",
    "BANKNIFTY27JAN2660600CE", "BANKNIFTY27JAN2660600PE",
    # Future for price
    "BANKNIFTY27JAN26FUT",
]

# --- Global State Management ---
# Using Streamlit's session state to persist data across reruns
if 'live_data' not in st.session_state:
    st.session_state.live_data = {symbol: {"oi": 0} for symbol in SYMBOLS_TO_MONITOR}
if 'past_data' not in st.session_state:
    st.session_state.past_data = st.session_state.live_data.copy()
if 'future_price' not in st.session_state:
    st.session_state.future_price = 0
if 'dashboard_df' not in st.session_state:
    # Initialize an empty DataFrame with correct columns based on PDF
    columns = sorted([
        "59700 ce", "59800 ce", "59900 ce", "60000 ce", "60100 ce",
        "60100 pe", "60200 pe", "60300 pe", "60400 pe", "60500 pe", "60600 pe"
    ])
    st.session_state.dashboard_df = pd.DataFrame(columns=columns)

# ==============================================================================
# ============================ HELPER FUNCTIONS ================================
# ==============================================================================

def get_current_time():
    """Gets the current time in Asia/Kolkata timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

def extract_strike_and_type(symbol):
    """Extracts strike and type from symbol string."""
    # Example: BANKNIFTY27JAN2660100CE -> "60100 ce"
    match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)$', symbol)
    if match:
        strike = match.group(1)
        opt_type = match.group(2).lower()
        return f"{strike} {opt_type}"
    return None

def style_dashboard(df, future_price):
    """Applies vibrant color coding based on moneyness."""
    if future_price == 0:
        return df.style

    def moneyness_styler(df_to_style: pd.DataFrame):
        df_style = pd.DataFrame('', index=df_to_style.index, columns=df_to_style.columns)
        atm_band = future_price * 0.005
        
        for col_name in df_to_style.columns:
            try:
                # Handle base and ATM-labeled columns
                cleaned_col_name = col_name.replace(' (ATM)', '')
                parts = cleaned_col_name.split()
                strike = float(parts[0])
                opt_type = parts[1]
            except (ValueError, IndexError):
                continue

            style = ''
            if abs(strike - future_price) <= atm_band:
                style = 'background-color: #ffff66' # Vibrant Yellow
            elif opt_type == 'ce' and strike < future_price: # ITM Call
                style = 'background-color: #66ff66' # Vibrant Green
            elif opt_type == 'pe' and strike > future_price: # ITM Put
                style = 'background-color: #ff6666' # Vibrant Red/Coral
            
            if style:
                df_style[col_name] = style
        return df_style

    return df.style.apply(moneyness_styler, axis=None)

# ==============================================================================
# ============================ STREAMLIT LAYOUT ================================
# ==============================================================================

# --- Placeholders for live data ---
st.info("Dashboard will update every 1 minute. Please wait for the first data to arrive...")
future_price_col, last_update_col = st.columns(2)
future_price_placeholder = future_price_col.empty()
last_update_placeholder = last_update_col.empty()
data_placeholder = st.empty()

# ==============================================================================
# ======================= DATA PROCESSING & WEB SOCKET =========================
# ==============================================================================

async def update_dashboard():
    """Calculates OI RoC and updates the Streamlit dashboard."""
    await asyncio.sleep(5) # Small initial sleep
    
    while True:
        st.session_state.past_data = st.session_state.live_data.copy()
        await asyncio.sleep(60) 
        
        new_row = {}
        for symbol in SYMBOLS_TO_MONITOR:
            if "FUT" in symbol:
                continue

            live_oi = st.session_state.live_data.get(symbol, {}).get("oi", 0)
            past_oi = st.session_state.past_data.get(symbol, {}).get("oi", 0)
            
            oi_roc = 0.0
            if past_oi > 0:
                oi_roc = ((live_oi - past_oi) / past_oi) * 100
            
            strike_col_name = extract_strike_and_type(symbol)
            if strike_col_name in st.session_state.dashboard_df.columns:
                new_row[strike_col_name] = f"{oi_roc:.2f}%"

        new_df_row = pd.DataFrame([new_row], index=[get_current_time()])
        st.session_state.dashboard_df = pd.concat([st.session_state.dashboard_df, new_df_row])
        
        # --- Prepare DataFrame for Display ---
        df_display = st.session_state.dashboard_df.copy()
        
        # Ensure new time is on top
        df_display = df_display.sort_index(ascending=False)
        df_display = df_display.head(20) # Limit entries

        # Find and rename ATM column header for display
        future_price = st.session_state.future_price
        if future_price > 0:
            atm_band = future_price * 0.005
            new_cols = {}
            for col in df_display.columns:
                try:
                    strike = float(col.split()[0])
                    if abs(strike - future_price) <= atm_band:
                        new_cols[col] = f"{col} (ATM)"
                except (ValueError, IndexError):
                    continue
            df_display.rename(columns=new_cols, inplace=True)

        # Update Streamlit elements
        future_price_placeholder.metric("Bank Nifty Future Price", f"{st.session_state.future_price:.2f}")
        last_update_placeholder.info(f"Last updated: {get_current_time()}")
        
        styled_table = style_dashboard(df_display, st.session_state.future_price)
        data_placeholder.dataframe(styled_table)


async def listen_to_gdfl():
    """Connects to GDFL WebSocket and processes live data."""
    try:
        async with websockets.connect(WSS_URL) as websocket:
            auth_request = {"MessageType": "Authenticate", "Password": API_KEY}
            await websocket.send(json.dumps(auth_request))
            auth_response = await websocket.recv()
            
            if not json.loads(auth_response).get("Complete"):
                st.error(f"GDFL Authentication FAILED: {auth_response}")
                return

            for symbol in SYMBOLS_TO_MONITOR:
                await websocket.send(json.dumps({
                    "MessageType": "SubscribeRealtime", "Exchange": "NFO",
                    "Unsubscribe": "false", "InstrumentIdentifier": symbol
                }))

            async for message in websocket:
                data = json.loads(message)
                if data.get("MessageType") == "RealtimeResult":
                    symbol = data.get("InstrumentIdentifier")
                    if symbol in st.session_state.live_data:
                        # Update OI
                        new_oi = data.get("OpenInterest")
                        if new_oi is not None:
                            st.session_state.live_data[symbol]["oi"] = new_oi
                        
                        # Update Future Price if it's the future symbol
                        if "FUT" in symbol:
                            new_price = data.get("LastTradePrice")
                            if new_price is not None:
                                st.session_state.future_price = new_price

    except Exception as e:
        st.error(f"An error occurred: {e}")

# ==============================================================================
# ============================ MAIN EXECUTION ==================================
# ==============================================================================

async def main():
    """Runs the WebSocket listener and the dashboard updater concurrently."""
    await asyncio.gather(
        listen_to_gdfl(),
        update_dashboard()
    )

if __name__ == "__main__":
    if API_KEY == "YOUR_API_KEY":
        st.warning("Please set the `API_KEY` environment variable for your GDFL feed.")
    else:
        try:
            asyncio.run(main())
        except Exception as e:
            st.error(f"Failed to start the application: {e}")
