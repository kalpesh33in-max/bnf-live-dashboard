import streamlit as st
import pandas as pd
import asyncio
import websockets
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import os

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
    columns = [
        "59700 ce", "59800 ce", "59900 ce", "60000 ce", "60100 ce",
        "60100 pe", "60200 pe", "60300 pe", "60400 pe",
    ]
    st.session_state.dashboard_df = pd.DataFrame(columns=columns)

# ==============================================================================
# ============================ HELPER FUNCTIONS ================================
# ==============================================================================

def get_current_time():
    """Gets the current time in Asia/Kolkata timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S")

def extract_strike_and_type(symbol):
    """Extracts strike and type from symbol string."""
    # Example: BANKNIFTY27JAN2660100CE -> (60100, ce)
    match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d+)(CE|PE)$', symbol)
    if match:
        strike = match.group(1)
        opt_type = match.group(2).lower()
        return f"{strike} {opt_type}"
    return None

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
    while True:
        await asyncio.sleep(60) # Wait for 1 minute
        
        # Copy live data to past data for calculation
        st.session_state.past_data = st.session_state.live_data.copy()
        
        new_row = {}
        for symbol in SYMBOLS_TO_MONITOR:
            if "FUT" in symbol:
                continue

            live_oi = st.session_state.live_data.get(symbol, {}).get("oi", 0)
            past_oi = st.session_state.past_data.get(symbol, {}).get("oi", 0)
            
            oi_roc = 0.0
            if past_oi > 0:
                oi_change = live_oi - past_oi
                oi_roc = (oi_change / past_oi) * 100
            
            strike_col_name = extract_strike_and_type(symbol)
            if strike_col_name in st.session_state.dashboard_df.columns:
                new_row[strike_col_name] = f"{oi_roc:.2f}%"

        # Create a new DataFrame for the new row of data
        new_df_row = pd.DataFrame([new_row], index=[get_current_time()])

        # Prepend the new row to the main DataFrame
        st.session_state.dashboard_df = pd.concat([new_df_row, st.session_state.dashboard_df])
        
        # Limit the dashboard to the last 20 entries
        st.session_state.dashboard_df = st.session_state.dashboard_df.head(20)

        # Update Streamlit elements
        future_price_placeholder.metric("Bank Nifty Future Price", f"{st.session_state.future_price:.2f}")
        last_update_placeholder.info(f"Last updated: {get_current_time()}")
        data_placeholder.dataframe(st.session_state.dashboard_df)


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
