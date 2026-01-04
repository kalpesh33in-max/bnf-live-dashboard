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
import queue
from io import BytesIO

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

def is_trading_day_and_hours():
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    # Check if it's a weekday (Monday=0, Friday=4)
    if not (0 <= now.weekday() <= 4):
        return False

    # Check if current time is within trading hours (9:15 AM to 3:30 PM)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    return market_open <= now <= market_close

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

def convert_df_to_csv(df):
    return df.to_csv(index=True).encode('utf-8')

def convert_df_to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=True, sheet_name='Sheet1')
    writer.close()
    processed_data = output.getvalue()
    return processed_data

# ==============================================================================
# ============================== CONFIGURATION =================================
# ==============================================================================

data_queue = queue.Queue()

DATA_DIR = "bnf_data"
os.makedirs(DATA_DIR, exist_ok=True)

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
st.subheader(f"Data for: {datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%d %B %Y')}")

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
    today_date_str = datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%Y-%m-%d')
    history_file_path = os.path.join(DATA_DIR, f"history_{today_date_str}.csv")

    if is_trading_day_and_hours() and os.path.exists(history_file_path):
        try:
            st.session_state.history_df = pd.read_csv(history_file_path, index_col=0)
            # Ensure index is datetime type if needed, or just keep as string
        except Exception as e:
            st.warning(f"Could not load historical data from {history_file_path}: {e}")
            st.session_state.history_df = pd.DataFrame(columns=[f"{s} {t.lower()}" for s in STRIKE_RANGE for t in ["ce", "pe"]])
    else:
        st.session_state.history_df = pd.DataFrame(columns=[f"{s} {t.lower()}" for s in STRIKE_RANGE for t in ["ce", "pe"]])
if 'atm_strike' not in st.session_state:
    st.session_state.atm_strike = 60100
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = "N/A"
if 'last_history_update_time' not in st.session_state:
    st.session_state.last_history_update_time = datetime.min.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
if 'last_save_time' not in st.session_state:
    st.session_state.last_save_time = datetime.min.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
if 'last_rerun_time' not in st.session_state:
    st.session_state.last_rerun_time = datetime.min.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

# ==============================================================================
# ======================= BACKGROUND DATA UPDATER ==============================
# ==============================================================================

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
                    data_queue.put(data)
    except Exception as e:
        # In a real app, you'd want more robust error handling/logging
        print(f"WebSocket Error: {e}")

def run_background_tasks():
    """Starts the asyncio event loop in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(listen_to_gdfl())

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

    future_price_col, atm_col, last_update_col, download_csv_col, download_xlsx_col = st.columns(5)
    future_price_col.metric("BNF Future Price", f"{st.session_state.future_price:.2f}")
    atm_col.metric("Selected ATM", st.session_state.atm_strike)
    last_update_col.info(f"Last updated: {st.session_state.last_update_time}")

    csv_data = convert_df_to_csv(st.session_state.history_df)
    download_csv_col.download_button(
        label="Download CSV",
        data=csv_data,
        file_name=f"BNF_OI_Dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="download_csv"
    )

    xlsx_data = convert_df_to_excel(st.session_state.history_df)
    download_xlsx_col.download_button(
        label="Download XLSX",
        data=xlsx_data,
        file_name=f"BNF_OI_Dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_xlsx"
    )

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

def process_queued_data():
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    # Process all available items in the queue
    data_updated = False
    while not data_queue.empty():
        data = data_queue.get()
        symbol = data.get("InstrumentIdentifier")
        if symbol:
            if symbol in st.session_state.live_data:
                new_oi = data.get("OpenInterest")
                if new_oi is not None:
                    st.session_state.live_data[symbol]["oi"] = new_oi
                    data_updated = True

            if "FUT" in symbol:
                new_price = data.get("LastTradePrice")
                if new_price is not None:
                    st.session_state.future_price = new_price
                    data_updated = True

    # Check if 60 seconds have passed for history_df update
    default_aware_datetime_min = datetime.min.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    if (now - st.session_state.get('last_history_update_time', default_aware_datetime_min)).total_seconds() >= 60:
        st.session_state.past_data = st.session_state.live_data.copy() # Capture current live_data as past_data

        new_row = {}
        for symbol in ALL_OPTION_SYMBOLS:
            live_oi = st.session_state.live_data.get(symbol, {}).get("oi", 0)
            past_oi = st.session_state.past_data.get(symbol, {}).get("oi", 0) # Use the captured past_data

            oi_roc = 0.0
            if past_oi > 0:
                oi_roc = ((live_oi - past_oi) / past_oi) * 100 # Corrected to multiply by 100

            strike_col_name = extract_strike_and_type(symbol)
            if strike_col_name:
                new_row[strike_col_name] = f"{oi_roc:.2f}%"

        if new_row:
            new_df_row = pd.DataFrame([new_row], index=[get_current_time()])
            st.session_state.history_df = pd.concat([st.session_state.history_df, new_df_row])
            st.session_state.last_update_time = get_current_time()
            st.session_state.last_history_update_time = now # Update the timestamp for history_df

    # Periodic saving of history_df to file
    if is_trading_day_and_hours() and (now - st.session_state.get('last_save_time', datetime.min.replace(tzinfo=ZoneInfo("Asia/Kolkata")))).total_seconds() >= 30:
        today_date_str = now.strftime('%Y-%m-%d')
        history_file_path = os.path.join(DATA_DIR, f"history_{today_date_str}.csv")
        try:
            st.session_state.history_df.to_csv(history_file_path)
            st.session_state.last_save_time = now
        except Exception as e:
            st.error(f"Error saving historical data to {history_file_path}: {e}")

    # Conditional st.rerun() to auto-refresh the dashboard
    if data_updated and (now - st.session_state.get('last_rerun_time', default_aware_datetime_min)).total_seconds() >= 5: # Rerun every 5 seconds if there's new data
        st.session_state.last_rerun_time = now
        st.rerun()


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

process_queued_data() # Call the new function to process updates
draw_dashboard()
