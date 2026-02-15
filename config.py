import os
import sys
from dotenv import load_dotenv
import logzero
from logzero import logger

# Configure logging
logzero.logfile("angel_scanner.log", maxBytes=1000000, backupCount=3)
formatter = logzero.LogFormatter(fmt='[%(asctime)s] %(module)s:%(lineno)d %(levelname)s:%(message)s')
logzero.formatter(formatter)

# Load .env file from the current directory
base_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(base_dir, '.env')
load_dotenv(env_path)

# --- ANGEL ONE CREDENTIALS ---
API_KEY = os.getenv('ANGELONE_API_KEY')
CLIENT_ID = os.getenv('ANGELONE_CLIENT_ID')
PASSWORD = os.getenv('ANGELONE_PASSWORD') or os.getenv('MPIN')
TOTP_SECRET = os.getenv('ANGELONE_TOTP_SECRET')

# --- TELEGRAM SETTINGS ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- STRATEGY CONFIG ---
SYMBOLS = os.getenv('SYMBOLS', 'NIFTY,BANKNIFTY').split(',')
try:
    IV_ROC_THRESHOLD = float(os.getenv('IV_ROC_THRESHOLD', 2.0))
    OI_ROC_THRESHOLD = float(os.getenv('OI_ROC_THRESHOLD', 5.0))
    IV_FETCH_INTERVAL = int(os.getenv('IV_FETCH_INTERVAL', 1))  # Minutes
    OI_FETCH_INTERVAL = int(os.getenv('OI_FETCH_INTERVAL', 1))  # Minutes
    MIN_OI_FOR_SIGNAL = int(os.getenv('MIN_OI_FOR_SIGNAL', 10000))  # Minimum OI to consider
    WEBSOCKET_HEALTH_CHECK = int(os.getenv('WEBSOCKET_HEALTH_CHECK', 30))  # Seconds
except ValueError:
    logger.error("Error reading numeric config. Using defaults.")
    IV_ROC_THRESHOLD = 2.0
    OI_ROC_THRESHOLD = 5.0
    IV_FETCH_INTERVAL = 5
    OI_FETCH_INTERVAL = 1
    MIN_OI_FOR_SIGNAL = 10000
    WEBSOCKET_HEALTH_CHECK = 30

# Check for missing credentials
if not all([API_KEY, CLIENT_ID, PASSWORD, TOTP_SECRET]):
    logger.error("Missing Angel One credentials in .env file.")
    sys.exit(1)

# API Endpoints
BASE_URL = "https://apiconnect.angelbroking.com"
OPTION_GREEKS_URL = f"{BASE_URL}/rest/secure/angelbroking/marketData/v1/optionGreek"
INSTRUMENT_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

logger.info("Configuration loaded successfully.")
