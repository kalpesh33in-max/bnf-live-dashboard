import requests
import config
from logzero import logger

def send_alert(message):
    """
    Send a message to the configured Telegram chat.
    """
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        logger.warning("Telegram Bot Token or Chat ID missing. Alert NOT sent.")
        return False
    
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Format message for better readability
    formatted_message = message
    
    # Handle both string messages and signal dictionaries
    if isinstance(message, dict):
        if 'signal' in message:
            # Format signal message
            emoji = "🟢" if message['signal'] == "BUY" else "🔴" if message['signal'] == "CONSIDER_SELL" else "⚠️"
            formatted_message = (
                f"{emoji} *{message['signal']} SIGNAL* {emoji}\n"
                f"📊 *{message['symbol']}* {message['strike']} {message['type']}\n"
                f"📝 Reason: {message['reason']}\n"
                f"📈 IV ROC: {message['iv_roc']}%\n"
                f"📉 OI ROC: {message['oi_roc']}%\n"
                f"💹 Confidence: {message.get('confidence', 0)}%\n"
                f"💰 LTP: {message.get('price', 0)}\n"
                f"⚡ OI: {message.get('oi_current', 0)}"
            )
    
    payload = {
        'chat_id': config.TELEGRAM_CHAT_ID,
        'text': formatted_message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Telegram alert sent")
        return True
    except requests.exceptions.Timeout:
        logger.error("Telegram API timeout")
        return False
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {str(e)}")
        return False

def send_summary(summary):
    """Send market summary via Telegram"""
    if not summary:
        return
        
    msg = (
        f"📊 *Market Summary - {summary['symbol']}*\n"
        f"⏰ {summary['timestamp'].strftime('%H:%M:%S')}\n\n"
        f"📈 Avg IV ROC: {summary['avg_iv_roc']}%\n"
        f"📉 Avg OI ROC: {summary['avg_oi_roc']}%\n"
        f"⬆️ Max IV ROC: {summary['max_iv_roc']}%\n"
        f"⬇️ Min IV ROC: {summary['min_iv_roc']}%\n"
        f"Active Strikes: {summary['total_active_strikes']}"
    )
    
    send_alert(msg)
