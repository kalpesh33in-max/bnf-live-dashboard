from flask import Flask, jsonify
import threading
import os
import time
import json

app = Flask('')

# Store references to components for health check
components = {
    'session': None,
    'ws_mgr': None,
    'data_mgr': None,
    'start_time': time.time()
}

@app.route('/')
def home():
    uptime = time.time() - components['start_time']
    hours = int(uptime // 3600)
    minutes = int((uptime % 3600) // 60)
    
    return jsonify({
        'status': 'alive',
        'service': 'Angel One Options Scanner',
        'uptime': f"{hours}h {minutes}m",
        'timestamp': time.time()
    })

@app.route('/health')
def health():
    """Return status of all components"""
    status = {
        'status': 'healthy',
        'timestamp': time.time(),
        'uptime': time.time() - components['start_time'],
        'components': {}
    }
    
    # Check session
    if components.get('session'):
        status['components']['login'] = components['session'].auth_token is not None
    
    # Check WebSocket
    if components.get('ws_mgr'):
        status['components']['websocket'] = components['ws_mgr'].is_connected()
        status['components']['websocket_data_points'] = len(components['ws_mgr'].latest_data)
    
    # Check Data Manager
    if components.get('data_mgr'):
        status['components']['instruments'] = len(components['data_mgr'].token_map)
    
    # Overall status
    if not all(status['components'].values()):
        status['status'] = 'degraded'
    
    return jsonify(status)

@app.route('/signals')
def get_signals():
    """Get latest signals (if strategy is available)"""
    from main import strat
    if strat:
        signals = []
        for symbol in components.get('symbols', []):
            # This is simplified - you'd need to store latest signals
            pass
        return jsonify({'signals': signals})
    return jsonify({'error': 'Strategy not available'})

def run():
    """Run Flask server"""
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def start_server(session=None, ws_mgr=None, data_mgr=None, symbols=None):
    """Start the keep-alive server with component references"""
    components['session'] = session
    components['ws_mgr'] = ws_mgr
    components['data_mgr'] = data_mgr
    components['symbols'] = symbols
    
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t
