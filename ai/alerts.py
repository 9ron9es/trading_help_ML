from datetime import datetime

def send_console_alert(message, level='info'):
    """Print timestamped console alert. level: info, warn, critical"""
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    level = level.upper() if level else 'INFO'
    print(f"[{ts}] [{level}] {message}")