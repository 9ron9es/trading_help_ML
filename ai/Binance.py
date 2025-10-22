



import os
import pandas as pd
from dotenv import load_dotenv
from binance import ThreadedWebsocketManager

load_dotenv()
api = os.getenv("binance_api")
secret = os.getenv("binance_secret")

twm = ThreadedWebsocketManager(api_key=api, api_secret=secret)
twm.start()

columns = ["event_time", "symbol", "open", "high", "low", "close", "volume_base", "volume_quote"]
file_path = "crypto.csv"

if not os.path.exists(file_path):
    pd.DataFrame(columns=columns).to_csv(file_path, index=False)

major_cryptos = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LTCUSDT", "ADAUSDT", "SOLUSDT", "DOGEUSDT"]

headers_printed = False

def _valid_price(v):
    try:
        return float(v) > 0.0
    except Exception:
        return False

def handle_msg(msg):
    global headers_printed
    try:
        o = msg.get('o') or msg.get('O') or msg.get('open')
        h = msg.get('h') or msg.get('H') or msg.get('high')
        l = msg.get('l') or msg.get('L') or msg.get('low')
        c = msg.get('c') or msg.get('C') or msg.get('close')
        v = msg.get('v') or msg.get('V') or msg.get('volume')
        q = msg.get('q') or msg.get('Q') or msg.get('quoteVolume')

        
        if not (_valid_price(o) and _valid_price(h) and _valid_price(l) and _valid_price(c)):
            
            print("Skipping message with invalid/non-positive OHLC:", msg.get('s'))
            return

        event_ts = pd.to_datetime(msg['E'], unit='ms', utc=True).isoformat()

        row = [
            event_ts,                   
            msg.get('s'),               
            float(o),                   
            float(h),                   
            float(l),                   
            float(c),                   
            float(v) if v is not None else 0.0,
            float(q) if q is not None else 0.0
        ]

        pd.DataFrame([row], columns=columns).to_csv(file_path, mode='a', header=False, index=False)

        if not headers_printed:
            print("  ".join(columns))
            headers_printed = True

        print("  ".join(str(x) for x in row))
    except Exception as e:
        print("Error handling msg:", e)

for symbol in major_cryptos:
    twm.start_symbol_ticker_socket(callback=handle_msg, symbol=symbol)

twm.join()