"""
Real-Time Intraday Stock Data Display

This module fetches near real-time intraday stock data for a set of major companies 
using the yfinance library and displays it in a clean, organized table format. 
It is intended to be integrated as part of a larger project.

Features:
- Supports multiple stock tickers.
- Fetches OHLCV data (Open, High, Low, Close, Volume) at 1-minute intervals.
- Clears and refreshes the console for a readable live display.
- Shows the timestamp of the latest update.
- Can be used as a reusable component inside other scripts.

Configuration:
- TICKERS: List of stock tickers to monitor.
- INTERVAL: Granularity of intraday data (e.g., "1m" for 1-minute candles).
- UPDATE_DELAY: Seconds between updates.
"""


import yfinance as yf
import pandas as pd
import datetime as dt
import os
import time

TICKERS = [
    "AAPL", "MSFT", "GOOG", "GOOGL", "AMZN",
    "META", "TSLA", "NVDA", "BRK-B", "JPM",
    "JNJ", "V", "WMT"
]

INTERVAL = "1m"
UPDATE_DELAY = 5
VOLUME_AGGREGATE_MINUTES = 15

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def fetch_latest_data(tickers, interval):
    today = dt.date.today().isoformat()
    return yf.download(tickers, start=today, interval=interval, group_by="ticker", progress=False, auto_adjust=True, prepost=True)

def save_to_csv(data, tickers, filename='stocks.csv'):
    stock_records = []
    timestamp = dt.datetime.utcnow().isoformat()  
    
    for ticker in tickers:
        if ticker in data and not data[ticker].empty:
            ticker_data = data[ticker]
            latest = ticker_data.iloc[-1]
            
            recent_data = ticker_data.tail(VOLUME_AGGREGATE_MINUTES)
            total_volume = recent_data['Volume'].sum() if 'Volume' in recent_data.columns else 0
            
            open_v = latest.get('Open', pd.NA)
            high_v = latest.get('High', pd.NA)
            low_v = latest.get('Low', pd.NA)
            close_v = latest.get('Close', pd.NA)
            
            
            open_v = open_v if pd.notna(open_v) else pd.NA
            high_v = high_v if pd.notna(high_v) else pd.NA
            low_v = low_v if pd.notna(low_v) else pd.NA
            close_v = close_v if pd.notna(close_v) else pd.NA
            
            
            try:
                if close_v is pd.NA or float(close_v) <= 0:
                    
                    continue
            except Exception:
                continue
            
            stock_records.append({
                'timestamp': timestamp,
                'ticker': ticker,
                'open': open_v,
                'high': high_v,
                'low': low_v,
                'close': close_v,
                'volume': int(total_volume) if pd.notna(total_volume) and total_volume > 0 else 0
            })
    
    if not stock_records:
        return None
    
    df = pd.DataFrame(stock_records)
    
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, low_memory=False)
        df = pd.concat([existing_df, df], ignore_index=True, sort=False)
    
    
    cols = ['timestamp','ticker','open','high','low','close','volume']
    df = df.loc[:, [c for c in cols if c in df.columns]]
    df.to_csv(filename, index=False)
    return df

def display_latest_data(data, tickers):
    clear_console()
    
    print("=" * 100)
    print(f"{'REAL-TIME STOCK MARKET DATA':^100}")
    print("=" * 100)
    print()
    print(f"{'Ticker':<8} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume (15min)':>18}")
    print("-" * 100)
    
    for ticker in tickers:
        if ticker in data and not data[ticker].empty:
            ticker_data = data[ticker]
            
            latest = ticker_data.iloc[-1]
            open_price = latest['Open'] if pd.notna(latest['Open']) else 0
            high_price = latest['High'] if pd.notna(latest['High']) else 0
            low_price = latest['Low'] if pd.notna(latest['Low']) else 0
            close_price = latest['Close'] if pd.notna(latest['Close']) else 0
            
            recent_data = ticker_data.tail(VOLUME_AGGREGATE_MINUTES)
            total_volume = recent_data['Volume'].sum() if 'Volume' in recent_data.columns else 0
            
            if pd.isna(total_volume):
                total_volume = 0
            
            volume_str = f"{int(total_volume):,}" if total_volume > 0 else "N/A"
            
            print(f"{ticker:<8} "
                  f"{open_price:>10.2f} "
                  f"{high_price:>10.2f} "
                  f"{low_price:>10.2f} "
                  f"{close_price:>10.2f} "
                  f"{volume_str:>18}")
        else:
            print(f"{ticker:<8} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>10} {'N/A':>18}")
    
    print("-" * 100)
    print(f"\nLast Updated: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Volume shown is aggregated over the last {VOLUME_AGGREGATE_MINUTES} minutes")
    print("=" * 100)
    
    
    try:
        save_to_csv(data, tickers)
    except Exception:
        pass

if __name__ == "__main__":
    while True:
        try:
            data = fetch_latest_data(TICKERS, INTERVAL)
            display_latest_data(data, TICKERS)
            time.sleep(UPDATE_DELAY)
        except KeyboardInterrupt:
            break