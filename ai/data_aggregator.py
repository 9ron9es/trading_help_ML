import pandas as pd
import os
from datetime import datetime, timedelta

def load_crypto_data(lookback_hours=24):
    if not os.path.exists('crypto.csv'):
        return pd.DataFrame()
    
    df = pd.read_csv('crypto.csv')
    df['event_time'] = pd.to_datetime(df['event_time'])
    
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    df = df[df['event_time'] > cutoff]
    
    return df

def load_stock_data(lookback_hours=8):
    if not os.path.exists('stocks.csv'):
        return pd.DataFrame()
    
    df = pd.read_csv('stocks.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    cutoff = datetime.now() - timedelta(hours=lookback_hours)
    df = df[df['timestamp'] > cutoff]
    
    return df

def load_forex_data(lookback_hours=24):
    if not os.path.exists('forex.csv'):
        return pd.DataFrame()
    
    df = pd.read_csv('forex.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    cutoff = datetime.now() - timedelta(hours=lookback_hours)
    df = df[df['timestamp'] > cutoff]
    
    return df

def load_sentiment_data():
    if not os.path.exists('sentiment.csv'):
        return pd.DataFrame()
    
    df = pd.read_csv('sentiment.csv')
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df['date'] = df['publishedAt'].dt.date
    
    daily_sentiment = df.groupby('date')['sentiment_score'].mean().reset_index()
    
    return daily_sentiment

def get_latest_prices():
    prices = {}
    
    crypto_df = load_crypto_data(lookback_hours=2)
    if not crypto_df.empty:
        latest_crypto = crypto_df.iloc[-1]
        prices['crypto'] = {
            'symbol': latest_crypto.get('symbol', 'CRYPTO'),
            'price': latest_crypto.get('close', 0),
            'volume_base': latest_crypto.get('volume_base', 0),
            'volume_quote': latest_crypto.get('volume_quote', 0),
            'timestamp': latest_crypto.get('event_time')
        }
    
    stock_df = load_stock_data(lookback_hours=2)
    if not stock_df.empty:
        prices['stocks'] = []
        for ticker in stock_df['ticker'].unique():
            ticker_data = stock_df[stock_df['ticker'] == ticker].iloc[-1]
            prices['stocks'].append({
                'ticker': ticker,
                'price': ticker_data.get('close', 0),
                'volume': ticker_data.get('volume', 0),
                'timestamp': ticker_data.get('timestamp')
            })
    
    forex_df = load_forex_data(lookback_hours=2)
    if not forex_df.empty:
        prices['forex'] = []
        for pair in forex_df['pair'].unique():
            pair_data = forex_df[forex_df['pair'] == pair].iloc[-1]
            prices['forex'].append({
                'pair': pair,
                'rate': pair_data.get('rate', 0),
                'timestamp': pair_data.get('timestamp')
            })
    
    return prices

def get_data_summary():
    summary = {
        'crypto': {'exists': os.path.exists('crypto.csv'), 'records': 0},
        'stocks': {'exists': os.path.exists('stocks.csv'), 'records': 0},
        'forex': {'exists': os.path.exists('forex.csv'), 'records': 0},
        'sentiment': {'exists': os.path.exists('sentiment.csv'), 'records': 0}
    }
    
    if summary['crypto']['exists']:
        summary['crypto']['records'] = len(pd.read_csv('crypto.csv'))
    
    if summary['stocks']['exists']:
        summary['stocks']['records'] = len(pd.read_csv('stocks.csv'))
    
    if summary['forex']['exists']:
        summary['forex']['records'] = len(pd.read_csv('forex.csv'))
    
    if summary['sentiment']['exists']:
        summary['sentiment']['records'] = len(pd.read_csv('sentiment.csv'))
    
    return summary
