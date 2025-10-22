import os
import pandas as pd
import numpy as np
import time
import json
from datetime import datetime, timedelta
from river import linear_model, preprocessing
import pandas_ta as pta
from dotenv import load_dotenv
from binance import ThreadedWebsocketManager
import yfinance as yf
import requests
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import threading
import sys
from econ_calendar import is_major_event_ongoing
from alerts import send_console_alert
import warnings

warnings.filterwarnings('ignore', category=FutureWarning, message='.*Downcasting object dtype arrays.*')
TRADING_HOURS = {
    'stocks': {'start': '09:30', 'end': '16:00'},  
    'forex': {'start': '00:00', 'end': '23:59'},   
    'crypto': {'start': '00:00', 'end': '23:59'}   
}

RISK_PARAMS = {
    'max_daily_loss_percent': 2.0,
    'max_total_drawdown_percent': 15.0,
    'max_consecutive_losses': 3,
    'max_daily_trades': 10,
    'position_size_percent': 1.0,
    'volatility_adjustment': True
}



load_dotenv()


BINANCE_API = os.getenv("binance_api")
BINANCE_SECRET = os.getenv("binance_secret")
NEWS_API = os.getenv("news_api")
HF_TOKEN = os.getenv("hf_token")
ACCOUNT_BALANCE_START = 10000.0  
RISK_PER_TRADE = 0.01  
SENTIMENT_REFRESH_INTERVAL = 14400  
TRADE_FILE = "trades.json"
TRADE_HISTORY_FILE = "trade_history.json"
ACCOUNT_STATE_FILE = "account_state.json"
UPDATE_INTERVAL = 60  
STOCK_UPDATE_DELAY = 5  
FOREX_UPDATE_INTERVAL = 60  
VOLUME_AGG_MINUTES = 15  
PIP_VALUE_DEFAULT = 0.0001  
REWARD_RISK_RATIO = 2.0  
ATR_MULTIPLIER_SL = 1.5  
MIN_SL_PIPS = 10  
CSV_FILES = {'crypto': 'crypto.csv', 'stocks': 'stocks.csv', 'forex': 'forex.csv'}
SENTIMENT_FILE = 'sentiment.csv'
AUTO_RESET_TRADES = False
AUTO_RESET_INTERVAL = 60
QUERIES = ["forex", "stock market", "gold", "oil", "currency", "dow jones", "nasdaq", "s&p 500"]
ASSETS = ["Apple", "Google", "Microsoft", "Amazon", "Tesla", "Gold", "Oil", "Dow Jones", "NASDAQ", "S&P 500"]
STOCK_TICKERS = ["AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "BRK-B", "JPM", "JNJ", "V", "WMT"]
CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "LTCUSDT", "ADAUSDT", "SOLUSDT", "DOGEUSDT"]
NEWS_API = os.getenv("news_api")
SENTIMENT_FILE = 'sentiment.csv'
running = True
major_forex_pairs = {
    "EUR/USD": {"name": "Euro / US Dollar", "pip_value": 0.0001},
    "GBP/USD": {"name": "British Pound / US Dollar", "pip_value": 0.0001},
    "USD/JPY": {"name": "US Dollar / Japanese Yen", "pip_value": 0.01},
    "USD/CHF": {"name": "US Dollar / Swiss Franc", "pip_value": 0.0001},
    "AUD/USD": {"name": "Australian Dollar / US Dollar", "pip_value": 0.0001},
    "USD/CAD": {"name": "US Dollar / Canadian Dollar", "pip_value": 0.0001},
    "NZD/USD": {"name": "New Zealand Dollar / US Dollar", "pip_value": 0.0001},
    "EUR/GBP": {"name": "Euro / British Pound", "pip_value": 0.0001},
    "EUR/JPY": {"name": "Euro / Japanese Yen", "pip_value": 0.01},
    "GBP/JPY": {"name": "British Pound / Japanese Yen", "pip_value": 0.01}
    
}



class AdvancedRiskManager:
    def __init__(self):
        self.daily_pnl = 0
        self.max_daily_loss = ACCOUNT_BALANCE_START * 0.02  
        self.max_total_drawdown = ACCOUNT_BALANCE_START * 0.15  
        self.consecutive_losses = 0
        self.max_consecutive_losses = 3
        self.positions = {}
        self.daily_trade_count = 0
        self.max_daily_trades = 10
        
    def pre_trade_checks(self, asset_type, symbol, signal, entry_price, sl_pips, volume):
        """Comprehensive pre-trade risk validation"""
        checks = []
        
        
        if self.daily_pnl <= -self.max_daily_loss:
            return False, "Daily loss limit reached"
            
        
        current_balance, total_trades, wins, total_pnl = load_account_state()
        if current_balance <= ACCOUNT_BALANCE_START * 0.85:
            return False, "Maximum account drawdown reached"
            
        
        if self.consecutive_losses >= self.max_consecutive_losses:
            return False, f"Too many consecutive losses ({self.consecutive_losses})"
            
        if self.daily_trade_count >= self.max_daily_trades:
            return False, "Daily trade limit reached"
        risk_amount = current_balance * RISK_PER_TRADE
        if volume <= 0:
            return False, "Invalid position size"
            
        
        if sl_pips < MIN_SL_PIPS:
            return False, f"Stop loss too small ({sl_pips:.1f} pips)"
            
        return True, "All checks passed"
    
    def update_trade_result(self, pnl_usd):
        """Update risk metrics after trade closure"""
        self.daily_pnl += pnl_usd
        self.daily_trade_count += 1
        
        if pnl_usd < 0:
            self.consecutive_losses += 1
        else:
            self.consecutive_losses = 0
            
    def reset_daily(self):
        """Reset daily counters (call at market open)"""
        self.daily_pnl = 0
        self.daily_trade_count = 0
        self.consecutive_losses = 0


risk_manager = AdvancedRiskManager()



class TradingCircuitBreaker:
    def __init__(self):
        self.trading_enabled = True
        self.emergency_stop = False
        self.last_health_check = time.time()
        
    def market_condition_check(self):
        """Check if market conditions are safe for trading"""
        try:
            
            if self._detect_volatility_spike():
                return False, "High volatility period detected"
                
            
            if self._major_news_ongoing():
                return False, "Major economic news event"
                
            
            if not self._sufficient_liquidity():
                return False, "Insufficient market liquidity"
                
            
            if not self._validate_data_quality():
                return False, "Data quality issues detected"
                
            return True, "Market conditions OK"
            
        except Exception as e:
            return False, f"Circuit breaker error: {e}"
    
    def _detect_volatility_spike(self):
        """Detect unusual volatility spikes using ATR"""
        try:
            
            volatility_checks = []
            for asset_type in ['crypto', 'stocks', 'forex']:
                df = load_data(asset_type, 24)  
                if not df.empty:
                    df = apply_ta(df, asset_type)
                    current_atr = df['ATR'].iloc[-1] if 'ATR' in df.columns else 0
                    avg_atr = df['ATR'].mean() if 'ATR' in df.columns else current_atr
                    
                    if avg_atr > 0 and current_atr > (avg_atr * 2.0):
                        volatility_checks.append(True)
            
            return len(volatility_checks) > 1  
        except:
            return False
    
    def _major_news_ongoing(self):
        """Use econ_calendar fallback to detect high-impact events."""
        try:
            if is_major_event_ongoing(window_hours=2):
                try:
                    send_console_alert("Major economic event detected by calendar/fallback", level='warn')
                except Exception:
                    pass
                return True
            return False
        except Exception:
            return False
    def _sufficient_liquidity(self):
        """Basic liquidity check"""
        
        prices = get_latest_prices()
        return len(prices.get('crypto', [])) > 0 and len(prices.get('stocks', [])) > 0
    
    def _validate_data_quality(self):
        """Validate data feed quality"""
        try:
            prices = get_latest_prices()
            for asset_type, data in prices.items():
                if data:
                    for item in data:
                        price = item.get('price') or item.get('rate', 0)
                        if price <= 0 or price > 1000000:  
                            return False
            return True
        except:
            return False
    
    def emergency_stop_trading(self):
            """Immediately halt all trading and attempt an emergency close."""
            try:
                self.emergency_stop = True
                self.trading_enabled = False
                try:
                    self._close_all_positions()
                except Exception as e:
                    print(f" Emergency close failed: {e}")
                try:
                    send_console_alert("EMERGENCY STOP ACTIVATED - ALL TRADING HALTED", level='critical')
                except Exception:
                    print("Emergency alert failed")
            except Exception as ex:
                print(f"emergency_stop_trading error: {ex}")
    def _close_all_positions(self):
        """Emergency close all open positions - FIXED VERSION"""
        try:
            
            try:
                if os.path.exists(TRADE_FILE) and os.path.getsize(TRADE_FILE) > 0:
                    with open(TRADE_FILE, 'r') as f:
                        content = f.read().strip()
                        if content:
                            open_trades = json.loads(content)
                        else:
                            open_trades = []
                else:
                    open_trades = []
            except (json.JSONDecodeError, Exception) as e:
                print(f" Error loading trades for emergency close: {e}")
                open_trades = []
            
            if not open_trades:
                print(" No open trades to close")
                return
                
            prices = get_latest_prices()
            closed_count = 0
            
            for trade in open_trades:
                try:
                    if trade.get('status') != 'open':
                        continue
                        
                    asset_type = trade['asset_type']
                    symbol = trade['symbol']
                    current_price = None
                    
                    
                    price_data = prices.get(asset_type, [])
                    for item in price_data:
                        if (asset_type == 'crypto' and item.get('symbol') == symbol) or \
                        (asset_type == 'stocks' and item.get('ticker') == symbol) or \
                        (asset_type == 'forex' and item.get('pair') == symbol):
                            current_price = item.get('price') or item.get('rate')
                            break
                    
                    if not current_price:
                        print(f" No current price for {symbol}, using entry price")
                        current_price = trade['entry']
                    
                    
                    entry = trade['entry']
                    volume = trade.get('volume', 1)
                    
                    if trade['signal'] == "BUY":
                        pnl = (current_price - entry) * volume
                    else:
                        pnl = (entry - current_price) * volume
                    
                    print(f" EMERGENCY CLOSE: {trade['signal']} {symbol} @ {current_price:.5f}, P&L: ${pnl:.2f}")
                    closed_count += 1
                    
                except Exception as trade_error:
                    print(f" Error closing trade {trade.get('id', 'unknown')}: {trade_error}")
                    continue
            
            
            try:
                save_open_trades([])
                print(f" Emergency close completed: {closed_count} trades closed")
            except Exception as save_error:
                print(f" Error saving empty trades: {save_error}")
                
        except Exception as e:
            print(f" Critical error in emergency close: {e}")


circuit_breaker = TradingCircuitBreaker()
def get_major_pairs():
    return major_forex_pairs

def get_pair_info(pair):
    return major_forex_pairs.get(pair, {"pip_value": PIP_VALUE_DEFAULT, "name": pair})








def start_crypto_stream():
    def crypto_worker():
        global running
        if not BINANCE_API or not BINANCE_SECRET:
            print("Binance API keys missing; using fallback crypto data.")
            
            file_path = CSV_FILES['crypto']
            columns = ["event_time", "symbol", "open", "high", "low", "close", "volume_base", "volume_quote"]
            if not os.path.exists(file_path):
                pd.DataFrame(columns=columns).to_csv(file_path, index=False)
            while running:
                try:
                    
                    now = pd.to_datetime(time.time(), unit='s')
                    dummy_rows = [
                        [now, "BTCUSDT", 60000.0, 60100.0, 59900.0, 60050.0, 100.0, 6005000.0],
                        [now, "ETHUSDT", 3000.0, 3010.0, 2990.0, 3005.0, 200.0, 601000.0]
                    ]
                    for row in dummy_rows:
                        pd.DataFrame([row], columns=columns).to_csv(file_path, mode='a', header=False, index=False)
                    print("Appended fallback crypto rows (BTC/ETH).")
                except Exception as e:
                    print(f"Fallback crypto error: {e}")
                time.sleep(60)  
            return
        
        
        twm = ThreadedWebsocketManager(api_key=BINANCE_API, api_secret=BINANCE_SECRET)
        
        
        latest_data = {}
        
        def handle_socket_message(msg):
            """Handle incoming WebSocket messages from Binance"""
            try:
                if 'data' in msg:
                    stream_data = msg['data']
                    symbol = stream_data['s']  
                    
                    
                    event_time = pd.to_datetime(stream_data['E'], unit='ms')  
                    open_price = float(stream_data['o'])  
                    high_price = float(stream_data['h'])  
                    low_price = float(stream_data['l'])  
                    close_price = float(stream_data['c'])  
                    volume_base = float(stream_data['v'])  
                    volume_quote = float(stream_data['q'])  
                    
                    
                    latest_data[symbol] = {
                        'event_time': event_time,
                        'symbol': symbol,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'volume_base': volume_base,
                        'volume_quote': volume_quote
                    }
                    
            except Exception as e:
                print(f"WebSocket message error: {e}")
        
        def save_latest_data():
            """Save the latest data to CSV file"""
            try:
                if latest_data:
                    
                    rows = []
                    for symbol, data in latest_data.items():
                        if symbol in CRYPTO_SYMBOLS:  
                            rows.append([
                                data['event_time'],
                                data['symbol'],
                                data['open'],
                                data['high'],
                                data['low'],
                                data['close'],
                                data['volume_base'],
                                data['volume_quote']
                            ])
                    
                    if rows:
                        
                        df = pd.DataFrame(rows, columns=columns)
                        df.to_csv(file_path, mode='a', header=False, index=False)
                        print(f"Saved {len(rows)} crypto data points to {file_path}")
                        
            except Exception as e:
                print(f"Error saving crypto data: {e}")
        
        
        streams = [f"{symbol.lower()}@kline_1m" for symbol in CRYPTO_SYMBOLS]
        
        try:
            
            twm.start()
            
            
            for stream in streams:
                twm.start_kline_socket(
                    symbol=stream.split('@')[0].upper(),
                    callback=handle_socket_message,
                    interval='1m'
                )
            
            print(f"Started Binance WebSocket for {len(streams)} crypto symbols")
            
            
            while running:
                try:
                    
                    save_latest_data()
                    time.sleep(60)
                    
                except Exception as e:
                    print(f"Crypto data collection error: {e}")
                    time.sleep(10)  
                    
        except Exception as e:
            print(f"WebSocket setup error: {e}")
            
        finally:
            
            try:
                twm.stop()
                print("Binance WebSocket stopped")
            except:
                pass
        
def debug_datetime_formats():
    """Debug function to check datetime formats in all data files"""
    print(" Debugging datetime formats...")
    
    for asset_type, file_path in CSV_FILES.items():
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                time_col = 'event_time' if asset_type == 'crypto' else 'timestamp'
                
                if time_col in df.columns:
                    print(f"\n{asset_type.upper()} datetime samples:")
                    print(f"First 3 values: {df[time_col].head(3).tolist()}")
                    
                    
                    parsed = pd.to_datetime(df[time_col], format='mixed', errors='coerce')
                    invalid_count = parsed.isna().sum()
                    print(f"Invalid timestamps: {invalid_count}/{len(df)}")
                    
            except Exception as e:
                print(f"Error checking {asset_type}: {e}")


debug_datetime_formats()
def start_forex_stream():
    """Fetch forex data every 5min using exchangerate-api with verbose logging and fallbacks."""
    def fetch_forex_data():
        print("Forex fetch starting...")  
        file = CSV_FILES['forex']
        existing_df = pd.DataFrame()
        if os.path.exists(file):
            try:
                existing_df = pd.read_csv(file)
                print(f"Loaded existing forex: {len(existing_df)} rows")
            except Exception as parse_e:
                print(f"Error parsing {file}: {parse_e}. Starting fresh.")
        
        new_rows = []
        pairs = get_major_pairs()  
        for pair_name, info in pairs.items():
            base = pair_name.split('/')[0]  
            quote = pair_name.split('/')[1]  
            try:
                print(f"Fetching {pair_name} (base: {base})...")
                url = f"https://api.exchangerate-api.com/v4/latest/{base}"
                response = requests.get(url, timeout=10)
                print(f"API response for {pair_name}: Status {response.status_code}")
                
                if response.status_code != 200:
                    raise Exception(f"API error: Status {response.status_code}")
                
                data = response.json()
                if 'rates' not in data or quote not in data['rates']:
                    raise Exception(f"No rate for {quote}")
                
                rate = data['rates'][quote]
                pip_value = info['pip_value']
                name = info['name']
                
                row = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  
                    'pair': pair_name,
                    'base': base,
                    'quote': quote,
                    'rate': float(rate),
                    'pip_value': pip_value,
                    'name': name
                }
                new_rows.append(row)
                print(f"Fetched {pair_name}: Rate {rate:.5f} (pip: {pip_value})")
                
                time.sleep(0.5)  
                
            except Exception as e:
                print(f"Error fetching {pair_name}: {e}. Skipping.")
        
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            if not existing_df.empty:
                combined = pd.concat([existing_df, new_df], ignore_index=True)
                
                if 'timestamp' in combined.columns:
                    combined['timestamp'] = pd.to_datetime(combined['timestamp'], format='mixed', errors='coerce')
                    combined['timestamp'] = combined['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                combined = combined.drop_duplicates(subset=['timestamp', 'pair'])
            else:
                combined = new_df
            combined.to_csv(file, index=False)
            print(f"Appended {len(new_rows)} forex rows to {file} (total: {len(combined)}).")
        else:
            print("No new forex data; adding fallback.")
            fallback_rows = [
                {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'pair': 'EUR/USD', 'base': 'EUR', 'quote': 'USD', 'rate': 1.0850, 'pip_value': 0.0001, 'name': 'Euro / US Dollar'},
                {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'pair': 'GBP/USD', 'base': 'GBP', 'quote': 'USD', 'rate': 1.2200, 'pip_value': 0.0001, 'name': 'British Pound / US Dollar'}
            ]
            fallback_df = pd.DataFrame(fallback_rows)
            if not existing_df.empty:
                combined = pd.concat([existing_df, fallback_df], ignore_index=True)
            else:
                combined = fallback_df
            combined.to_csv(file, index=False)
            print("Added fallback forex data.")
    
    print("Forex thread started.")  
    while running:
        try:
            fetch_forex_data()
        except Exception as e:
            print(f"Forex thread error: {e}. Retrying in 5min.")
        time.sleep(300)  

load_dotenv()






ASSETS = [
    "Apple", "Google", "Microsoft", "Amazon", "Tesla", "Meta", "NVIDIA",
    "Gold", "Oil", "Bitcoin", "Ethereum", "USD", "EUR", "JPY", "GBP",
    "Dow Jones", "NASDAQ", "S&P 500", "Federal Reserve", "ECB"
]

def fetch_market_headlines(from_date, to_date, page=1):
    """Fetch market-related headlines with error handling"""
    if not NEWS_API:
        print(" News API key missing - using fallback data")
        return get_fallback_articles()
    
    all_articles = []
    for query in QUERIES:
        params = {
            "q": query,
            "from": from_date,
            "to": to_date,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 50,  
            "page": page,
            "apiKey": NEWS_API
        }
        try:
            response = requests.get("https://newsapi.org/v2/everything", params=params, timeout=10)
            if response.status_code == 200:
                articles = response.json().get("articles", [])
                for article in articles:
                    article['query'] = query  
                all_articles.extend(articles)
                print(f" Fetched {len(articles)} articles for: {query}")
            else:
                print(f" API error for {query}: Status {response.status_code}")
        except Exception as e:
            print(f" Error fetching {query}: {e}")
    
    return all_articles

def get_fallback_articles():
    """Provide fallback articles when API fails"""
    fallback_articles = [
        {
            'title': 'Stock Markets Show Mixed Signals Amid Economic Data',
            'description': 'Global markets uncertain as investors await key economic indicators',
            'publishedAt': datetime.utcnow().isoformat() + 'Z',  
            'source': {'name': 'Fallback News'},
            'query': 'stock market'
        },
        {
            'title': 'Federal Reserve Holds Steady on Interest Rates',
            'description': 'Central bank maintains current policy amid inflation concerns',
            'publishedAt': (datetime.utcnow() - timedelta(hours=1)).isoformat() + 'Z',
            'source': {'name': 'Fallback News'},
            'query': 'federal reserve'
        },
        {
            'title': 'Cryptocurrency Markets Experience Volatility',
            'description': 'Bitcoin and Ethereum show price fluctuations in trading session',
            'publishedAt': (datetime.utcnow() - timedelta(hours=2)).isoformat() + 'Z',
            'source': {'name': 'Fallback News'},
            'query': 'cryptocurrency'
        }
    ]
    print(" Using fallback news articles")
    return fallback_articles

def clean_text(article):
    """Enhanced text cleaning for financial content"""
    text = " ".join(filter(None, [
        article.get("title", ""), 
        article.get("description", "")
    ]))
    
    
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"[^\w\s]", " ", text)  
    text = re.sub(r"\s+", " ", text)
    
    return text.strip()

def enhanced_vader_sentiment(texts):
    """Enhanced VADER sentiment with financial lexicon adjustments"""
    analyzer = SentimentIntensityAnalyzer()
    
    
    financial_words = {
        'bullish': 2.0, 'bearish': -2.0, 'rally': 1.5, 'plunge': -1.8,
        'surge': 1.7, 'tumble': -1.7, 'soar': 1.8, 'crash': -2.5,
        'peak': 0.5, 'bottom': -0.5, 'resistance': -0.3, 'support': 0.3
    }
    
    
    for word, score in financial_words.items():
        analyzer.lexicon[word] = score
    
    scores = []
    for text in texts:
        if not text or len(text.strip()) < 10:  
            scores.append(0.0)
            continue
            
        sentiment = analyzer.polarity_scores(text)
        compound = sentiment['compound']
        
        
        financial_boost = 0
        for word in financial_words:
            if word in text.lower():
                financial_boost += financial_words[word] * 0.1
        
        adjusted_score = compound + financial_boost
        scores.append(max(-1.0, min(1.0, adjusted_score)))
    
    return scores

def enhanced_textblob_sentiment(texts):
    """Enhanced TextBlob sentiment with financial context"""
    scores = []
    for text in texts:
        if not text or len(text.strip()) < 10:
            scores.append(0.0)
            continue
            
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            
            
            financial_indicators = {
                'positive': ['bullish', 'rally', 'surge', 'soar', 'gain', 'profit', 'growth'],
                'negative': ['bearish', 'plunge', 'tumble', 'crash', 'loss', 'decline', 'drop']
            }
            
            
            boost = 0
            text_lower = text.lower()
            for pos_word in financial_indicators['positive']:
                if pos_word in text_lower:
                    boost += 0.1
            for neg_word in financial_indicators['negative']:
                if neg_word in text_lower:
                    boost -= 0.1
            
            adjusted_score = polarity + boost
            scores.append(max(-1.0, min(1.0, adjusted_score)))
            
        except Exception as e:
            print(f"TextBlob error: {e}")
            scores.append(0.0)
    
    return scores

def calculate_ensemble_sentiment(vader_scores, textblob_scores):
    """Intelligent ensemble weighting"""
    ensemble_scores = []
    
    for vader, textblob in zip(vader_scores, textblob_scores):
        
        weight_vader = 0.6
        weight_textblob = 0.4
        
        
        vader_confidence = abs(vader)
        textblob_confidence = abs(textblob)
        
        if vader_confidence > 0.5 and textblob_confidence < 0.2:
            weight_vader = 0.8
            weight_textblob = 0.2
        elif textblob_confidence > 0.5 and vader_confidence < 0.2:
            weight_vader = 0.2
            weight_textblob = 0.8
        
        ensemble_score = (vader * weight_vader) + (textblob * weight_textblob)
        ensemble_scores.append(ensemble_score)
    
    return ensemble_scores

def detect_asset_mentioned(text):
    """Enhanced asset detection with context"""
    text_lower = text.lower()
    detected_assets = []
    
    asset_keywords = {
        'Apple': ['apple', 'aapl', 'iphone', 'ipad', 'mac'],
        'Google': ['google', 'googl', 'alphabet', 'android', 'youtube'],
        'Microsoft': ['microsoft', 'msft', 'windows', 'azure', 'xbox'],
        'Amazon': ['amazon', 'amzn', 'aws', 'prime'],
        'Tesla': ['tesla', 'tsla', 'elon musk', 'electric vehicle', 'ev'],
        'Gold': ['gold', 'xau', 'precious metal', 'bullion'],
        'Oil': ['oil', 'crude', 'brent', 'wti', 'opec', 'energy'],
        'Bitcoin': ['bitcoin', 'btc', 'crypto', 'blockchain'],
        'Ethereum': ['ethereum', 'eth', 'smart contract'],
        'USD': ['us dollar', 'dollar', 'usd', 'fed', 'federal reserve'],
        'EUR': ['euro', 'eur', 'ecb', 'european central bank'],
        'JPY': ['yen', 'jpy', 'bank of japan', 'japanese'],
        'GBP': ['pound', 'gbp', 'sterling', 'bank of england'],
        'Dow Jones': ['dow', 'dow jones', 'industrial average'],
        'NASDAQ': ['nasdaq', 'tech stocks', 'composite'],
        'S&P 500': ['s&p', 's&p 500', 'sp500', 'standard & poor']
    }
    
    for asset, keywords in asset_keywords.items():
        for keyword in keywords:
            if keyword in text_lower:
                detected_assets.append(asset)
                break
    
    return detected_assets if detected_assets else ["General Market"]

def sentiment_label(score):
    """Categorize sentiment scores"""
    if score < -0.3:
        return "Strongly Negative"
    elif score < -0.1:
        return "Negative"
    elif score <= 0.1:
        return "Neutral"
    elif score <= 0.3:
        return "Positive"
    else:
        return "Strongly Positive"

def calculate_market_sentiment_summary(df):
    """Calculate comprehensive market sentiment metrics - FIXED DATETIME VERSION"""
    if df.empty:
        return {
            'overall_sentiment': 0.0,
            'sentiment_trend': 'neutral',
            'bullish_ratio': 0.5,
            'article_count': 0,
            'asset_sentiments': {}
        }
    
    
    overall = df['sentiment_score'].mean()
    
    
    try:
        if len(df) > 10:  
            
            half_point = len(df) // 2
            recent_sentiment = df.iloc[half_point:]['sentiment_score'].mean()
            older_sentiment = df.iloc[:half_point]['sentiment_score'].mean()
            
            if pd.isna(recent_sentiment):
                recent_sentiment = 0
            if pd.isna(older_sentiment):
                older_sentiment = 0
                
            if recent_sentiment > older_sentiment + 0.1:
                trend = "improving"
            elif recent_sentiment < older_sentiment - 0.1:
                trend = "deteriorating"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"
            
    except Exception as e:
        print(f" Error in trend calculation: {e}")
        trend = "unknown"
    
    
    bullish_count = len(df[df['sentiment_score'] > 0.1])
    bearish_count = len(df[df['sentiment_score'] < -0.1])
    total_classified = bullish_count + bearish_count
    bullish_ratio = bullish_count / total_classified if total_classified > 0 else 0.5
    
    
    asset_sentiments = {}
    for asset in df['asset'].unique():
        if pd.isna(asset):  
            continue
        asset_df = df[df['asset'] == asset]
        if len(asset_df) >= 2:
            asset_sentiments[asset] = {
                'score': asset_df['sentiment_score'].mean(),
                'count': len(asset_df),
                'trend': 'positive' if asset_df['sentiment_score'].mean() > 0.1 else 'negative'
            }
    
    return {
        'overall_sentiment': float(overall) if not pd.isna(overall) else 0.0,
        'sentiment_trend': trend,
        'bullish_ratio': float(bullish_ratio),
        'article_count': len(df),
        'asset_sentiments': asset_sentiments
    }
def safe_datetime_conversion(df, column='publishedAt'):
    """Safely convert datetime columns handling timezones - FIXED UTC VERSION"""
    try:
        
        df[column] = pd.to_datetime(df[column], errors='coerce', utc=True)
        
        
        df[column] = df[column].dt.tz_convert(None)
        
        return df
    except Exception as e:
        print(f" Datetime conversion warning: {e}")
        return df
def refresh_sentiment():
    """Main function to refresh sentiment data - FIXED TIMEZONE VERSION"""
    print(" Refreshing market sentiment analysis...")
    
    
    from_date = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
    to_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    
    articles = fetch_market_headlines(from_date, to_date)
    
    if not articles:
        print(" No new articles fetched, using existing data")
        return load_sentiment_data()
    
    
    df = pd.DataFrame(articles)
    
    
    df["source"] = df["source"].apply(lambda x: x["name"] if isinstance(x, dict) else str(x))
    
    
    df = safe_datetime_conversion(df, 'publishedAt')
    
    
    print(" Analyzing article sentiment...")
    texts = [clean_text(article) for article in articles]
    
    
    vader_scores = enhanced_vader_sentiment(texts)
    textblob_scores = enhanced_textblob_sentiment(texts)
    
    
    ensemble_scores = calculate_ensemble_sentiment(vader_scores, textblob_scores)
    
    
    df["sentiment_score"] = ensemble_scores
    df["sentiment_label"] = [sentiment_label(score) for score in ensemble_scores]
    df["asset"] = [", ".join(detect_asset_mentioned(text)) for text in texts]
    
    
    market_metrics = calculate_market_sentiment_summary(df)
    
    
    if os.path.exists(SENTIMENT_FILE):
        try:
            existing_df = pd.read_csv(SENTIMENT_FILE)
            
            existing_df = safe_datetime_conversion(existing_df, 'publishedAt')
            
            
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['title', 'publishedAt'], keep='last')
        except Exception as e:
            print(f" Error merging with existing data: {e}")
            combined_df = df
    else:
        combined_df = df
    
    
    combined_df.to_csv(SENTIMENT_FILE, index=False)
    
    print(f" Sentiment analysis complete: {len(df)} new articles processed")
    print(f" Market Summary: Overall {market_metrics['overall_sentiment']:.3f} ({market_metrics['sentiment_trend']})")
    print(f" Bullish Ratio: {market_metrics['bullish_ratio']:.1%}")
    
    return combined_df

def load_sentiment_data():
    """Load existing sentiment data with error handling and timezone fixes"""
    if os.path.exists(SENTIMENT_FILE):
        try:
            df = pd.read_csv(SENTIMENT_FILE)
            
            df = safe_datetime_conversion(df, 'publishedAt')
            print(f" Loaded existing sentiment data: {len(df)} articles")
            return df
        except Exception as e:
            print(f" Error loading sentiment data: {e}")
    
    
    print(" No existing sentiment data found")
    empty_df = pd.DataFrame(columns=['publishedAt', 'title', 'sentiment_score', 'sentiment_label', 'asset'])
    return safe_datetime_conversion(empty_df, 'publishedAt')

def get_current_sentiment_summary():
    """Get current sentiment summary for dashboard"""
    df = load_sentiment_data()
    return calculate_market_sentiment_summary(df)


def test_sentiment_analysis():
    """Test the sentiment analysis on sample text"""
    test_texts = [
        "Stock markets rally as Fed signals pause in rate hikes",
        "Bitcoin plunges 10% amid regulatory concerns",
        "Apple reports strong earnings, shares surge after hours",
        "Economic data shows mixed signals for inflation outlook"
    ]
    
    print(" Testing sentiment analysis...")
    vader_scores = enhanced_vader_sentiment(test_texts)
    textblob_scores = enhanced_textblob_sentiment(test_texts)
    ensemble_scores = calculate_ensemble_sentiment(vader_scores, textblob_scores)
    
    for i, text in enumerate(test_texts):
        print(f"Text: {text[:60]}...")
        print(f"  VADER: {vader_scores[i]:.3f}, TextBlob: {textblob_scores[i]:.3f}, Ensemble: {ensemble_scores[i]:.3f}")
        print(f"  Label: {sentiment_label(ensemble_scores[i])}")
        print()


if __name__ == "__main__":
    test_sentiment_analysis()
    df = refresh_sentiment()
    summary = get_current_sentiment_summary()
    print(f"Final Summary: {summary}")




def load_data(asset_type, lookback_hours=24, full_history=False):
    """
    Load data from CSV with robust datetime parsing for all formats.
    """
    file = CSV_FILES.get(asset_type)
    if not os.path.exists(file):
        print(f"No {asset_type} data file found.")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file)
        
        
        time_col = 'event_time' if asset_type == 'crypto' else 'timestamp'
        
        if time_col in df.columns:
            
            df[time_col] = pd.to_datetime(df[time_col], format='mixed', errors='coerce', utc=True)
            
            
            initial_count = len(df)
            df = df.dropna(subset=[time_col])
            if len(df) < initial_count:
                print(f" Dropped {initial_count - len(df)} rows with invalid timestamps")
            
            if not full_history and len(df) > 0:
                
                from datetime import timezone
                cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
                df = df[df[time_col] > cutoff]
                
        return df
        
    except Exception as e:
        print(f" Error loading {asset_type} data: {e}")
        return pd.DataFrame()
def get_latest_prices():
    """
    Get latest prices for dashboard and trade exit checks (recent 2 hours, limit display to 5 symbols).
    """
    prices = {}
    
    crypto_df = load_data('crypto', 2)
    if not crypto_df.empty:
        prices['crypto'] = []
        for symbol in crypto_df['symbol'].unique()[:5]:  
            sym_data = crypto_df[crypto_df['symbol'] == symbol].iloc[-1]
            prices['crypto'].append({
                'symbol': symbol, 'price': sym_data['close'], 'volume_base': sym_data['volume_base'],
                'timestamp': sym_data['event_time']
            })
    
    stock_df = load_data('stocks', 2)
    if not stock_df.empty:
        prices['stocks'] = []
        for ticker in stock_df['ticker'].unique()[:5]:
            t_data = stock_df[stock_df['ticker'] == ticker].iloc[-1]
            prices['stocks'].append({
                'ticker': ticker, 'price': t_data['close'], 'volume': t_data['volume'],
                'timestamp': t_data['timestamp']
            })
    
    forex_df = load_data('forex', 2)
    if not forex_df.empty:
        prices['forex'] = []
        for pair in forex_df['pair'].unique()[:5]:
            p_data = forex_df[forex_df['pair'] == pair].iloc[-1]
            prices['forex'].append({
                'pair': pair, 'rate': p_data['rate'], 'pip_value': p_data['pip_value'],
                'timestamp': p_data['timestamp']
            })
    return prices

def apply_ta(df, asset_type, volume_col=None):
    """
    Apply technical indicators using pandas_ta with proper column handling.
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    
    if asset_type == 'forex':
        df['open'] = df['high'] = df['low'] = df['close'] = df['rate']
        volume_col = None  
    
    
    if 'close' not in df.columns:
        df['close'] = df.get('rate', df.get('price', 0))
        print(f"Warning: No 'close' column in {asset_type} data; using fallback.")
    
    
    df['RSI'] = pta.rsi(df['close'], length=14)
    
    
    try:
        macd_data = pta.macd(df['close'], fast=12, slow=26, signal=9)
        if macd_data is not None and not macd_data.empty:
            
            macd_data = macd_data.rename(columns={
                'MACD_12_26_9': 'MACD',
                'MACDs_12_26_9': 'MACD_signal', 
                'MACDh_12_26_9': 'MACD_histogram'
            })
            
            df = pd.concat([df, macd_data], axis=1)
    except Exception as e:
        print(f"MACD calculation error: {e}")
        
        df['MACD'] = 0.0
        df['MACD_signal'] = 0.0
        df['MACD_histogram'] = 0.0
    
    
    df['SMA50'] = pta.sma(df['close'], length=50)
    df['EMA20'] = pta.ema(df['close'], length=20)
    
    
    try:
        adx_data = pta.adx(df['high'], df['low'], df['close'], length=14)
        if adx_data is not None and not adx_data.empty:
            
            adx_data = adx_data.rename(columns={
                'ADX_14': 'ADX',
                'DMP_14': 'DMP',
                'DMN_14': 'DMN'
            })
            
            df = pd.concat([df, adx_data], axis=1)
    except Exception as e:
        print(f"ADX calculation error: {e}")
        
        df['ADX'] = 0.0
        df['DMP'] = 0.0
        df['DMN'] = 0.0
    
    
    df['ATR'] = pta.atr(df['high'], df['low'], df['close'], length=14)
    
    
    if volume_col and volume_col in df.columns and not df[volume_col].isna().all():
        try:
            df['OBV'] = pta.obv(df['close'], df[volume_col])
            df['CMF'] = pta.cmf(df['high'], df['low'], df['close'], df[volume_col])
        except Exception as e:
            print(f"Volume indicator error: {e}")
            df['OBV'] = 0.0
            df['CMF'] = 0.0
    else:
        df['OBV'] = 0.0
        df['CMF'] = 0.0
    
    
    ta_columns = [
        'RSI', 'MACD', 'MACD_signal', 'MACD_histogram', 'SMA50', 'EMA20', 
        'ADX', 'DMP', 'DMN', 'ATR', 'OBV', 'CMF'
    ]
    
    for col in ta_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)
        else:
            df[col] = 0.0  
    
    return df
def train_model_from_data(asset_type, sentiment_df):
    """
    Train River LogisticRegression on full historical data and update model with live data.
    """
    cfg = {
        'crypto': {'file': CSV_FILES['crypto'], 'time_col': 'event_time', 'volume_col': 'volume_base', 'symbol_col': 'symbol', 'price_col': 'close'},
        'stocks': {'file': CSV_FILES['stocks'], 'time_col': 'timestamp', 'volume_col': 'volume', 'symbol_col': 'ticker', 'price_col': 'close'},
        'forex': {'file': CSV_FILES['forex'], 'time_col': 'timestamp', 'volume_col': None, 'symbol_col': 'pair', 'price_col': 'rate'}
    }
    
    if asset_type not in cfg:
        return
    
    cfg = cfg[asset_type]
    df = load_data(asset_type, full_history=True)
    
    if df.empty:
        return
    
    df = apply_ta(df, asset_type, cfg['volume_col'])
    
    df['date'] = pd.to_datetime(df[cfg['time_col']]).dt.date
    
    if not sentiment_df.empty:
        sentiment_df['date'] = pd.to_datetime(sentiment_df['publishedAt']).dt.date
        daily_sent = sentiment_df.groupby('date')['sentiment_score'].mean().reset_index()
        df = pd.merge(df, daily_sent, on='date', how='left')
        df['sentiment_score'] = df['sentiment_score'].fillna(0)
    else:
        df['sentiment_score'] = 0
    
    
    df['target'] = (df[cfg['price_col']].shift(-1) > df[cfg['price_col']]).astype(int)
    df = df.dropna(subset=['target'])
    
    if df.empty:
        return
    
    excluded = [
        cfg['time_col'], cfg['symbol_col'], 'open', 'high', 'low', cfg['price_col'], 
        'volume', 'volume_base', 'volume_quote', 'date', 'target', 'pip_value', 
        'name', 'base', 'quote'
    ]
    
    expected_ta_columns = [
        'RSI', 'MACD', 'MACD_signal', 'MACD_histogram', 'SMA50', 'EMA20', 
        'ADX', 'DMP', 'DMN', 'ATR', 'OBV', 'CMF', 'sentiment_score'
    ]
    
    feature_columns = []
    for col in expected_ta_columns:
        if col in df.columns:
            feature_columns.append(col)
    
    df[feature_columns] = df[feature_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
    
    model = preprocessing.StandardScaler() | linear_model.LogisticRegression()
    
    # Continuous learning from live data
    for _, row in df.iterrows():
        model.learn_one(row[feature_columns].to_dict(), row['target'])
    
    print(f" Initial training completed for {asset_type}: {len(df)} samples, {len(feature_columns)} features")
    return model, feature_columns
def get_latest_features(asset_type, sentiment_df):
    """
    Get latest row with TA and sentiment for prediction - FIXED DATETIME VERSION.
    """
    cfg = {
        'crypto': {'time_col': 'event_time', 'volume_col': 'volume_base', 'symbol_col': 'symbol', 'price_col': 'close'},
        'stocks': {'time_col': 'timestamp', 'volume_col': 'volume', 'symbol_col': 'ticker', 'price_col': 'close'},
        'forex': {'time_col': 'timestamp', 'volume_col': None, 'symbol_col': 'pair', 'price_col': 'rate'}
    }
    
    if asset_type not in cfg:
        return {}, []
        
    cfg = cfg[asset_type]
    df = load_data(asset_type, 24)  
    
    if df.empty:
        return {}, []
    
    
    if asset_type in ['crypto', 'stocks']:
        latest_symbol = df[cfg['symbol_col']].iloc[-1]
        df = df[df[cfg['symbol_col']] == latest_symbol]
    
    
    df = apply_ta(df, asset_type, cfg['volume_col'])
    
    
    try:
        df['date'] = df[cfg['time_col']].dt.date
        
        if not sentiment_df.empty:
            
            if 'publishedAt' in sentiment_df.columns:
                sentiment_df = sentiment_df.copy()
                sentiment_df['publishedAt'] = pd.to_datetime(sentiment_df['publishedAt'], errors='coerce', utc=True)
                sentiment_df['date'] = sentiment_df['publishedAt'].dt.date
                
                daily_sent = sentiment_df.groupby('date')['sentiment_score'].mean().reset_index()
                df = pd.merge(df, daily_sent, on='date', how='left')
                df['sentiment_score'] = df['sentiment_score'].fillna(0)
            else:
                df['sentiment_score'] = 0
        else:
            df['sentiment_score'] = 0
            
    except Exception as e:
        print(f" Error merging sentiment for {asset_type}: {e}")
        df['sentiment_score'] = 0
    
    
    if len(df) == 0:
        return {}, []
        
    latest_row = df.iloc[-1]
    
    
    excluded = [cfg['time_col'], cfg['symbol_col'], 'open', 'high', 'low', cfg['price_col'], 
                'volume', 'volume_base', 'volume_quote', 'date', 'target', 'pip_value', 
                'name', 'base', 'quote']
    
    valid_features = [col for col in latest_row.index if col not in excluded and pd.notna(latest_row[col])]
    
    
    latest_dict = {}
    for feature in valid_features:
        try:
            value = latest_row[feature]
            if pd.isna(value):
                latest_dict[feature] = 0.0
            else:
                latest_dict[feature] = float(value)
        except (ValueError, TypeError):
            latest_dict[feature] = 0.0
    
    return latest_dict, valid_features


def incremental_model_update(models, feature_columns, asset_type, sentiment_df):
    """Update models incrementally with new data every 60 seconds - FIXED VERSION"""
    if asset_type not in models:
        return models, feature_columns
    
    try:
        # Load only recent data (last 2 hours for efficiency)
        df = load_data(asset_type, lookback_hours=2)
        if df.empty:
            return models, feature_columns
        
        # Apply technical indicators
        cfg = {
            'crypto': {'time_col': 'event_time', 'volume_col': 'volume_base', 'symbol_col': 'symbol', 'price_col': 'close'},
            'stocks': {'time_col': 'timestamp', 'volume_col': 'volume', 'symbol_col': 'ticker', 'price_col': 'close'},
            'forex': {'time_col': 'timestamp', 'volume_col': None, 'symbol_col': 'pair', 'price_col': 'rate'}
        }
        
        if asset_type not in cfg:
            return models, feature_columns
            
        cfg = cfg[asset_type]
        df = apply_ta(df, asset_type, cfg['volume_col'])
        
        # Add sentiment data
        try:
            df['date'] = df[cfg['time_col']].dt.date
            if not sentiment_df.empty:
                sentiment_df_copy = sentiment_df.copy()
                sentiment_df_copy['publishedAt'] = pd.to_datetime(sentiment_df_copy['publishedAt'], errors='coerce', utc=True)
                sentiment_df_copy['date'] = sentiment_df_copy['publishedAt'].dt.date
                daily_sent = sentiment_df_copy.groupby('date')['sentiment_score'].mean().reset_index()
                df = pd.merge(df, daily_sent, on='date', how='left')
                df['sentiment_score'] = df['sentiment_score'].fillna(0)
            else:
                df['sentiment_score'] = 0
        except Exception as e:
            print(f" Error adding sentiment in incremental learning for {asset_type}: {e}")
            df['sentiment_score'] = 0
        
        # Create target variable
        df['target'] = (df[cfg['price_col']].shift(-1) > df[cfg['price_col']]).astype(int)
        df = df.dropna(subset=['target'])
        
        if df.empty:
            return models, feature_columns
        
        # Get the feature columns that the model was originally trained with
        expected_features = feature_columns.get(asset_type, [])
        if not expected_features:
            return models, feature_columns
        
        # Incremental learning on new data
        learning_samples = 0
        for _, row in df.iterrows():
            try:
                features = {}
                for col in expected_features:
                    if col in df.columns and pd.notna(row[col]):
                        features[col] = float(row[col])
                    else:
                        features[col] = 0.0
                
                if features:  # Only learn if we have valid features
                    models[asset_type].learn_one(features, row['target'])
                    learning_samples += 1
                    
            except Exception as e:
                continue  # Skip problematic rows
        
        if learning_samples > 0:
            print(f" Incrementally learned {learning_samples} new samples for {asset_type}")
        
        return models, feature_columns
        
    except Exception as e:
        print(f" Error in incremental learning for {asset_type}: {e}")
        return models, feature_columns  
        """Update models incrementally with new data every 60 seconds"""
    if asset_type not in models:
        return models, feature_columns
    
    # Load only recent data (last 2 hours for efficiency)
    df = load_data(asset_type, lookback_hours=2)
    if df.empty:
        return models, feature_columns
    
    # Apply technical indicators
    cfg = {
        'crypto': {'time_col': 'event_time', 'volume_col': 'volume_base', 'symbol_col': 'symbol', 'price_col': 'close'},
        'stocks': {'time_col': 'timestamp', 'volume_col': 'volume', 'symbol_col': 'ticker', 'price_col': 'close'},
        'forex': {'time_col': 'timestamp', 'volume_col': None, 'symbol_col': 'pair', 'price_col': 'rate'}
    }
    
    if asset_type not in cfg:
        return models, feature_columns
        
    cfg = cfg[asset_type]
    df = apply_ta(df, asset_type, cfg['volume_col'])
    
    # Add sentiment data
    try:
        df['date'] = df[cfg['time_col']].dt.date
        if not sentiment_df.empty:
            sentiment_df_copy = sentiment_df.copy()
            sentiment_df_copy['publishedAt'] = pd.to_datetime(sentiment_df_copy['publishedAt'], errors='coerce', utc=True)
            sentiment_df_copy['date'] = sentiment_df_copy['publishedAt'].dt.date
            daily_sent = sentiment_df_copy.groupby('date')['sentiment_score'].mean().reset_index()
            df = pd.merge(df, daily_sent, on='date', how='left')
            df['sentiment_score'] = df['sentiment_score'].fillna(0)
        else:
            df['sentiment_score'] = 0
    except Exception as e:
        print(f" Error adding sentiment in incremental learning for {asset_type}: {e}")
        df['sentiment_score'] = 0
    
    # Create target variable
    df['target'] = (df[cfg['price_col']].shift(-1) > df[cfg['price_col']]).astype(int)
    df = df.dropna(subset=['target'])
    
    if df.empty:
        return models, feature_columns
    
    # Define feature columns (same as in training)
    excluded_columns = [
        cfg['time_col'], cfg['symbol_col'], 'open', 'high', 'low', cfg['price_col'], 
        'volume', 'volume_base', 'volume_quote', 'date', 'target', 'pip_value', 
        'name', 'base', 'quote'
    ]
    
    feature_cols = [col for col in df.columns if col not in excluded_columns and col in feature_columns.get(asset_type, [])]
    
    # Incremental learning on new data
    learning_samples = 0
    for _, row in df.iterrows():
        try:
            features = {}
            for col in feature_cols:
                if col in row and pd.notna(row[col]):
                    features[col] = float(row[col])
                else:
                    features[col] = 0.0
            
            if features:  # Only learn if we have valid features
                models[asset_type].learn_one(features, row['target'])
                learning_samples += 1
                
        except Exception as e:
            continue  # Skip problematic rows
    
    if learning_samples > 0:
        print(f" Incrementally learned {learning_samples} new samples for {asset_type}")
    
    return models, feature_columns
def generate_signal(model, feature_columns, sentiment_df, asset_type, current_price, symbol=None):
    """
    Generate signal, probs, TP/SL in pips/points, suggested volume.
    Uses latest features for prediction.
    """
    if not model:
        return "HOLD", 0.5, 0.5, None, None, None, 0.0
    latest_row, valid_features = get_latest_features(asset_type, sentiment_df)
    if not valid_features:
        return "HOLD", 0.5, 0.5, None, None, None, 0.0
    X_new = {k: latest_row.get(k, 0) for k in feature_columns if k in valid_features}
    prediction = model.predict_proba_one(X_new)
    prob_up = prediction.get(1, 0.5)
    prob_down = prediction.get(0, 0.5)
    atr = latest_row.get('ATR', 0.01)  
    if atr == 0:
        atr = 0.01 * current_price  
    
    signal = "HOLD"
    sl_distance = None
    tp_distance = None
    if prob_up > 0.6:
        signal = "BUY"
        sl_distance = ATR_MULTIPLIER_SL * atr
        tp_distance = sl_distance * REWARD_RISK_RATIO
    elif prob_down > 0.6:
        signal = "SELL"
        sl_distance = ATR_MULTIPLIER_SL * atr
        tp_distance = sl_distance * REWARD_RISK_RATIO
    if signal == "HOLD" or sl_distance is None:
        return signal, prob_up, prob_down, None, None, None, atr
    
    pair_info = get_pair_info(symbol) if asset_type == 'forex' and symbol else {"pip_value": PIP_VALUE_DEFAULT}
    pip_value = pair_info['pip_value']
    sl_pips = sl_distance / pip_value if pip_value > 0 else sl_distance / (0.01 * current_price)  
    tp_pips = tp_distance / pip_value if pip_value > 0 else tp_distance / (0.01 * current_price)
    if sl_pips < MIN_SL_PIPS:
        print(f"SL too small ({sl_pips:.1f} pips) for {symbol}; skipping signal.")
        return "HOLD", prob_up, prob_down, None, None, None, atr
    
    volume = None  
    return signal, prob_up, prob_down, tp_pips, sl_pips, volume, atr

def calculate_position_size(asset_type, signal, current_price, sl_pips, balance, symbol=None):
    """
    Enhanced position sizing with multiple safety checks
    """
    if balance <= 100:  
        return 0
    
    
    risk_usd = balance * RISK_PER_TRADE
    
    
    volatility_factor = get_volatility_adjustment(asset_type, symbol)
    risk_usd *= volatility_factor
    
    pair_info = get_pair_info(symbol) if asset_type == 'forex' and symbol else {"pip_value": PIP_VALUE_DEFAULT}
    pip_value = pair_info['pip_value']
    
    if asset_type == 'forex':
        
        usd_per_pip_per_full_lot = 10.0
        sl_risk_per_full_lot = sl_pips * usd_per_pip_per_full_lot
        
        if sl_risk_per_full_lot <= 0:
            return 0
            
        lot_size = risk_usd / sl_risk_per_full_lot
        
        
        min_lot = 0.01
        max_lot = min(10.0, balance / 1000)  
        
        lot_size = max(min_lot, min(lot_size, max_lot))
        return round(lot_size, 2)
        
    else:
        
        point_value = pip_value * current_price if pip_value > 0 else current_price * 0.01
        
        sl_price_distance = sl_pips * point_value
        if sl_price_distance <= 0:
            return 0
            
        quantity = risk_usd / sl_price_distance
        
        
        max_affordable = (balance * 0.1) / current_price  
        quantity = min(quantity, max_affordable)
        quantity = max(1, int(quantity))
        
        return quantity if quantity > 0 else 0

def get_volatility_adjustment(asset_type, symbol=None):
    """Reduce position size during high volatility"""
    try:
        df = load_data(asset_type, 24)  
        if df.empty:
            return 0.5  
            
        df = apply_ta(df, asset_type)
        current_atr = df['ATR'].iloc[-1] if 'ATR' in df.columns else 0
        avg_atr = df['ATR'].mean() if 'ATR' in df.columns else current_atr
        
        if avg_atr > 0:
            volatility_ratio = current_atr / avg_atr
            
            if volatility_ratio > 2.0:
                return 0.3  
            elif volatility_ratio > 1.5:
                return 0.5  
            elif volatility_ratio > 1.2:
                return 0.8  
                
        return 1.0  
    except:
        return 0.5  

def generate_signal(model, feature_columns, sentiment_df, asset_type, current_price, symbol=None):
    """
    Generate signal, probs, TP/SL in pips/points, suggested volume (volume needs balance).
    Uses latest features for prediction. Symbol needed for forex pip_value.
    """
    if not model or not feature_columns:
        return "HOLD", 0.5, 0.5, None, None, None, 0.0
    latest_row, valid_features = get_latest_features(asset_type, sentiment_df)
    if not valid_features:
        return "HOLD", 0.5, 0.5, None, None, None, 0.0
    
    X_new = {k: latest_row.get(k, 0.0) for k in feature_columns if k in valid_features}
    prediction = model.predict_proba_one(X_new)
    prob_up = prediction.get(1, 0.5)
    prob_down = prediction.get(0, 0.5)
    
    atr = latest_row.get('ATR', 0.01)
    if atr <= 0:
        atr = 0.01 * current_price  
    
    signal = "HOLD"
    sl_distance = None  
    tp_distance = None
    if prob_up > 0.6:
        signal = "BUY"
        sl_distance = ATR_MULTIPLIER_SL * atr  
        tp_distance = sl_distance * REWARD_RISK_RATIO  
    elif prob_down > 0.6:
        signal = "SELL"
        sl_distance = ATR_MULTIPLIER_SL * atr  
        tp_distance = sl_distance * REWARD_RISK_RATIO  
    if signal == "HOLD" or sl_distance is None or sl_distance <= 0:
        return signal, prob_up, prob_down, None, None, None, atr
    
    pair_info = get_pair_info(symbol) if asset_type == 'forex' and symbol else {"pip_value": PIP_VALUE_DEFAULT}
    pip_value = pair_info['pip_value']
    if pip_value <= 0:
        pip_value = 0.0001  
    sl_pips = sl_distance / pip_value  
    tp_pips = tp_distance / pip_value
    
    if sl_pips < MIN_SL_PIPS:
        print(f"SL too small ({sl_pips:.1f} pips) for {symbol or asset_type}; holding.")
        return "HOLD", prob_up, prob_down, None, None, None, atr
    
    volume = None
    return signal, prob_up, prob_down, tp_pips, sl_pips, volume, atr

def calculate_position_size(asset_type, signal, current_price, sl_pips, balance, symbol=None):
    """
    Calculate volume/lot size based on risk (balance * RISK_PER_TRADE) / SL_risk_per_unit.
    - Forex: Returns lot size (float, e.g., 0.1 mini-lots; $1/pip per 0.01 lot approx).
    - Stocks/Crypto: Returns quantity (int, e.g., 10 shares; based on USD SL distance).
    Assumes USD account; caps at affordable.
    """
    if balance <= 100:  
        print("Balance too low; no new positions.")
        return 0
    risk_usd = balance * RISK_PER_TRADE  
    pair_info = get_pair_info(symbol) if asset_type == 'forex' and symbol else {"pip_value": PIP_VALUE_DEFAULT}
    pip_value = pair_info['pip_value']
    if asset_type == 'forex':
        
        
        usd_per_pip_per_full_lot = 10.0  
        sl_risk_per_full_lot = sl_pips * usd_per_pip_per_full_lot
        lot_size = risk_usd / sl_risk_per_full_lot if sl_risk_per_full_lot > 0 else 0
        lot_size = max(0.01, min(lot_size, 10.0))  
        return round(lot_size, 2)
    else:
        
        point_value = pip_value * current_price if pip_value > 0 else current_price * 0.01  
        sl_price_distance = sl_pips * point_value  
        if sl_price_distance <= 0:
            return 0
        quantity = risk_usd / sl_price_distance  
        
        max_affordable = balance / current_price
        quantity = min(quantity, max_affordable)
        quantity = max(1, int(quantity)) if quantity >= 1 else 0
        return quantity

def load_account_state():
    """Load or initialize account state (balance, stats)."""
    if os.path.exists(ACCOUNT_STATE_FILE):
        with open(ACCOUNT_STATE_FILE, 'r') as f:
            state = json.load(f)
            return state.get('balance', ACCOUNT_BALANCE_START), state.get('total_trades', 0), state.get('wins', 0), state.get('total_pnl', 0.0)
    
    """
    state = {'balance': ACCOUNT_BALANCE_START, 'total_trades': 0, 'wins': 0, 'total_pnl': 0.0}
    save_account_state(state)
    return ACCOUNT_BALANCE_START, 0, 0, 0.0
    """
    save_account_state(balance=ACCOUNT_BALANCE_START , total_trades=0 , wins=0 , total_pnl=0)
    return ACCOUNT_BALANCE_START, 0, 0, 0.0


def save_account_state(balance, total_trades, wins, total_pnl):
    """Save account state after updates (safe against zero trades)."""
    state = {'balance': balance, 'total_trades': total_trades, 'wins': wins, 'total_pnl': total_pnl}
    with open(ACCOUNT_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)
    
    if total_trades and total_trades > 0:
        win_rate_str = f"{(wins / total_trades * 100.0):.1f}%"
    else:
        win_rate_str = "N/A"
    print(f"Account updated: Balance ${balance:.2f}, Total Trades: {total_trades}, Win Rate: {win_rate_str}")

def load_open_trades():
    """Load open trades from trades.json."""
    if os.path.exists(TRADE_FILE):
        with open(TRADE_FILE, 'r') as f:
            return json.load(f)
    return []

def save_open_trades(trades):
    """Save open trades."""
    with open(TRADE_FILE, 'w') as f:
        json.dump(trades, f, indent=2)

def load_trade_history():
    """Load closed trade history."""
    if os.path.exists(TRADE_HISTORY_FILE):
        with open(TRADE_HISTORY_FILE, 'r') as f:
            return json.load(f)
    return []

def save_trade_history(history):
    """Append to and save trade history."""
    with open(TRADE_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)


def execute_trade(asset_type, symbol, signal, entry_price, tp_pips, sl_pips, volume, atr):
    """Enhanced trade execution with comprehensive safety checks"""
    
    
    if circuit_breaker.emergency_stop:
        print(f" Trade blocked: Emergency stop active")
        return None
        
    market_ok, market_reason = circuit_breaker.market_condition_check()
    if not market_ok:
        print(f" Trade blocked: {market_reason}")
        return None
    
    
    risk_ok, risk_reason = risk_manager.pre_trade_checks(
        asset_type, symbol, signal, entry_price, sl_pips, volume
    )
    if not risk_ok:
        print(f" Trade blocked: {risk_reason}")
        return None
    
    
    trades = load_open_trades()
    trade_id = len(trades) + 1
    pair_info = get_pair_info(symbol) if asset_type == 'forex' else {"pip_value": PIP_VALUE_DEFAULT}
    pip_value = pair_info['pip_value']
    
    
    sl_distance = sl_pips * pip_value
    tp_distance = tp_pips * pip_value
    
    if signal == "BUY":
        sl_price = entry_price - sl_distance
        tp_price = entry_price + tp_distance
    else:  
        sl_price = entry_price + sl_distance
        tp_price = entry_price - tp_distance
    
    trade = {
        'id': trade_id,
        'asset_type': asset_type,
        'symbol': symbol,
        'signal': signal,
        'entry': entry_price,
        'tp': tp_price,
        'sl': sl_price,
        'volume': volume,
        'atr': atr,
        'risk_usd': volume * sl_distance,  
        'open_time': datetime.now().isoformat(),
        'status': 'open'
    }
    
    trades.append(trade)
    save_open_trades(trades)
    
    
    risk_manager.daily_trade_count += 1
    
    print(f" Executed {signal} {symbol}: Entry {entry_price}, TP {tp_price:.5f}, SL {sl_price:.5f}, Volume {volume}")
    
    
    send_trade_alert(trade, "OPEN")
    
    return trade

def send_trade_alert(trade, action):
    """Send trade alerts (integrate with your preferred notification service)"""
    message = f"""
     {action} TRADE
    Symbol: {trade['symbol']}
    Signal: {trade['signal']}
    Entry: {trade['entry']:.5f}
    TP: {trade['tp']:.5f}
    SL: {trade['sl']:.5f}
    Volume: {trade['volume']}
    Risk: ${trade['risk_usd']:.2f}
    Time: {datetime.now().strftime('%H:%M:%S')}
    """
    print(message)

def trade_verdict_text(pnl_usd):
    """Return an experienced-trader style short verdict for a closed trade.
    - Good real: clear positive and sizable win
    - Bad real: clear negative and sizable loss
    - Neutral: small win/loss or break-even
    Uses absolute thresholds relative to account starting balance.
    """
    pct = (pnl_usd / ACCOUNT_BALANCE_START) * 100.0
    if pnl_usd >= ACCOUNT_BALANCE_START * 0.01:  
        return "Good (real good)"
    if pnl_usd <= -ACCOUNT_BALANCE_START * 0.01:
        return "Bad (real bad)"
    if abs(pnl_usd) < ACCOUNT_BALANCE_START * 0.001:  
        return "Neutral"
    return "Mixed"
    
    


def check_trade_exits(prices):
    """Check all open trades for TP/SL hits using live prices. Update balance/P&L."""
    trades = load_open_trades()
    if not trades:
        return
    history = load_trade_history()
    balance, total_trades, wins, total_pnl = load_account_state()
    closed_any = False
    for trade in trades[:]:  
        if trade['status'] != 'open':
            continue
        asset_type = trade['asset_type']
        symbol = trade['symbol']
        current_price = None
        
        if asset_type == 'crypto' and prices.get('crypto'):
            for p in prices['crypto']:
                if p['symbol'] == symbol:
                    current_price = p['price']
                    break
        elif asset_type == 'stocks' and prices.get('stocks'):
            for p in prices['stocks']:
                if p['ticker'] == symbol:
                    current_price = p['price']
                    break
        elif asset_type == 'forex' and prices.get('forex'):
            for p in prices['forex']:
                if p['pair'] == symbol:
                    current_price = p['rate']
                    break
        if current_price is None:
            continue  
        signal = trade['signal']
        tp_price = trade['tp']
        sl_price = trade['sl']
        volume = trade['volume']
        hit_tp = False
        hit_sl = False
        if signal == "BUY":
            if current_price >= tp_price:
                hit_tp = True
            elif current_price <= sl_price:
                hit_sl = True
        else:  
            if current_price <= tp_price:
                hit_tp = True
            elif current_price >= sl_price:
                hit_sl = True
        if hit_tp or hit_sl:
            
            entry = trade['entry']
            exit_price = tp_price if hit_tp else sl_price
            pair_info = get_pair_info(symbol) if asset_type == 'forex' else {"pip_value": PIP_VALUE_DEFAULT}
            pip_value = pair_info['pip_value']
            pips_hit = abs(exit_price - entry) / pip_value
            if signal == "SELL":
                pips_hit = -pips_hit  
            
            if asset_type == 'forex':
                usd_per_pip_per_lot = 10.0  
                pnl_usd = pips_hit * volume * usd_per_pip_per_lot
            else:
                
                pnl_usd = (exit_price - entry) * volume if signal == "BUY" else (entry - exit_price) * volume
            balance += pnl_usd
            total_trades += 1
            if pnl_usd > 0:
                wins += 1
            total_pnl += pnl_usd
            
            trade['status'] = 'closed'
            trade['exit_price'] = exit_price
            trade['pips'] = pips_hit
            trade['pnl_usd'] = round(pnl_usd, 2)
            trade['exit_time'] = datetime.now().isoformat()
            trade['hit_tp'] = hit_tp
            
            try:
                trade['verdict'] = trade_verdict_text(pnl_usd)
            except Exception:
                trade['verdict'] = 'Unknown'
            history.append(trade)
            trades.remove(trade)  
            closed_any = True
            print(f"Closed {signal} {symbol}: P&L ${pnl_usd:.2f}, New Balance ${balance:.2f}")
    if closed_any:
        save_open_trades(trades)
        save_trade_history(history)
        save_account_state(balance, total_trades, wins, total_pnl)


def auto_reset_open_trades(prices):
    """Force-close or reset open trades every AUTO_RESET_INTERVAL seconds.
    This will close open trades at current market price and mark them with reason 'reset'.
    It uses the same P&L calculation as check_trade_exits but always forces an exit.
    """
    trades = load_open_trades()
    if not trades:
        return
    history = load_trade_history()
    balance, total_trades, wins, total_pnl = load_account_state()
    closed_count = 0

    for trade in trades[:]:
        try:
            if trade.get('status') != 'open':
                continue
            asset_type = trade['asset_type']
            symbol = trade['symbol']
            current_price = None

            if asset_type == 'crypto' and prices.get('crypto'):
                for p in prices['crypto']:
                    if p['symbol'] == symbol:
                        current_price = p['price']
                        break
            elif asset_type == 'stocks' and prices.get('stocks'):
                for p in prices['stocks']:
                    if p['ticker'] == symbol:
                        current_price = p['price']
                        break
            elif asset_type == 'forex' and prices.get('forex'):
                for p in prices['forex']:
                    if p['pair'] == symbol:
                        current_price = p['rate']
                        break

            if current_price is None:
                
                continue

            entry = trade['entry']
            volume = trade.get('volume', 1)
            signal = trade['signal']

            pair_info = get_pair_info(symbol) if asset_type == 'forex' else { 'pip_value': PIP_VALUE_DEFAULT }
            pip_value = pair_info['pip_value']

            if signal == 'BUY':
                pnl_usd = (current_price - entry) * volume
            else:
                pnl_usd = (entry - current_price) * volume

            
            if asset_type == 'forex':
                usd_per_pip_per_lot = 10.0
                
                pips = abs(current_price - entry) / (pip_value if pip_value > 0 else PIP_VALUE_DEFAULT)
                if signal == 'SELL':
                    pips = -pips
                pnl_usd = pips * volume * usd_per_pip_per_lot

            balance += pnl_usd
            total_trades += 1
            if pnl_usd > 0:
                wins += 1
            total_pnl += pnl_usd

            trade['status'] = 'closed'
            trade['exit_price'] = current_price
            trade['pips'] = abs(current_price - entry) / (pip_value if pip_value > 0 else PIP_VALUE_DEFAULT)
            trade['pnl_usd'] = round(pnl_usd, 2)
            trade['exit_time'] = datetime.now().isoformat()
            trade['hit_tp'] = False
            trade['reset_reason'] = 'auto_reset_interval'
            try:
                trade['verdict'] = trade_verdict_text(pnl_usd)
            except Exception:
                trade['verdict'] = 'Unknown'

            history.append(trade)
            trades.remove(trade)
            closed_count += 1
            print(f"Auto-reset closed {signal} {symbol} @ {current_price:.5f}, P&L: ${pnl_usd:.2f}")

        except Exception as e:
            print(f"Error auto-resetting trade {trade.get('id', 'unknown')}: {e}")
            continue

    if closed_count > 0:
        save_open_trades(trades)
        save_trade_history(history)
        save_account_state(balance, total_trades, wins, total_pnl)
        print(f"Auto-reset completed: {closed_count} trades closed")


def get_trade_history_summary():
    """Get dynamic summary for dashboard: total, win rate, open count, last 3 open, last 5 closed."""
    open_trades = load_open_trades()
    history = load_trade_history()
    balance, total_trades, wins, total_pnl = load_account_state()
    
    win_rate = (wins / total_trades * 100.0) if total_trades and total_trades > 0 else 0.0
    summary = {
        'balance': round(balance, 2),
        'total_trades': total_trades,
        'wins': wins,
        'win_rate': round(win_rate, 1),
        'open_count': len(open_trades),
        'total_pnl': round(total_pnl, 2),
        'last_open': open_trades[-3:] if len(open_trades) >= 3 else open_trades,
        'recent_closed': history[-5:] if len(history) >= 5 else history
    }
    return summary





def display_dashboard(models, feature_columns, sentiment_df):
    """Display real-time dashboard: Prices, Sentiment, Signals, Trade Summary."""
    
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 80)
    print(f"TRADING DASHBOARD - Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("=" * 80)
    
    
    prices = get_latest_prices()
    summary = get_trade_history_summary()
    
    
    print("\n LIVE PRICES (Last 2 Hours, Top 5 Symbols)")
    print("-" * 50)

    
    for asset_type, data in prices.items():
        print(f"\n{asset_type.upper()}:")
        
        if not data:
            print("  No data available.")
            continue

        for item in data:
            
            ts = item.get('timestamp')
            if hasattr(ts, 'strftime'):
                ts_str = ts.strftime('%Y-%m-%d %H:%M')
            else:
                ts_str = str(ts)[:16]

            if asset_type == 'forex':
                print(f"  {item.get('pair')}: {item.get('rate', 0):.5f} (Pip: {item.get('pip_value')}) - {ts_str}")
            else:
                key = 'symbol' if asset_type == 'crypto' else 'ticker'
                vol_key = 'volume_base' if asset_type == 'crypto' else 'volume'
                vol = item.get(vol_key) or 0
                price = item.get('price') or item.get('close') or 0.0
                
                try:
                    price_f = float(price)
                except Exception:
                    price_f = 0.0
                try:
                    vol_i = int(vol)
                except Exception:
                    vol_i = 0
                print(f"  {item.get(key)}: ${price_f:.2f} (Vol: {vol_i:,}) - {ts_str}")

    
    print("\n SENTIMENT ANALYSIS (Last 7 Days)")
    print("-" * 50)
    if sentiment_df.empty:
        print("  No sentiment data available.")
    else:
        avg_score = sentiment_df['sentiment_score'].mean()
        label = sentiment_label(avg_score)

        recent_articles = sentiment_df.tail(3)[['title', 'sentiment_label', 'publishedAt']].to_dict('records')
        print(f"  Overall Score: {avg_score:.3f} ({label})")
        print(f"  Total Articles: {len(sentiment_df)}")
        print("  Recent Articles:")
        for art in recent_articles:
            ts = art.get('publishedAt')
            if hasattr(ts, 'strftime'):
                ts_str = ts.strftime('%Y-%m-%d')
            else:
                ts_str = str(ts)[:10]
            title = (art.get('title') or '')[:60]
            label_art = art.get('sentiment_label') or ''
            print(f"    - {title}... [{label_art}] ({ts_str})")


    print("\n TRADING SIGNALS (Model Predictions)")
    print("-" * 80)
    print(" [Models actively learning every 60s with latest data]")
    balance = summary['balance']
    for asset_type in ['crypto', 'stocks', 'forex']:
        model = models.get(asset_type)
        cols = feature_columns.get(asset_type)
        if not model or not cols:
            print(f"\n{asset_type.upper()}: No model trained yet.")
            continue
        
        price_data = prices.get(asset_type, [])
        if not price_data:
            print(f"\n{asset_type.upper()}: No live prices.")
            continue
        latest_item = price_data[0]  
        current_price = latest_item['price' if asset_type != 'forex' else 'rate']
        symbol = latest_item['symbol' if asset_type == 'crypto' else 'ticker' if asset_type == 'stocks' else 'pair']
        
        signal, prob_up, prob_down, tp_pips, sl_pips, _, atr = generate_signal(
            model, cols, sentiment_df, asset_type, current_price, symbol
        )
        if tp_pips is None:  
            print(f"\n{asset_type.upper()} ({symbol}): HOLD | Prob Up: {prob_up:.2f}, Down: {prob_down:.2f} | ATR: {atr:.4f}")
            continue
        
        volume = calculate_position_size(asset_type, signal, current_price, sl_pips, balance, symbol)
        if volume == 0:
            print(f"\n{asset_type.upper()} ({symbol}): {signal} (Low balance; no volume)")
            continue
        
        pair_info = get_pair_info(symbol) if asset_type == 'forex' else {"pip_value": PIP_VALUE_DEFAULT}
        pip_value = pair_info['pip_value']
        sl_distance = sl_pips * pip_value
        tp_distance = tp_pips * pip_value
        if signal == "BUY":
            sl_price = current_price - sl_distance
            tp_price = current_price + tp_distance
        else:
            sl_price = current_price + sl_distance
            tp_price = current_price - tp_distance
        
    
            vol_type = "Lot" if asset_type == 'forex' else "Qty"
    
            
            price_str = f"{current_price:.5f}" if asset_type == 'forex' else f"{current_price:.2f}"
            tp_str = f"{tp_price:.5f}" if asset_type == 'forex' else f"{tp_price:.2f}"
            sl_str = f"{sl_price:.5f}" if asset_type == 'forex' else f"{sl_price:.2f}"
    
            print(f"\n{asset_type.upper()} ({symbol}):")
            print(f"  Signal: {signal} @ {price_str}")
            print(f"  TP: {tp_pips:.1f} pips ({tp_str})")
            print(f"  SL: {sl_pips:.1f} pips ({sl_str})")
            print(f"  {vol_type}: {volume} | Prob Up: {prob_up:.2f}, Down: {prob_down:.2f} | ATR: {atr:.4f}")
            print(f"  Risk: ${(balance * RISK_PER_TRADE):.2f} | Suggested: Execute if confident.")
    
    
    print("\n ACCOUNT & TRADE SUMMARY")
    print("-" * 50)
    print(f"  Balance: ${summary['balance']:.2f}")
    print(f"  Total P&L: ${summary['total_pnl']:.2f}")
    print(f"  Total Trades: {summary['total_trades']} | Wins: {summary['wins']} | Win Rate: {summary['win_rate']:.1f}%")
    print(f"  Open Trades: {summary['open_count']}")
    if summary['last_open']:
        print("  Last 3 Open Trades:")
        for trade in summary['last_open']:
            print(f"    {trade['id']}: {trade['signal']} {trade['symbol']} @ {trade['entry']:.5f}, Vol: {trade['volume']}, Open: {trade['open_time'][:16]}")
    if summary['recent_closed']:
        print("  Last 5 Closed Trades:")
        for trade in summary['recent_closed']:
            pnl = trade['pnl_usd']
            status = "WIN" if pnl > 0 else "LOSS"
            print(f"    {trade['id']}: {trade['signal']} {trade['symbol']}, Pips: {trade['pips']:.1f}, P&L: ${pnl:.2f} ({status}), Closed: {trade['exit_time'][:16]}")
    else:
        print("  No closed trades yet.")
    
    print("\n" + "=" * 80)
    print("Dashboard refreshes every 60s. Press Ctrl+C to stop.\n")


def run_exit_checks(prices):
    """Run trade exit checks."""
    check_trade_exits(prices)



class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.daily_stats = {
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'pnl': 0.0
        }
    
    def generate_daily_report(self):
        """Generate comprehensive daily performance report"""
        balance, total_trades, wins, total_pnl = load_account_state()
        open_trades = load_open_trades()
        history = load_trade_history()
        
        
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        avg_win = self._calculate_avg_win(history)
        avg_loss = self._calculate_avg_loss(history)
        profit_factor = self._calculate_profit_factor(history)
        max_drawdown = self._calculate_max_drawdown(history)
        
        report = f"""
         DAILY PERFORMANCE REPORT
        ===========================
        Date: {datetime.now().strftime('%Y-%m-%d')}
        
         ACCOUNT METRICS:
        Balance: ${balance:.2f}
        Total P&L: ${total_pnl:.2f}
        Open Trades: {len(open_trades)}
        
         TRADING METRICS:
        Total Trades: {total_trades}
        Win Rate: {win_rate:.1f}%
        Profit Factor: {profit_factor:.2f}
        Max Drawdown: {max_drawdown:.2f}%
        
         DAILY SUMMARY:
        Trades Today: {risk_manager.daily_trade_count}
        Daily P&L: ${risk_manager.daily_pnl:.2f}
        Consecutive Losses: {risk_manager.consecutive_losses}
        
         RISK METRICS:
        Daily Loss Limit: ${risk_manager.max_daily_loss:.2f}
        Account Drawdown Limit: {((ACCOUNT_BALANCE_START - balance) / ACCOUNT_BALANCE_START * 100):.1f}%
        """
        
        print(report)
        return report
    
    def _calculate_avg_win(self, history):
        wins = [t for t in history if t.get('pnl_usd', 0) > 0]
        return np.mean([t['pnl_usd'] for t in wins]) if wins else 0
    
    def _calculate_avg_loss(self, history):
        losses = [t for t in history if t.get('pnl_usd', 0) < 0]
        return np.mean([t['pnl_usd'] for t in losses]) if losses else 0
    
    def _calculate_profit_factor(self, history):
        gross_profit = sum(t['pnl_usd'] for t in history if t.get('pnl_usd', 0) > 0)
        gross_loss = abs(sum(t['pnl_usd'] for t in history if t.get('pnl_usd', 0) < 0))
        return gross_profit / gross_loss if gross_loss > 0 else float('inf')
    
    def _calculate_max_drawdown(self, history):
        if not history:
            return 0.0
        balances = [ACCOUNT_BALANCE_START]
        for trade in history:
            balances.append(balances[-1] + trade.get('pnl_usd', 0))
        
        peak = ACCOUNT_BALANCE_START
        max_dd = 0.0
        for balance in balances:
            if balance > peak:
                peak = balance
            dd = (peak - balance) / peak * 100
            if dd > max_dd:
                max_dd = dd
                
        return max_dd


performance_monitor = PerformanceMonitor()












def enhanced_main_loop():
    """Main trading loop with proper variable scope handling"""
    global running  
    
    
    print("Starting ENHANCED trading system with safety protocols...")
    
    
    for path in CSV_FILES.values():
        if not os.path.exists(path):
            print(f"Creating empty CSV: {path}")
            if path == CSV_FILES['crypto']:
                pd.DataFrame(columns=["event_time", "symbol", "open", "high", "low", "close", "volume_base", "volume_quote"]).to_csv(path, index=False)
            elif path == CSV_FILES['stocks']:
                pd.DataFrame(columns=["timestamp", "ticker", "open", "high", "low", "close", "volume"]).to_csv(path, index=False)
            elif path == CSV_FILES['forex']:
                pd.DataFrame(columns=["timestamp", "pair", "base", "quote", "rate", "pip_value", "name"]).to_csv(path, index=False)
    
    
    print("Starting data streams...")
    
    
    
    print("Loading sentiment data...")
    sentiment_df = load_sentiment_data()
    sentiment_df = refresh_sentiment()
    
    
    print("Initial training models...")
    models = {}
    feature_columns = {}
    for asset_type in ['crypto', 'stocks', 'forex']:
        model, cols = train_model_from_data(asset_type, sentiment_df)
        if model:
            models[asset_type] = model
            feature_columns[asset_type] = cols
            print(f" Initial {asset_type} model trained with {len(cols)} features")
        else:
            print(f"Skipping {asset_type} model (no data).")
    
    
    balance, total_trades, wins, total_pnl = load_account_state()
    print(f"Account loaded: Balance ${balance:.2f}")
    
    
    last_sentiment_refresh = time.time()
    
    print("Enhanced main loop starting...")
    
    try:
        while running:  
            
            if not perform_system_health_check():
                print(" System health check failed - activating emergency stop")
                circuit_breaker.emergency_stop_trading()
                break
                
            
            market_ok, market_reason = circuit_breaker.market_condition_check()
            if not market_ok:
                print(f" Market conditions not optimal: {market_reason}")
                time.sleep(300)
                continue
                
            
            if time.time() - last_sentiment_refresh > 14400:
                print("Refreshing sentiment...")
                sentiment_df = refresh_sentiment()
                last_sentiment_refresh = time.time()
            
            
            # ========== ADD INCREMENTAL LEARNING HERE ==========
            print(" Updating models with latest data...")
            for asset_type in ['crypto', 'stocks', 'forex']:
                if asset_type in models:  # Only update if model exists
                    models, feature_columns = incremental_model_update(
                        models, feature_columns, asset_type, sentiment_df
                    )
            # ========== END INCREMENTAL LEARNING ==========
            
            
            prices = get_latest_prices()
            run_exit_checks(prices)

            
            if AUTO_RESET_TRADES:
                
                if not hasattr(enhanced_main_loop, '_last_reset'):
                    enhanced_main_loop._last_reset = 0
                if time.time() - enhanced_main_loop._last_reset >= AUTO_RESET_INTERVAL:
                    enhanced_main_loop._last_reset = time.time()
                    try:
                        auto_reset_open_trades(prices)
                    except Exception as e:
                        print(f"Auto-reset error: {e}")
            
            
            current_time = datetime.now()
            if current_time.hour == 9 and current_time.minute == 30:
                risk_manager.reset_daily()
                print(" Daily counters reset")
            
            
            balance, _, _, _ = load_account_state()  
            
            for asset_type in ['crypto', 'stocks', 'forex']:
                model = models.get(asset_type)
                cols = feature_columns.get(asset_type)
                
                if not model or not cols or not prices.get(asset_type):
                    continue
                    
                
                latest_item = prices[asset_type][0] if prices[asset_type] else None
                if not latest_item:
                    continue
                    
                current_price = latest_item.get('price') or latest_item.get('rate')
                symbol = latest_item.get('symbol') or latest_item.get('ticker' if asset_type == 'stocks' else 'pair')
                
                if not current_price or not symbol:
                    continue
                
                
                signal, prob_up, prob_down, tp_pips, sl_pips, _, atr = generate_signal(
                    model, cols, sentiment_df, asset_type, current_price, symbol
                )
                
                if signal in ["BUY", "SELL"] and tp_pips is not None and sl_pips is not None:
                    volume = calculate_position_size(asset_type, signal, current_price, sl_pips, balance, symbol)
                    
                    if volume > 0:
                        
                        trade = execute_trade(asset_type, symbol, signal, current_price, tp_pips, sl_pips, volume, atr)
                        if trade:
                            print(f" Auto-executed {signal} for {symbol}")
            
            
            if datetime.now().hour == 16 and datetime.now().minute == 0:
                performance_monitor.generate_daily_report()
            
            
            display_dashboard(models, feature_columns, sentiment_df)
            
            time.sleep(60)   
    except KeyboardInterrupt:
        print("\n Shutdown requested by user...")
    except Exception as e:
        print(f" Critical error in main loop: {e}")
        import traceback
        traceback.print_exc()
        circuit_breaker.emergency_stop_trading()
        performance_monitor.generate_daily_report()
    finally:
        
        running = False
        balance, total_trades, wins, total_pnl = load_account_state()
        save_account_state(balance, total_trades, wins, total_pnl)
        print(" Final account state saved")
        print(" Enhanced trading system shutdown complete")
def perform_system_health_check():
    """Comprehensive system health check - FIXED VERSION"""
    try:
        print(" Running system health check...")
        
        
        for file_type, file_path in CSV_FILES.items():
            if not os.path.exists(file_path):
                print(f" Missing {file_type} data file: {file_path}")
                
            else:
                
                file_size = os.path.getsize(file_path)
                if file_size < 100:  
                    print(f" {file_type} data file seems empty: {file_path}")
        
        
        try:
            balance, total_trades, wins, total_pnl = load_account_state()
            if balance <= ACCOUNT_BALANCE_START * 0.5:  
                print(" Account balance below safety threshold")
                return False
            print(f" Account health: ${balance:.2f}")
        except Exception as account_error:
            print(f" Account health check warning: {account_error}")
            
            
        
        prices = get_latest_prices()
        if not prices:
            print(" No price data available")
        else:
            data_points = sum(len(asset_data) for asset_data in prices.values())
            print(f" Data health: {data_points} price points available")
        
        
        try:
            sentiment_df = load_sentiment_data()
            if sentiment_df.empty:
                print(" No sentiment data available")
            else:
                print(f" Sentiment health: {len(sentiment_df)} articles")
        except Exception as sentiment_error:
            print(f" Sentiment health warning: {sentiment_error}")
        
        print(" System health check passed")
        return True
        
    except Exception as e:
        print(f" Health check failed: {e}")
        return False  



if __name__ == "__main__":
    
    running = True
    
    
    print("Starting data streams...")
    
    
    try:
        enhanced_main_loop()
    except Exception as e:
        print(f" Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        running = False
        print(" System shutdown complete")
