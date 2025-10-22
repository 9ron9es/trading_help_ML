


import os
import pandas as pd
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import forex_pairs

load_dotenv()

forex_api_key = os.getenv("forex_api_key")

def fetch_forex_rates():
    major_pairs = forex_pairs.get_major_pairs()
    
    forex_data = []
    
    for pair, info in major_pairs.items():
        base = pair.split('/')[0]
        quote = pair.split('/')[1]
        
        try:
            url = f"https://api.exchangerate-api.com/v4/latest/{base}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                rate = data['rates'].get(quote, None)
                
                if rate is not None:
                    try:
                        rate_f = float(rate)
                    except Exception:
                        rate_f = None

                    
                    if rate_f and rate_f > 0:
                        forex_data.append({
                            'timestamp': datetime.utcnow().isoformat(),
                            'pair': pair,
                            'base': base,
                            'quote': quote,
                            'rate': rate_f,
                            'pip_value': info.get('pip_value'),
                            'name': info.get('name')
                        })
                        print(f"{pair}: {rate_f:.6f}")
                    else:
                        print(f"Skipping {pair}, invalid rate: {rate}")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error fetching {pair}: {e}")
    
    return forex_data

def save_to_csv(forex_data, filename='forex.csv'):
    if not forex_data:
        return None
    df = pd.DataFrame(forex_data)
    
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename, low_memory=False)
        df = pd.concat([existing_df, df], ignore_index=True, sort=False)
    
    
    cols = ['timestamp','pair','base','quote','rate','pip_value','name']
    df = df.loc[:, [c for c in cols if c in df.columns]]
    df.to_csv(filename, index=False)
    print(f"\nForex data saved to {filename}")
    return df

def run_forex_stream(update_interval=60):
    print("=" * 60)
    print("FOREX DATA STREAMING")
    print("=" * 60)
    
    while True:
        try:
            print(f"\n[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}] Fetching forex rates...")
            forex_data = fetch_forex_rates()
            
            if forex_data:
                save_to_csv(forex_data)
            
            print(f"\nNext update in {update_interval} seconds...")
            time.sleep(update_interval)
            
        except KeyboardInterrupt:
            print("\nForex streaming stopped.")
            break
        except Exception as e:
            print(f"Error in forex stream: {e}")
            time.sleep(update_interval)

if __name__ == "__main__":
    run_forex_stream(update_interval=60)