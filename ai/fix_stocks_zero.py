
import pandas as pd
df = pd.read_csv('stocks.csv')
for col in ['close','open','high','low','price']:
    if col in df.columns:
        df[col].replace(0, pd.NA, inplace=True)
df.dropna(subset=['close'], inplace=False)   
df.to_csv('stocks.csv', index=False)
print("stocks.csv cleaned (zeros -> NaN).")