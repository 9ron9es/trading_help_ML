import pandas_ta as ta
import pandas as pd

def rsi(data):
    data["RSI"] = ta.rsi(data["close"], length=14)
    return data

def macd(data):
    macd = ta.macd(data["close"])
    data = pd.concat([data, macd], axis=1)
    return data

def ma(data):
    data["SMA50"] = ta.sma(data["close"], length=50)
    data["EMA20"] = ta.ema(data["close"], length=20)
    return data

def bb(data):
    bb = ta.bbands(data["close"], length=20, std=2)
    data = pd.concat([data, bb], axis=1)
    return data

def adx(data):
    adx = ta.adx(data["high"], data["low"], data["close"], length=14)
    data = pd.concat([data, adx], axis=1)
    return data

def cmf(data, volume_col='volume_base'):
    if volume_col not in data.columns:
        # Skip CMF if volume not available
        data["CMF"] = 0
        return data
    data["CMF"] = ta.cmf(data["high"], data["low"], data["close"], data[volume_col])
    return data


def obv(data, volume_col='volume_base'):
    if volume_col not in data.columns:
        data["OBV"] = 0
        return data
    data["OBV"] = ta.obv(data["close"], data[volume_col])
    return data


