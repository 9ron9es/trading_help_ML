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

minor_forex_pairs = {
    "EUR/AUD": {"name": "Euro / Australian Dollar", "pip_value": 0.0001},
    "EUR/CAD": {"name": "Euro / Canadian Dollar", "pip_value": 0.0001},
    "EUR/CHF": {"name": "Euro / Swiss Franc", "pip_value": 0.0001},
    "GBP/AUD": {"name": "British Pound / Australian Dollar", "pip_value": 0.0001},
    "GBP/CAD": {"name": "British Pound / Canadian Dollar", "pip_value": 0.0001},
    "GBP/CHF": {"name": "British Pound / Swiss Franc", "pip_value": 0.0001},
    "AUD/JPY": {"name": "Australian Dollar / Japanese Yen", "pip_value": 0.01},
    "CAD/JPY": {"name": "Canadian Dollar / Japanese Yen", "pip_value": 0.01},
    "CHF/JPY": {"name": "Swiss Franc / Japanese Yen", "pip_value": 0.01},
    "NZD/JPY": {"name": "New Zealand Dollar / Japanese Yen", "pip_value": 0.01}
}

exotic_forex_pairs = {
    "USD/TRY": {"name": "US Dollar / Turkish Lira", "pip_value": 0.0001},
    "USD/ZAR": {"name": "US Dollar / South African Rand", "pip_value": 0.0001},
    "USD/MXN": {"name": "US Dollar / Mexican Peso", "pip_value": 0.0001},
    "USD/SGD": {"name": "US Dollar / Singapore Dollar", "pip_value": 0.0001},
    "USD/HKD": {"name": "US Dollar / Hong Kong Dollar", "pip_value": 0.0001}
}

def get_all_pairs():
    return {**major_forex_pairs, **minor_forex_pairs, **exotic_forex_pairs}

def get_major_pairs():
    return major_forex_pairs

def get_pair_info(pair):
    all_pairs = get_all_pairs()
    return all_pairs.get(pair, None)
