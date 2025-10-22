import os
import json
from datetime import datetime, timedelta
import pandas as pd

SENTIMENT_FILE = 'sentiment.csv'
EconEventsFile = 'econ_events.json'

HIGH_IMPACT_KEYWORDS = [
    'nfp', 'nonfarm', 'unemployment', 'cpi', 'inflation', 'fed', 'fomc',
    'interest rate', 'rate hike', 'rate cut', 'gdp', 'recession', 'default'
]

def _load_sentiment_recent(hours=3):
    if not os.path.exists(SENTIMENT_FILE):
        return []
    try:
        df = pd.read_csv(SENTIMENT_FILE)
        if 'publishedAt' in df.columns:
            df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce', utc=True)
            cutoff = datetime.utcnow().replace(tzinfo=None) - pd.to_timedelta(hours, unit='h')
            recent = df[df['publishedAt'].dt.tz_localize(None) >= cutoff]
            return recent.to_dict('records')
    except Exception:
        return []
    return []

def _keyword_match_in_articles(articles):
    for a in articles:
        txt = " ".join(filter(None, [str(a.get('title','')), str(a.get('description',''))])).lower()
        for kw in HIGH_IMPACT_KEYWORDS:
            if kw in txt:
                return True
    return False

def _is_first_friday_of_month(dt):
    
    return dt.weekday() == 4 and 1 <= dt.day <= 7

def _load_custom_events():
    if os.path.exists(EconEventsFile):
        try:
            with open(EconEventsFile, 'r') as f:
                return json.load(f)
        except Exception:
            return []
    return []

def is_major_event_ongoing(window_hours=2):
    """
    Returns True if:
      - Recent articles (last window_hours) mention high-impact keywords, OR
      - It's (approx) NFP time (first Friday ~13:30 UTC default), OR
      - A user-supplied econ_events.json contains an event timestamp within window_hours.
    """
    try:
        
        recent = _load_sentiment_recent(hours=window_hours)
        if recent and _keyword_match_in_articles(recent):
            return True

        
        now = datetime.utcnow()
        if _is_first_friday_of_month(now):
            nfp_time = now.replace(hour=13, minute=30, second=0, microsecond=0)
            delta = abs((now - nfp_time).total_seconds()) / 60.0
            if delta <= 45:
                return True

        
        events = _load_custom_events()
        for ev in events:
            try:
                ts = datetime.fromisoformat(ev.get('timestamp'))
                if abs((datetime.utcnow() - ts).total_seconds()) <= window_hours * 3600:
                    impact = ev.get('impact', 'high').lower()
                    if impact in ('high', 'major'):
                        return True
            except Exception:
                continue

        return False
    except Exception:
        return False