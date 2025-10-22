import os
from dotenv import load_dotenv
import datetime as dt
import requests
import pandas as pd
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

load_dotenv()
news_api = os.getenv("news_api")
hf_token = os.getenv("hf_token") 

queries = ["forex", "stock market", "gold", "oil", "currency", "dow jones", "nasdaq", "s&p 500"]
assets = ["Apple", "Google", "Microsoft", "Amazon", "Tesla", "Gold", "Oil", "Dow Jones", "NASDAQ", "S&P 500"]

def fetch_market_headlines(from_d, to_d, page=1):
    all_articles = []
    for q in queries:
        params = {
            "q": q,
            "from": from_d,
            "to": to_d,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 100,
            "page": page,
            "apiKey": news_api
        }
        try:
            r = requests.get("https://newsapi.org/v2/everything", params=params)
            r.raise_for_status()
            articles = r.json().get("articles", [])
            for a in articles:
                a['query'] = q
                all_articles.append(a)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {q}: {e}")
    return all_articles

def clean_text(article):
    text = " ".join(filter(None, [article.get("title", ""), article.get("description", "")]))
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def vader_sentiment(texts):
    vader = SentimentIntensityAnalyzer()
    return [vader.polarity_scores(t)["compound"] for t in texts]

def textblob_sentiment(texts):
    return [TextBlob(t).sentiment.polarity for t in texts]

def finbert_api_sentiment(texts):
    url = "https://api-inference.huggingface.co/models/yiyanghkust/finbert-tone"
    headers = {"Authorization": f"Bearer {hf_token}"}
    scores = []
    for t in texts:
        payload = {"inputs": t[:512]}
        try:
            response = requests.post(url, headers=headers, json=payload)
            result = response.json()
            label = result[0]['label'].lower()
            score = result[0]['score']
            if label == "positive":
                scores.append(score)
            elif label == "negative":
                scores.append(-score)
            else:
                scores.append(0)
        except:
            scores.append(0)
    return scores

def sentiment_label(score):
    if score < -0.5:
        return "Strongly Negative"
    elif score < -0.05:
        return "Negative"
    elif score <= 0.05:
        return "Neutral"
    elif score <= 0.5:
        return "Positive"
    else:
        return "Strongly Positive"

def detect_asset(title):
    for asset in assets:
        if asset.lower() in title.lower():
            return asset
    return "Other"

def aggregate_daily_sentiment(articles, scores):
    df = pd.DataFrame({
        "date": [pd.to_datetime(a["publishedAt"]).date() for a in articles],
        "score": scores
    })
    daily_sent = df.groupby("date").mean()
    return daily_sent

from_date = (dt.date.today() - dt.timedelta(days=7)).isoformat()
to_date = dt.date.today().isoformat()

articles = fetch_market_headlines(from_date, to_date)
df = pd.DataFrame(articles)
df["source"] = df["source"].apply(lambda x: x["name"] if isinstance(x, dict) else x)

texts = [clean_text(a) for a in articles]
vader_scores = vader_sentiment(texts)
tb_scores = textblob_sentiment(texts)
finbert_scores = finbert_api_sentiment(texts)

ensemble_scores = [(v + t + f)/3 for v, t, f in zip(vader_scores, tb_scores, finbert_scores)]
df["sentiment_score"] = ensemble_scores
df["sentiment_label"] = [sentiment_label(s) for s in ensemble_scores]
df["asset"] = df["title"].apply(detect_asset)

daily_sentiment = aggregate_daily_sentiment(articles, ensemble_scores)

print(df[["publishedAt", "title", "asset", "sentiment_score", "sentiment_label"]].head(20))
print(f"\nTotal articles fetched: {len(df)}")
df.to_csv("sentiment.csv", index=False)
print(daily_sentiment)