import os
import glob
import time
import json
from datetime import datetime
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def load_all_csvs(data_dir):
    paths = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    if not paths:
        raise FileNotFoundError(f"No CSV files in {data_dir}")
    dfs = [pd.read_csv(p) for p in paths]
    return pd.concat(dfs, ignore_index=True)

def prepare_features(df, label_column):
    if label_column not in df.columns:
        raise KeyError(f"Label column '{label_column}' not found in data")
    y = df[label_column]
    X = df.drop(columns=[label_column])
    X = pd.get_dummies(X)  # simple encoding for mixed types
    X = X.fillna(0)
    return X, y

def train_and_evaluate(data_dir, model_dir, label_column):
    df = load_all_csvs(data_dir)
    X, y = prepare_features(df, label_column)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y if len(y.unique())>1 else None)
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    acc = float(accuracy_score(y_test, preds))
    os.makedirs(model_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    model_path = os.path.join(model_dir, f"model_{ts}.joblib")
    joblib.dump({"model": model, "columns": X.columns.tolist()}, model_path)
    return {"metric_name": "accuracy", "metric_value": acc, "model_path": model_path, "timestamp": ts}
