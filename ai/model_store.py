import os
import json
from datetime import datetime, timedelta

def _load_metrics(metrics_file):
    if not os.path.exists(metrics_file):
        return []
    with open(metrics_file, "r") as f:
        return json.load(f)

def _save_metrics(metrics_file, metrics):
    os.makedirs(os.path.dirname(metrics_file), exist_ok=True)
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

def record_run(metrics_file, entry):
    metrics = _load_metrics(metrics_file)
    metrics.append(entry)
    _save_metrics(metrics_file, metrics)

def is_trusted(metrics_file, metric_name, threshold, trust_days):
    metrics = _load_metrics(metrics_file)
    if not metrics:
        return False
    now = datetime.utcnow()
    start = now - timedelta(days=trust_days)
    # group by day: consider a day satisfied if any run on that UTC date meets threshold
    satisfied_days = set()
    for m in metrics:
        if m.get("metric_name") != metric_name:
            continue
        ts = datetime.strptime(m["timestamp"], "%Y%m%dT%H%M%SZ")
        if ts >= start and m.get("metric_value", 0) >= threshold:
            satisfied_days.add(ts.date())
    # need trust_days distinct UTC dates satisfied
    return len(satisfied_days) >= trust_days

def save_trust_flag(model_dir, model_path, trusted):
    meta = {"model_path": model_path, "trusted": trusted, "checked_at": datetime.utcnow().isoformat() + "Z"}
    meta_path = os.path.join(model_dir, "latest_model_meta.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    return meta_path
