"""Generate synthetic marketing data for local development and testing."""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

ROOT = Path(__file__).resolve().parent
RAW = ROOT / "raw"
RAW.mkdir(exist_ok=True)

NUM_CUSTOMERS = 2000
NUM_CAMPAIGNS = 20
NUM_EVENTS = 50000

SEGMENTS = ["Champions", "Loyal", "Potential Loyalist", "At Risk", "Hibernating"]
CHANNELS = ["email", "sms", "push", "social", "paid_search"]
EVENT_TYPES = ["impression", "click", "open", "conversion", "unsubscribe"]
PRODUCTS = ["Basic Plan", "Pro Plan", "Enterprise Plan", "Add-on Storage", "Support Pack"]


def _random_date(start_days_ago: int = 180) -> datetime:
    return datetime.utcnow() - timedelta(days=random.randint(0, start_days_ago))


def generate_customers() -> pd.DataFrame:
    rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup = _random_date(730)
        rows.append({
            "customer_id": f"C{i:05d}",
            "email": fake.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone": fake.phone_number(),
            "country": random.choice(["NG", "GH", "KE", "ZA", "EG"]),
            "city": fake.city(),
            "signup_date": signup.strftime("%Y-%m-%d"),
            "total_spend": round(random.uniform(0, 5000), 2),
            "last_purchase_date": (_random_date(60)).strftime("%Y-%m-%d"),
            "purchase_count": random.randint(0, 50),
            "is_active": random.choice([True, True, True, False]),
            "preferred_channel": random.choice(CHANNELS),
        })
    df = pd.DataFrame(rows)
    path = RAW / "crm_customers.csv"
    df.to_csv(path, index=False)
    print(f"  CRM customers → {path} ({len(df)} rows)")
    return df


def generate_campaigns() -> list[dict]:
    campaigns = []
    for i in range(1, NUM_CAMPAIGNS + 1):
        start = _random_date(120)
        campaigns.append({
            "campaign_id": f"CAMP{i:04d}",
            "name": f"{random.choice(['Q4', 'Q3', 'Promo', 'Holiday', 'Launch'])} {fake.bs().title()} Campaign",
            "channel": random.choice(CHANNELS),
            "product": random.choice(PRODUCTS),
            "budget": round(random.uniform(500, 10000), 2),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": (start + timedelta(days=random.randint(7, 30))).strftime("%Y-%m-%d"),
            "target_segment": random.choice(SEGMENTS),
            "status": random.choice(["active", "completed", "paused"]),
        })
    path = RAW / "campaigns.json"
    path.write_text(json.dumps(campaigns, indent=2))
    print(f"  Campaigns → {path} ({len(campaigns)} records)")
    return campaigns


def generate_events(customer_ids: list[str], campaign_ids: list[str]) -> None:
    rows = []
    for _ in range(NUM_EVENTS):
        event_time = _random_date(90)
        rows.append({
            "event_id": fake.uuid4(),
            "customer_id": random.choice(customer_ids),
            "campaign_id": random.choice(campaign_ids),
            "event_type": random.choice(EVENT_TYPES),
            "channel": random.choice(CHANNELS),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "event_timestamp": event_time.isoformat(),
            "revenue": round(random.uniform(0, 200), 2) if random.random() < 0.08 else 0.0,
            "metadata": json.dumps({"session": fake.uuid4(), "ip": fake.ipv4()}),
        })

    path = RAW / "campaign_events.json"
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"  Campaign events → {path} ({len(rows)} rows, JSONL)")


def generate_web_analytics(customer_ids: list[str]) -> None:
    rows = []
    pages = ["/home", "/pricing", "/signup", "/features", "/blog", "/contact", "/checkout"]
    for _ in range(20000):
        rows.append({
            "session_id": fake.uuid4(),
            "customer_id": random.choice(customer_ids + [None] * 500),
            "page": random.choice(pages),
            "referrer": random.choice(["google", "facebook", "direct", "email", "linkedin"]),
            "session_duration_s": random.randint(5, 900),
            "pageviews": random.randint(1, 15),
            "bounce": random.random() < 0.4,
            "event_date": _random_date(60).strftime("%Y-%m-%d"),
        })
    df = pd.DataFrame(rows)
    path = RAW / "web_analytics.parquet"
    df.to_parquet(path, index=False)
    print(f"  Web analytics → {path} ({len(df)} rows)")


if __name__ == "__main__":
    print("Generating synthetic marketing data...")
    customers_df = generate_customers()
    campaign_data = generate_campaigns()
    generate_events(
        customers_df["customer_id"].tolist(),
        [c["campaign_id"] for c in campaign_data],
    )
    generate_web_analytics(customers_df["customer_id"].tolist())
    print("Done.")
