"""Tests for bronze ingestion layer."""
import json
from pathlib import Path

import pytest


def _make_csv(tmp_path: Path) -> Path:
    p = tmp_path / "crm.csv"
    p.write_text(
        "customer_id,email,first_name,last_name,country,total_spend,purchase_count,is_active,preferred_channel,signup_date,last_purchase_date,city,phone\n"
        "C00001,a@x.com,Ada,Lovelace,NG,250.0,5,true,email,2024-01-01,2024-10-01,Lagos,+234123\n"
        "C00002,b@y.com,Grace,Hopper,GH,0.0,0,false,sms,2023-06-15,2024-09-01,Accra,+233456\n"
        "C00001,c@z.com,Ada,L,NG,300.0,7,true,email,2024-01-01,2024-11-01,Lagos,+234999\n"  # duplicate
    )
    return p


def _make_campaigns_json(tmp_path: Path) -> Path:
    p = tmp_path / "campaigns.json"
    p.write_text(json.dumps([
        {
            "campaign_id": "CAMP0001", "name": "Test Campaign", "channel": "email",
            "product": "Pro Plan", "budget": 1000.0, "start_date": "2024-01-01",
            "end_date": "2024-01-31", "target_segment": "Champions", "status": "active",
        }
    ]))
    return p


def _make_events_jsonl(tmp_path: Path) -> Path:
    p = tmp_path / "events.json"
    lines = [
        {"event_id": "e1", "customer_id": "C00001", "campaign_id": "CAMP0001",
         "event_type": "click", "channel": "email", "device": "mobile",
         "event_timestamp": "2024-10-15T12:00:00", "revenue": 0.0, "metadata": "{}"},
        {"event_id": "e2", "customer_id": "C00001", "campaign_id": "CAMP0001",
         "event_type": "conversion", "channel": "email", "device": "desktop",
         "event_timestamp": "2024-10-15T13:00:00", "revenue": 99.0, "metadata": "{}"},
        {"event_id": "e3", "customer_id": None, "campaign_id": "CAMP0001",
         "event_type": "impression", "channel": "sms", "device": "mobile",
         "event_timestamp": "2024-10-14T10:00:00", "revenue": 0.0, "metadata": "{}"},
    ]
    p.write_text("\n".join(json.dumps(r) for r in lines))
    return p


def _make_web_parquet(spark, tmp_path: Path) -> Path:
    import pandas as pd
    df = pd.DataFrame([
        {"session_id": "s1", "customer_id": "C00001", "page": "/home", "referrer": "google",
         "session_duration_s": 120, "pageviews": 3, "bounce": False, "event_date": "2024-10-01"},
        {"session_id": "s2", "customer_id": None, "page": "/pricing", "referrer": "direct",
         "session_duration_s": 60, "pageviews": 1, "bounce": True, "event_date": "2024-10-02"},
    ])
    p = tmp_path / "web.parquet"
    df.to_parquet(p, index=False)
    return p


class TestIngest:
    def test_ingest_crm_rows(self, spark, tmp_data_dir):
        from pipelines import ingest
        p = _make_csv(tmp_data_dir)
        df = ingest.ingest_crm(spark, str(p))
        assert df.count() == 3  # bronze = raw, dedup happens in silver
        assert "_source" in df.columns
        assert "_ingested_at" in df.columns

    def test_ingest_campaigns(self, spark, tmp_data_dir):
        from pipelines import ingest
        p = _make_campaigns_json(tmp_data_dir)
        df = ingest.ingest_campaigns(spark, str(p))
        assert df.count() == 1
        assert "campaign_id" in df.columns

    def test_ingest_events(self, spark, tmp_data_dir):
        from pipelines import ingest
        p = _make_events_jsonl(tmp_data_dir)
        df = ingest.ingest_events(spark, str(p))
        assert df.count() == 3
        assert "event_date" in df.columns

    def test_ingest_web_analytics(self, spark, tmp_data_dir):
        from pipelines import ingest
        p = _make_web_parquet(spark, tmp_data_dir)
        df = ingest.ingest_web_analytics(spark, str(p))
        assert df.count() == 2
        assert "_source" in df.columns
