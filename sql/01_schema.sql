-- Marketing Data Platform — PostgreSQL schema
-- Serving layer: populated from Gold parquet by the load step

CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(10) PRIMARY KEY,
    email_masked    VARCHAR(100),
    country         CHAR(2),
    city            VARCHAR(100),
    preferred_channel VARCHAR(20),
    signup_date     DATE,
    last_purchase_date DATE,
    total_spend     NUMERIC(12,2) DEFAULT 0,
    purchase_count  INT DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customers_audit (
    audit_id        BIGSERIAL PRIMARY KEY,
    customer_id     VARCHAR(10) NOT NULL,
    operation       CHAR(1) NOT NULL,   -- U=Update, D=Delete
    old_data        JSONB,
    changed_at      TIMESTAMPTZ DEFAULT NOW(),
    changed_by      VARCHAR(100) DEFAULT CURRENT_USER
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id     VARCHAR(10) PRIMARY KEY,
    name            VARCHAR(200),
    channel         VARCHAR(20),
    product         VARCHAR(100),
    budget          NUMERIC(12,2),
    start_date      DATE,
    end_date        DATE,
    duration_days   INT,
    target_segment  VARCHAR(50),
    status          VARCHAR(20),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaign_events (
    event_id        UUID PRIMARY KEY,
    customer_id     VARCHAR(10) REFERENCES customers(customer_id),
    campaign_id     VARCHAR(10) REFERENCES campaigns(campaign_id),
    event_type      VARCHAR(20) NOT NULL,
    channel         VARCHAR(20),
    device          VARCHAR(20),
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_date      DATE NOT NULL,
    revenue         NUMERIC(10,2) DEFAULT 0,
    is_conversion   BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_events_customer   ON campaign_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_events_campaign   ON campaign_events(campaign_id);
CREATE INDEX IF NOT EXISTS idx_events_date       ON campaign_events(event_date);
CREATE INDEX IF NOT EXISTS idx_events_type_date  ON campaign_events(event_type, event_date);

CREATE TABLE IF NOT EXISTS customer_segments (
    customer_id     VARCHAR(10) PRIMARY KEY REFERENCES customers(customer_id),
    segment         VARCHAR(50) NOT NULL,
    r_score         SMALLINT,
    f_score         SMALLINT,
    m_score         SMALLINT,
    rfm_score       SMALLINT,
    recency_days    INT,
    frequency       INT,
    monetary        NUMERIC(12,2),
    segmented_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS campaign_metrics (
    campaign_id         VARCHAR(10) PRIMARY KEY REFERENCES campaigns(campaign_id),
    impressions         INT DEFAULT 0,
    clicks              INT DEFAULT 0,
    opens               INT DEFAULT 0,
    conversions         INT DEFAULT 0,
    unsubscribes        INT DEFAULT 0,
    total_revenue       NUMERIC(12,2) DEFAULT 0,
    unique_customers    INT DEFAULT 0,
    ctr                 NUMERIC(6,2),   -- click-through rate %
    cvr                 NUMERIC(6,2),   -- conversion rate %
    cost_per_conversion NUMERIC(10,2),
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);
