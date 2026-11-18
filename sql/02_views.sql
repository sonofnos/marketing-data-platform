-- Reporting views for the marketing data platform

-- Campaign performance: joins metrics with campaign metadata
CREATE OR REPLACE VIEW campaign_performance_vw AS
SELECT
    c.campaign_id,
    c.name                                              AS campaign_name,
    c.channel,
    c.product,
    c.budget,
    c.start_date,
    c.end_date,
    c.status,
    c.target_segment,
    COALESCE(m.impressions, 0)                          AS impressions,
    COALESCE(m.clicks, 0)                               AS clicks,
    COALESCE(m.conversions, 0)                          AS conversions,
    COALESCE(m.total_revenue, 0)                        AS total_revenue,
    COALESCE(m.ctr, 0)                                  AS ctr_pct,
    COALESCE(m.cvr, 0)                                  AS cvr_pct,
    COALESCE(m.cost_per_conversion, 0)                  AS cost_per_conversion,
    CASE
        WHEN COALESCE(m.total_revenue, 0) = 0 THEN 0
        ELSE ROUND((m.total_revenue - c.budget) / NULLIF(c.budget, 0) * 100, 2)
    END                                                 AS roi_pct,
    COALESCE(m.unique_customers, 0)                     AS unique_customers
FROM campaigns c
LEFT JOIN campaign_metrics m ON c.campaign_id = m.campaign_id;


-- Customer engagement: last 90 days activity summary per customer
CREATE OR REPLACE VIEW customer_engagement_vw AS
SELECT
    cu.customer_id,
    cu.country,
    cu.preferred_channel,
    cu.is_active,
    cs.segment,
    cs.rfm_score,
    COUNT(e.event_id)                                   AS total_events_90d,
    COUNT(e.event_id) FILTER (WHERE e.event_type = 'click')         AS clicks_90d,
    COUNT(e.event_id) FILTER (WHERE e.event_type = 'conversion')    AS conversions_90d,
    SUM(e.revenue)                                      AS revenue_90d,
    MAX(e.event_date)                                   AS last_active_date,
    CURRENT_DATE - MAX(e.event_date)                    AS days_since_last_event
FROM customers cu
LEFT JOIN customer_segments cs ON cu.customer_id = cs.customer_id
LEFT JOIN campaign_events e
    ON cu.customer_id = e.customer_id
    AND e.event_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY cu.customer_id, cu.country, cu.preferred_channel, cu.is_active, cs.segment, cs.rfm_score;


-- High-value customers: Champions and Loyal segments with spend data
CREATE OR REPLACE VIEW high_value_customers_vw AS
SELECT
    cu.customer_id,
    cu.email_masked,
    cu.country,
    cu.preferred_channel,
    cs.segment,
    cs.rfm_score,
    cs.monetary                                         AS lifetime_revenue,
    cs.recency_days,
    cs.frequency                                        AS total_conversions,
    RANK() OVER (PARTITION BY cu.country ORDER BY cs.monetary DESC) AS revenue_rank_in_country
FROM customers cu
JOIN customer_segments cs ON cu.customer_id = cs.customer_id
WHERE cs.segment IN ('Champions', 'Loyal')
  AND cu.is_active = TRUE;


-- Segment summary: distribution and monetisation by segment
CREATE OR REPLACE VIEW segment_summary_vw AS
SELECT
    cs.segment,
    COUNT(*)                                            AS customer_count,
    ROUND(AVG(cs.rfm_score), 2)                        AS avg_rfm_score,
    ROUND(AVG(cs.monetary), 2)                         AS avg_lifetime_revenue,
    SUM(cs.monetary)                                   AS total_segment_revenue,
    ROUND(AVG(cs.recency_days), 1)                     AS avg_recency_days,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM customer_segments cs
GROUP BY cs.segment
ORDER BY avg_rfm_score DESC;


-- Channel attribution: revenue and conversions by channel for active campaigns
CREATE OR REPLACE VIEW channel_attribution_vw AS
SELECT
    e.channel,
    COUNT(DISTINCT e.campaign_id)                       AS campaign_count,
    COUNT(DISTINCT e.customer_id)                       AS reached_customers,
    COUNT(e.event_id) FILTER (WHERE e.event_type = 'conversion') AS conversions,
    SUM(e.revenue)                                      AS total_revenue,
    ROUND(SUM(e.revenue) / NULLIF(COUNT(DISTINCT e.customer_id), 0), 2) AS revenue_per_customer
FROM campaign_events e
JOIN campaigns c ON e.campaign_id = c.campaign_id
WHERE c.status = 'active'
GROUP BY e.channel;
