-- Stored procedures for the marketing data platform

-- Refresh customer segments from latest event data
-- Called after each pipeline run to keep segments current
CREATE OR REPLACE PROCEDURE sp_refresh_segments()
LANGUAGE plpgsql
AS $$
DECLARE
    v_today        DATE := CURRENT_DATE;
    v_rows_updated INT;
BEGIN
    -- Recompute RFM scores using a CTE, upsert into customer_segments
    WITH rfm_base AS (
        SELECT
            e.customer_id,
            (v_today - MAX(e.event_date))::INT                          AS recency_days,
            COUNT(*) FILTER (WHERE e.event_type = 'conversion')        AS frequency,
            COALESCE(SUM(e.revenue) FILTER (WHERE e.event_type = 'conversion'), 0) AS monetary
        FROM campaign_events e
        GROUP BY e.customer_id
    ),
    rfm_scored AS (
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency_days)              AS r_score,
            NTILE(5) OVER (ORDER BY frequency DESC)            AS f_score,
            NTILE(5) OVER (ORDER BY monetary DESC)             AS m_score
        FROM rfm_base
    ),
    rfm_labeled AS (
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            (r_score + f_score + m_score)                      AS rfm_score,
            CASE
                WHEN (r_score + f_score + m_score) >= 13              THEN 'Champions'
                WHEN (r_score + f_score + m_score) >= 10 AND f_score >= 4  THEN 'Loyal'
                WHEN (r_score + f_score + m_score) >= 9 AND r_score >= 4   THEN 'Potential Loyalist'
                WHEN r_score <= 2 AND (r_score + f_score + m_score) >= 8   THEN 'At Risk'
                WHEN r_score <= 2                                      THEN 'Hibernating'
                WHEN (r_score + f_score + m_score) >= 7               THEN 'Promising'
                ELSE                                                   'Needs Attention'
            END AS segment
        FROM rfm_scored
    )
    INSERT INTO customer_segments (customer_id, segment, r_score, f_score, m_score, rfm_score, recency_days, frequency, monetary, segmented_at)
    SELECT customer_id, segment, r_score, f_score, m_score, rfm_score, recency_days, frequency, monetary, NOW()
    FROM rfm_labeled
    ON CONFLICT (customer_id) DO UPDATE SET
        segment      = EXCLUDED.segment,
        r_score      = EXCLUDED.r_score,
        f_score      = EXCLUDED.f_score,
        m_score      = EXCLUDED.m_score,
        rfm_score    = EXCLUDED.rfm_score,
        recency_days = EXCLUDED.recency_days,
        frequency    = EXCLUDED.frequency,
        monetary     = EXCLUDED.monetary,
        segmented_at = EXCLUDED.segmented_at;

    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
    RAISE NOTICE 'sp_refresh_segments: % customer segments updated', v_rows_updated;
END;
$$;


-- Archive processed events older than N days into a cold-storage table
CREATE TABLE IF NOT EXISTS campaign_events_archive (LIKE campaign_events INCLUDING ALL);

CREATE OR REPLACE PROCEDURE sp_archive_old_events(p_days_old INT DEFAULT 365)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff DATE := CURRENT_DATE - p_days_old;
    v_archived INT;
BEGIN
    IF p_days_old < 30 THEN
        RAISE EXCEPTION 'archive threshold must be >= 30 days (got %)', p_days_old;
    END IF;

    -- Move to archive in one transaction
    WITH moved AS (
        DELETE FROM campaign_events
        WHERE event_date < v_cutoff
        RETURNING *
    )
    INSERT INTO campaign_events_archive SELECT * FROM moved;

    GET DIAGNOSTICS v_archived = ROW_COUNT;
    RAISE NOTICE 'sp_archive_old_events: % events archived (older than %)', v_archived, v_cutoff;
END;
$$;


-- Recompute campaign_metrics for one or all campaigns
CREATE OR REPLACE PROCEDURE sp_recompute_campaign_metrics(p_campaign_id VARCHAR DEFAULT NULL)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO campaign_metrics (
        campaign_id, impressions, clicks, opens, conversions, unsubscribes,
        total_revenue, unique_customers, ctr, cvr, cost_per_conversion, computed_at
    )
    SELECT
        e.campaign_id,
        COUNT(*) FILTER (WHERE e.event_type = 'impression'),
        COUNT(*) FILTER (WHERE e.event_type = 'click'),
        COUNT(*) FILTER (WHERE e.event_type = 'open'),
        COUNT(*) FILTER (WHERE e.event_type = 'conversion'),
        COUNT(*) FILTER (WHERE e.event_type = 'unsubscribe'),
        SUM(e.revenue),
        COUNT(DISTINCT e.customer_id),
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE e.event_type = 'click')
                / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'impression'), 0), 2
        ),
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE e.event_type = 'conversion')
                / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'click'), 0), 2
        ),
        ROUND(c.budget / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'conversion'), 0), 2),
        NOW()
    FROM campaign_events e
    JOIN campaigns c ON e.campaign_id = c.campaign_id
    WHERE p_campaign_id IS NULL OR e.campaign_id = p_campaign_id
    GROUP BY e.campaign_id, c.budget
    ON CONFLICT (campaign_id) DO UPDATE SET
        impressions         = EXCLUDED.impressions,
        clicks              = EXCLUDED.clicks,
        opens               = EXCLUDED.opens,
        conversions         = EXCLUDED.conversions,
        unsubscribes        = EXCLUDED.unsubscribes,
        total_revenue       = EXCLUDED.total_revenue,
        unique_customers    = EXCLUDED.unique_customers,
        ctr                 = EXCLUDED.ctr,
        cvr                 = EXCLUDED.cvr,
        cost_per_conversion = EXCLUDED.cost_per_conversion,
        computed_at         = EXCLUDED.computed_at;

    RAISE NOTICE 'Campaign metrics recomputed (campaign_id filter: %)', COALESCE(p_campaign_id, 'ALL');
END;
$$;
