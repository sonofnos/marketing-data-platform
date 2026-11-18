-- Triggers for the marketing data platform

-- ── updated_at auto-maintenance ───────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON customers;
CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();


-- ── Audit log on customers table ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_customers_audit()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO customers_audit (customer_id, operation, old_data)
        VALUES (OLD.customer_id, 'U', row_to_json(OLD)::jsonb);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO customers_audit (customer_id, operation, old_data)
        VALUES (OLD.customer_id, 'D', row_to_json(OLD)::jsonb);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_customers_audit ON customers;
CREATE TRIGGER trg_customers_audit
    AFTER UPDATE OR DELETE ON customers
    FOR EACH ROW EXECUTE FUNCTION fn_customers_audit();


-- ── Prevent archiving active campaigns ───────────────────────────────────────

CREATE OR REPLACE FUNCTION fn_guard_active_campaign_delete()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'active' THEN
        RAISE EXCEPTION 'Cannot delete an active campaign (%). Set status to "completed" or "paused" first.', OLD.campaign_id;
    END IF;
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_active_campaign_delete ON campaigns;
CREATE TRIGGER trg_guard_active_campaign_delete
    BEFORE DELETE ON campaigns
    FOR EACH ROW EXECUTE FUNCTION fn_guard_active_campaign_delete();


-- ── Invalidate cached segment on significant spend change ─────────────────────
-- If a customer's total_spend changes by >20%, mark their segment for refresh

CREATE OR REPLACE FUNCTION fn_flag_segment_refresh()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.total_spend > 0
       AND ABS(NEW.total_spend - OLD.total_spend) / OLD.total_spend > 0.20
    THEN
        UPDATE customer_segments
        SET segmented_at = '1970-01-01'::TIMESTAMPTZ  -- stale sentinel
        WHERE customer_id = NEW.customer_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_flag_segment_refresh ON customers;
CREATE TRIGGER trg_flag_segment_refresh
    AFTER UPDATE OF total_spend ON customers
    FOR EACH ROW EXECUTE FUNCTION fn_flag_segment_refresh();
