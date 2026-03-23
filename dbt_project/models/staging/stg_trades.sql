-- =============================================================================
-- models/staging/stg_trades.sql
--
-- Staging layer: reads directly from Silver Delta table.
-- Renames, casts, and applies the first business rule
-- (exclude CANCELLED trades).
--
-- Materialised as: view (no storage cost, always fresh)
-- Source: Silver Delta → /Volumes/.../silver/trades_enriched
-- =============================================================================

WITH source AS (

    SELECT *
    FROM delta.`/Volumes/workspace/default/trade-analytics/silver/trades_enriched`

),

cleaned AS (

    SELECT
        trade_id,
        trader_id,
        instrument,
        direction,
        desk,
        region,
        counterparty,
        status,
        CAST(notional            AS DECIMAL(24, 4)) AS notional,
        CAST(notional_eur        AS DECIMAL(24, 4)) AS notional_eur,
        CAST(signed_notional_eur AS DECIMAL(24, 4)) AS signed_notional_eur,
        CAST(price               AS DECIMAL(18, 6)) AS price,
        trade_timestamp,
        trade_date,
        trade_hour,
        trade_minute,
        time_of_day,
        is_large_trade,
        risk_tier,
        velocity_flag,
        trades_in_hour,
        CAST(trader_daily_notional_run AS DECIMAL(28, 4)) AS trader_daily_notional_run,
        alert_level,
        is_anomaly,
        _ingested_at,
        _source

    FROM source

    -- Business rule: CANCELLED trades excluded from all Gold analytics. They remain in Silver for audit purposes.
    WHERE status != 'CANCELLED'

)

SELECT * FROM cleaned