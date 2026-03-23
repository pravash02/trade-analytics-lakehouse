-- =============================================================================
-- models/marts/mart_top_instruments.sql
--
-- Gold mart: trade volume and anomaly rates ranked by instrument.
-- Powers the "Volume by Instrument" donut chart and
-- "Anomaly Rate by Instrument" bar chart in the Streamlit dashboard.
--
-- Materialised as: Delta table
-- Source: int_trade_enriched (intermediate view)
--
-- Key metrics:
--   trade_count         — number of trades per instrument
--   total_volume_eur    — total EUR volume
--   avg_trade_size_eur  — average trade size
--   unique_traders      — distinct traders on this instrument
--   anomaly_rate_pct    — percentage of trades flagged as anomalous
--   volume_rank         — rank by total volume (1 = highest volume)
--   volume_share_pct    — percentage of total market volume
-- =============================================================================

WITH base AS (

    SELECT * FROM {{ ref('int_trade_enriched') }}

),

instrument_stats AS (

    SELECT
        instrument,
        desk,

        COUNT(trade_id)        AS trade_count,
        SUM(notional_eur)      AS total_volume_eur,
        AVG(notional_eur)      AS avg_trade_size_eur,
        MIN(notional_eur)      AS min_trade_size_eur,
        MAX(notional_eur)      AS max_trade_size_eur,

        SUM(
            CASE WHEN direction = 'BUY'
                 THEN notional_eur ELSE 0 END
        )                      AS buy_volume_eur,

        SUM(
            CASE WHEN direction = 'SELL'
                 THEN notional_eur ELSE 0 END
        )                      AS sell_volume_eur,

        SUM(signed_notional_eur) AS net_volume_eur,

        COUNT(DISTINCT trader_id)  AS unique_traders,
        COUNT(DISTINCT trade_date) AS active_days,
        COUNT(DISTINCT region)     AS active_regions,

        COUNT(CASE WHEN is_anomaly     = TRUE THEN 1 END) AS anomaly_count,
        COUNT(CASE WHEN velocity_flag  = TRUE THEN 1 END) AS velocity_flagged_count,
        COUNT(CASE WHEN is_large_trade = TRUE THEN 1 END) AS large_trade_count,
        COUNT(CASE WHEN alert_level = 'CRITICAL' THEN 1 END) AS critical_alert_count,

        MIN(trade_date)  AS first_trade_date,
        MAX(trade_date)  AS last_trade_date

    FROM base
    GROUP BY
        instrument,
        desk

),

ranked AS (

    SELECT
        *,
        ROUND(
            anomaly_count * 100.0 / NULLIF(trade_count, 0),
            2
        )                    AS anomaly_rate_pct,

        ROUND(
            large_trade_count * 100.0 / NULLIF(trade_count, 0),
            2
        )                    AS large_trade_rate_pct,

        ROUND(
            buy_volume_eur * 100.0 / NULLIF(total_volume_eur, 0),
            2
        )                    AS buy_volume_pct,

        ROUND(
            total_volume_eur * 100.0
            / NULLIF(SUM(total_volume_eur) OVER (), 0),
            2
        )                    AS volume_share_pct,

        RANK() OVER (
            ORDER BY total_volume_eur DESC
        )                    AS volume_rank,

        RANK() OVER (
            ORDER BY anomaly_count DESC
        )                    AS anomaly_rank

    FROM instrument_stats

)

SELECT * FROM ranked
ORDER BY volume_rank