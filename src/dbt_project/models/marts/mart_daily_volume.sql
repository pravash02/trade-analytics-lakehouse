-- =============================================================================
-- models/marts/mart_daily_volume.sql
--
-- Gold mart: daily trade volume aggregated by date, desk, and instrument.
-- Powers the "Daily Trade Volume" line chart in the Streamlit dashboard.
--
-- Materialised as: Delta table (persisted, queryable from Unity Catalog)
-- Source: int_trade_enriched (intermediate view)
--
-- Key metrics:
--   trade_count          — number of executed trades
--   total_notional_eur   — total volume in EUR
--   avg_notional_eur     — average trade size
--   buy_volume_eur       — total long volume
--   sell_volume_eur      — total short volume
--   net_volume_eur       — net directional volume (buy minus sell)
--   large_trade_count    — trades above EUR 1M threshold
--   anomaly_count        — trades flagged as anomalous
--   anomaly_rate_pct     — anomaly percentage for the day/desk/instrument
-- =============================================================================

WITH base AS (

    SELECT * FROM {{ ref('int_trade_enriched') }}

),

daily_aggregates AS (

    SELECT
        trade_date,
        desk,
        instrument,
        region,
        day_name,
        week_number,
        is_weekend,

        COUNT(trade_id)     AS trade_count,
        SUM(notional_eur)   AS total_notional_eur,
        AVG(notional_eur)   AS avg_notional_eur,
        MIN(notional_eur)   AS min_notional_eur,
        MAX(notional_eur)   AS max_notional_eur,

        SUM(
            CASE WHEN direction = 'BUY'
                 THEN notional_eur ELSE 0 END
        )                   AS buy_volume_eur,

        SUM(
            CASE WHEN direction = 'SELL'
                 THEN notional_eur ELSE 0 END
        )                   AS sell_volume_eur,

        SUM(signed_notional_eur) AS net_volume_eur,

        COUNT(CASE WHEN direction = 'BUY'  THEN 1 END) AS buy_count,
        COUNT(CASE WHEN direction = 'SELL' THEN 1 END) AS sell_count,

        COUNT(CASE WHEN is_large_trade = TRUE     THEN 1 END) AS large_trade_count,
        COUNT(CASE WHEN is_anomaly     = TRUE     THEN 1 END) AS anomaly_count,
        COUNT(CASE WHEN velocity_flag  = TRUE     THEN 1 END) AS velocity_flagged_count,
        COUNT(CASE WHEN alert_level = 'CRITICAL'  THEN 1 END) AS critical_alert_count,
        COUNT(CASE WHEN alert_level = 'WARNING'   THEN 1 END) AS warning_alert_count,

        COUNT(CASE WHEN is_market_hours = TRUE  THEN 1 END)   AS market_hours_count,
        COUNT(CASE WHEN is_market_hours = FALSE THEN 1 END)   AS after_hours_count

    FROM base
    GROUP BY
        trade_date,
        desk,
        instrument,
        region,
        day_name,
        week_number,
        is_weekend

)

SELECT
    *,
    ROUND(
        anomaly_count * 100.0 / NULLIF(trade_count, 0),
        2
    ) AS anomaly_rate_pct,

    ROUND(
        buy_count * 100.0 / NULLIF(trade_count, 0),
        2
    ) AS buy_pct,

    ROUND(
        sell_count * 100.0 / NULLIF(trade_count, 0),
        2
    ) AS sell_pct

FROM daily_aggregates
ORDER BY trade_date DESC, total_notional_eur DESC