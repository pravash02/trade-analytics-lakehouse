-- =============================================================================
-- models/marts/mart_risk_exposure.sql
--
-- Gold mart: net risk exposure per trader.
-- Powers the "Trader Risk Exposure" bar chart and alert table
-- in the Streamlit dashboard.
--
-- Materialised as: Delta table
-- Source: int_trade_enriched (intermediate view)
--
-- Key metrics:
--   net_exposure_eur    — net signed position (long minus short)
--   gross_long_eur      — total BUY notional
--   gross_short_eur     — total SELL notional
--   exposure_risk_tier  — HIGH / MEDIUM / LOW based on abs(net_exposure)
--   worst_alert_level   — highest severity alert across all trader's trades
--   position_direction  — LONG / SHORT / FLAT
-- =============================================================================

WITH base AS (

    SELECT * FROM {{ ref('int_trade_enriched') }}

),

trader_exposure AS (

    SELECT
        trader_id,
        desk,
        region,

        COUNT(trade_id)                                  AS total_trades,
        COUNT(CASE WHEN direction = 'BUY'  THEN 1 END)  AS buy_trades,
        COUNT(CASE WHEN direction = 'SELL' THEN 1 END)  AS sell_trades,

        SUM(signed_notional_eur)                         AS net_exposure_eur,

        SUM(
            CASE WHEN direction = 'BUY'
                 THEN notional_eur ELSE 0 END
        )                                                AS gross_long_eur,

        SUM(
            CASE WHEN direction = 'SELL'
                 THEN notional_eur ELSE 0 END
        )                                                AS gross_short_eur,

        SUM(notional_eur)                                AS total_notional_eur,
        AVG(notional_eur)                                AS avg_trade_size_eur,
        MAX(notional_eur)                                AS largest_trade_eur,

        MAX(CASE WHEN velocity_flag  = TRUE THEN 1 ELSE 0 END) AS has_velocity_flag,
        MAX(CASE WHEN is_large_trade = TRUE THEN 1 ELSE 0 END) AS has_large_trade,
        MAX(CASE WHEN is_anomaly     = TRUE THEN 1 ELSE 0 END) AS has_anomaly,

        COUNT(CASE WHEN velocity_flag = TRUE THEN 1 END) AS velocity_flagged_trades,
        COUNT(CASE WHEN is_anomaly    = TRUE THEN 1 END) AS anomaly_trades,

        MAX(CASE WHEN alert_level = 'CRITICAL' THEN 1 ELSE 0 END) AS has_critical_alert,
        MAX(CASE WHEN alert_level = 'WARNING'  THEN 1 ELSE 0 END) AS has_warning_alert,

        CASE
            WHEN MAX(CASE WHEN alert_level = 'CRITICAL' THEN 1 ELSE 0 END) = 1
                THEN 'CRITICAL'
            WHEN MAX(CASE WHEN alert_level = 'WARNING'  THEN 1 ELSE 0 END) = 1
                THEN 'WARNING'
            ELSE 'NORMAL'
        END                                              AS worst_alert_level,

        MIN(trade_date)                                  AS first_trade_date,
        MAX(trade_date)                                  AS last_trade_date,
        COUNT(DISTINCT trade_date)                       AS active_trading_days

    FROM base
    GROUP BY
        trader_id,
        desk,
        region

)

SELECT
    *,

    CASE
        WHEN ABS(net_exposure_eur) > 5000000 THEN 'HIGH'
        WHEN ABS(net_exposure_eur) > 1000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS exposure_risk_tier,

    CASE
        WHEN net_exposure_eur > 0 THEN 'LONG'
        WHEN net_exposure_eur < 0 THEN 'SHORT'
        ELSE 'FLAT'
    END AS position_direction,

    ROUND(
        anomaly_trades * 100.0 / NULLIF(total_trades, 0),
        2
    ) AS anomaly_rate_pct

FROM trader_exposure
ORDER BY ABS(net_exposure_eur) DESC