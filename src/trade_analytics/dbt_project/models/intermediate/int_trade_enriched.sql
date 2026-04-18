-- =============================================================================
-- models/intermediate/int_trade_enriched.sql
--
-- Intermediate layer: adds derived columns on top of stg_trades.
-- Computes direction multiplier, signed exposure, day-of-week,
-- market hours flag, and exposure tier used by all three mart models.
--
-- Materialised as: view (recomputed on each mart query — no storage cost)
-- Source: stg_trades (staging view)
-- =============================================================================

WITH base AS (

    SELECT * FROM {{ ref('stg_trades') }}

),

enriched AS (

    SELECT
        trade_id,
        trader_id,
        instrument,
        direction,
        desk,
        region,
        counterparty,
        status,
        notional,
        notional_eur,
        signed_notional_eur,
        price,
        trade_timestamp,
        trade_date,
        trade_hour,
        trade_minute,
        time_of_day,
        is_large_trade,
        risk_tier,
        velocity_flag,
        trades_in_hour,
        trader_daily_notional_run,
        alert_level,
        is_anomaly,
        _ingested_at,
        _source,

        -- BUY  = +1 (long exposure)
        -- SELL = -1 (short exposure)
        CASE
            WHEN direction = 'BUY'  THEN  1
            WHEN direction = 'SELL' THEN -1
            ELSE 0
        END AS direction_multiplier,

        -- 1 = Sunday, 2 = Monday, ..., 7 = Saturday (Spark DAYOFWEEK convention)
        DAYOFWEEK(trade_date)           AS day_of_week,

        DATE_FORMAT(trade_date, 'EEEE') AS day_name,

        WEEKOFYEAR(trade_date)          AS week_number,

        CASE
            WHEN DAYOFWEEK(trade_date) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        CASE
            WHEN ABS(signed_notional_eur) > 5000000 THEN 'HIGH'
            WHEN ABS(signed_notional_eur) > 1000000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS exposure_tier,
        -- European market hours CET 08:00-18:00 (UTC 07:00-17:00)
        CASE
            WHEN trade_hour BETWEEN 7 AND 17 THEN TRUE
            ELSE FALSE
        END AS is_market_hours

    FROM base

)

SELECT * FROM enriched