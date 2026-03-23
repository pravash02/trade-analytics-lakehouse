-- =============================================================================
-- tests/assert_no_negative_trade_count.sql
--
-- Custom dbt test: fails if any daily volume row has zero or negative
-- trade_count. This should never happen — every row in mart_daily_volume
-- represents at least one actual trade.
-- =============================================================================

SELECT
    trade_date,
    desk,
    instrument,
    trade_count
FROM {{ ref('mart_daily_volume') }}
WHERE trade_count <= 0