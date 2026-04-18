-- =============================================================================
-- tests/assert_positive_notional.sql
--
-- Custom dbt test: fails if any trade has zero or negative notional_eur.
--
-- dbt convention: rows returned = test FAILURE, empty result = PASS.
--
-- Guards against:
--   - Data generator bugs producing zero/negative notional
--   - FX rate misconfig producing zero EUR equivalent
--   - Corrupt Bronze records slipping through Pydantic validation
-- =============================================================================

SELECT
    trade_id,
    trader_id,
    instrument,
    notional_eur,
    _ingested_at
FROM {{ ref('stg_trades') }}
WHERE notional_eur <= 0