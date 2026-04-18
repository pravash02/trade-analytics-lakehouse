-- =============================================================================
-- macros/cents_to_euros.sql
--
-- Utility macro: converts a cents value to euros.
-- Included as an example of dbt macros for the portfolio.
--
-- Usage in a model:
--   SELECT {{ cents_to_euros('notional_cents') }} AS notional_eur
-- =============================================================================

{% macro cents_to_euros(column_name) %}
    ROUND({{ column_name }} / 100.0, 2)
{% endmacro %}