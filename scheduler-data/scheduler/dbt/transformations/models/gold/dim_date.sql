{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- Generate a continuous sequence of dates
WITH date_series AS (
    SELECT generate_series(
        '2022-01-01'::DATE, 
        '2025-12-31'::DATE, 
        '1 day'::INTERVAL
    )::DATE AS date_key
)

SELECT 
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(DAY FROM date_key) AS day,
    EXTRACT(ISODOW FROM date_key) AS day_of_week,
    -- ISODOW returns 1-7 (Monday-Sunday). 6 and 7 represent the weekend.
    CASE WHEN EXTRACT(ISODOW FROM date_key) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series

{% if is_incremental() %}
    -- Only insert dates we haven't processed yet
    WHERE date_key NOT IN (SELECT date_key FROM {{ this }})
{% endif %}