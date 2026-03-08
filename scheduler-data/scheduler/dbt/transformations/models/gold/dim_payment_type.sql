{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- Hardcode the official TLC data dictionary
WITH tlc_payment_mapping AS (
    SELECT 0 AS payment_type, 'Flex Fare trip' AS description UNION ALL
    SELECT 1, 'Credit card' UNION ALL
    SELECT 2, 'Cash' UNION ALL
    SELECT 3, 'No charge' UNION ALL
    SELECT 4, 'Dispute' UNION ALL
    SELECT 5, 'Unknown' UNION ALL
    SELECT 6, 'Voided trip'
),

distinct_payments AS (
    SELECT DISTINCT payment_type::INT AS payment_type_key
    FROM {{ ref('int_taxi_trips_zones') }}
    WHERE payment_type IS NOT NULL
)

SELECT 
    dp.payment_type,
    COALESCE(map.description, 'Unmapped code') AS description
FROM distinct_payments dp
LEFT JOIN tlc_payment_mapping map 
    ON dp.payment_type = map.payment_type

{% if is_incremental() %}
    WHERE dp.payment_type NOT IN (SELECT payment_type FROM {{ this }})
{% endif %}