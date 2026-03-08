{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

-- Hardcode the official TLC data dictionary
WITH tlc_payment_mapping AS (
    SELECT 0 AS payment_type_key, 'Flex Fare trip' AS payment_type_name UNION ALL
    SELECT 1, 'Credit card' UNION ALL
    SELECT 2, 'Cash' UNION ALL
    SELECT 3, 'No charge' UNION ALL
    SELECT 4, 'Dispute' UNION ALL
    SELECT 5, 'Unknown' UNION ALL
    SELECT 6, 'Voided trip'
),

distinct_payments AS (
    SELECT DISTINCT payment_type_id::INT AS payment_type_key
    FROM {{ ref('int_taxi_trips_zones') }}
    WHERE payment_type_id IS NOT NULL
)

SELECT 
    dp.payment_type_key,
    COALESCE(map.payment_type_name, 'Unmapped code') AS payment_type_name
FROM distinct_payments dp
LEFT JOIN tlc_payment_mapping map 
    ON dp.payment_type_key = map.payment_type_key

{% if is_incremental() %}
    WHERE dp.payment_type_key NOT IN (SELECT payment_type_key FROM {{ this }})
{% endif %}