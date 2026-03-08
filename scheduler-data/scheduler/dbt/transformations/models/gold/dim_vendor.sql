{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH distinct_vendors AS (
    -- Extract every unique vendor ID from your cleaned trips
    SELECT DISTINCT vendor_id
    FROM {{ ref('int_taxi_trips_zones') }}
    WHERE vendor_id IS NOT NULL
)

SELECT 
    vendor_id as vendor_key,
    CASE 
        WHEN vendor_id = 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN vendor_id = 2 THEN 'VeriFone Inc.'
        WHEN vendor_id = 6 THEN 'Myle Technologies Inc'
        WHEN vendor_id = 7 THEN 'Helix'
        ELSE 'Unknown Vendor'
    END AS vendor_name
FROM distinct_vendors

{% if is_incremental() %}
    -- Only insert vendors that don't already exist in the target table
    WHERE vendor_id NOT IN (SELECT vendor_id FROM {{ this }})
{% endif %}