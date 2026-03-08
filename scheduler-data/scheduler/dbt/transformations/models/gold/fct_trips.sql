{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH silver_trips AS (
    SELECT * FROM {{ ref('int_taxi_trips_zones') }}
)

SELECT 
    -- 1. Generate a unique trip key
    md5(pickup_ts::text || pu_zone_key::text || do_zone_key::text || service_type) AS trip_key,
    
    -- 2. Zone Keys
    pu_zone_key,
    do_zone_key,
    
    -- 3. The Partition Key (RANGE)
    DATE(pickup_ts) AS pickup_date_key,
    
    -- 4. Categorical data
    service_type,
    payment_type,
    
    -- 5. Metrics
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance

FROM silver_trips

-- Optional but recommended: Only process new data on future pipeline runs
{% if is_incremental() %}
    -- Assuming your silver view exposes the ingest_ts
    WHERE ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }})
{% endif %}