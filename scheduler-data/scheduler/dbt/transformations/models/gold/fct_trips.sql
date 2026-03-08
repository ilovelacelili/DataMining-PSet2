{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH silver_trips AS (
    SELECT * FROM {{ ref('int_taxi_trips_zones') }}
)

SELECT 
    -- Unique trip key
    md5(pickup_ts::text || pickup_location_id::text || dropoff_location_id::text || service_type) AS trip_key,
    
    -- Partition Key (by RANGE)
    DATE(pickup_ts) AS pickup_date_key,

    -- Zone Keys
    pickup_location_id as pu_zone_key,
    dropoff_location_id as do_zone_key,
    pickup_ts,
    dropoff_ts,
    
    -- Categorical data
    CASE 
        WHEN service_type = 'yellow' THEN 1
        WHEN service_type = 'green' THEN 2 
    END as service_type_key,

    COALESCE(payment_type_id, 5) as payment_type_key, -- 5 means Unknown in the Docs
    
    -- Metrics
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance,

    -- Metadata
    ingest_ts, 
    source_month

FROM silver_trips 
-- Only process new data on future pipeline runs
{% if is_incremental() %}
    WHERE ingest_ts > (SELECT COALESCE(MAX(ingest_ts), '1900-01-01'::TIMESTAMP) FROM {{ this }})
{% endif %}