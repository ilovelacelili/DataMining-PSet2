{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

WITH source_data AS (
    -- Read from the cleaned Silver view
    SELECT * FROM {{ ref('int_taxi_trips_zones') }}
),

deduplicated_data AS (
    -- Assign a row number of 1 to the first instance of a trip.
    -- If an exact duplicate exists, it gets row number 2, 3, etc.
    SELECT 
        *,
        ROW_NUMBER() OVER(
            PARTITION BY 
                vendor_id, 
                pickup_ts, 
                dropoff_ts, 
                pickup_location_id, 
                dropoff_location_id 
            ORDER BY ingest_ts DESC -- Keeps the most recently ingested version
        ) as row_num
    FROM source_data
)

SELECT 
    -- Generate a unique hash based on the exact same columns
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 
        'pickup_ts', 
        'dropoff_ts', 
        'pickup_location_id', 
        'dropoff_location_id'
    ]) }} AS trip_key,

    pickup_location_id AS pu_zone_key,
    dropoff_location_id AS do_zone_key,

    -- Partition Key
    DATE(pickup_ts) AS pickup_date_key,

    pickup_ts,
    dropoff_ts,

    -- Foreign Keys to dimensions
    CASE
        WHEN service_type = 'yellow' THEN 1
        WHEN service_type = 'green' THEN 2
    END as service_type_key,
    payment_type_id AS payment_type_key,

    -- Measures
    fare_amount,
    tip_amount,
    total_amount,
    trip_distance,

    -- Metadata
    ingest_ts,
    source_month

FROM deduplicated_data
WHERE row_num = 1