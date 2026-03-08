with base as (
    select * from {{ source('raw', 'raw_taxi_trips') }}
)
select 
    base.vendorid::int as vendor_id,
    base.tpep_pickup_datetime::timestamp as t_pickup_ts,
    base.tpep_dropoff_datetime::timestamp as t_dropoff_ts,
    base.lpep_pickup_datetime::timestamp as l_pickup_ts,
    base.lpep_dropoff_datetime::timestamp as l_dropoff_ts,
    base.passenger_count:: int as passenger_count,
    base.trip_distance::numeric(10,2) as trip_distance,
    base.RatecodeID::int as rate_code_id,
    base.PULocationID::int as pickup_location_id,
    base.DOLocationID::int as dropoff_location_id,
    base.payment_type::int as payment_type_id,
    base.fare_amount::numeric(10,2) as fare_amount,
    base.tolls_amount::numeric(10,2) as tolls_amount,
    base.tip_amount::numeric(10,2) as tip_amount,
    base.total_amount::numeric(10,2) as total_amount,
    
    -- Mandatory Ingestion Metadata 
    base.ingest_ts::timestamp as ingest_ts,
    base.source_month,
    base.service_type

from base