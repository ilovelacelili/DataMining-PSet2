with trips  as (select * from {{ ref('stg_taxi_trips') }}),
zones as (select * from {{ ref('stg_taxi_zones') }})
select 
    trips.vendor_id,

    -- COALESCE automatically grabs whichever one isn't NULL (for tpep vs lpep)
    COALESCE(trips.t_pickup_ts, l_pickup_ts) AS pickup_ts,
    COALESCE(trips.t_dropoff_ts, l_dropoff_ts) AS dropoff_ts,

    COALESCE(trips.passenger_count, 0) as passenger_count, -- replace NULL values as 0
    trips.trip_distance,
    trips.rate_code_id,
    trips.pickup_location_id,
    trips.dropoff_location_id,
    trips.payment_type_id,
    trips.fare_amount,
    trips.tolls_amount,
    trips.tip_amount,
    trips.total_amount,

    -- Metadata from the ingestion
    trips.ingest_ts,
    trips.source_month,
    trips.service_type,

    -- Enrichment by zones
    -- Pickup zone
    zones_pickup.zone_name as pickup_location_name,
    zones_pickup.borough as pickup_borough,
    zones_pickup.service_zone as pickup_service_zone,

    -- Dropoff zone
    zones_dropoff.zone_name as dropoff_location_name,
    zones_dropoff.borough as dropoff_borough,
    zones_dropoff.service_zone as dropoff_service_zone

from trips 
    left join zones zones_pickup on trips.pickup_location_id = zones_pickup.location_id
    left join zones zones_dropoff on trips.dropoff_location_id = zones_dropoff.location_id
where
    trips.pickup_location_id is not null and trips.dropoff_location_id is not null
    and COALESCE(t_pickup_ts, l_pickup_ts) <= COALESCE(t_dropoff_ts, l_dropoff_ts)
    and trip_distance >= 0 and total_amount >= 0