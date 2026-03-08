with base as(
    select * from {{ source('raw', 'raw_taxi_zones') }}
)
select
    base.locationid::int as location_id,
    base.borough,
    base._zone as zone_name,
    base.service_zone
from base