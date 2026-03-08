{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with zones as (
    select * from {{ ref('stg_taxi_zones') }}
)

select distinct
    location_id as zone_key, -- HASH partition key
    borough,
    zone_name,
    service_zone
from zones