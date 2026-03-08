-- Docs: https://docs.mage.ai/guides/sql-blocks

with base as (
    select * from {{ ref('stg_taxi_trips') }}
)
SELECT 
    LEFT(source_month, 4) AS trip_year,
    RIGHT(source_month, 2) AS trip_month,
    source_month AS year_month,
    service_type,
    COUNT(*) AS total_rows
FROM 
    base
GROUP BY 
    LEFT(source_month, 4),
    RIGHT(source_month, 2),
    source_month,
    service_type
ORDER BY 
    trip_year DESC, 
    trip_month DESC, 
    service_type