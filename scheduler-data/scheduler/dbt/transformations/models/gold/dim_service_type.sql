{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH service_types AS (
    SELECT * FROM (VALUES
        (1, 'yellow', 'Yellow Taxi'),
        (2, 'green',  'Green Taxi')
    ) AS t(service_type_key, service_name, service_description)
)

SELECT
    service_type_key,
    service_name,
    service_description
FROM service_types
ORDER BY service_type_key