-- Docs: https://docs.mage.ai/guides/sql-blocks

CREATE SCHEMA IF NOT EXISTS analytics_gold;

-- fct_trips - PARTITION BY RANGE (pickup_date)
DROP TABLE IF EXISTS analytics_gold.fct_trips CASCADE;

CREATE TABLE analytics_gold.fct_trips (
    trip_key TEXT,
    pu_zone_key INT,
    do_zone_key INT,
    pickup_date_key DATE,
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    service_type VARCHAR(10),
    payment_type INT,
    fare_amount NUMERIC(10,2),
    tip_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    trip_distance NUMERIC(10,2)
) PARTITION BY RANGE (pickup_date_key);

-- 2022 Partitions
CREATE TABLE analytics_gold.fct_trips_2022_01 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE analytics_gold.fct_trips_2022_02 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE analytics_gold.fct_trips_2022_03 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');
CREATE TABLE analytics_gold.fct_trips_2022_04 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');
CREATE TABLE analytics_gold.fct_trips_2022_05 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-05-01') TO ('2022-06-01');
CREATE TABLE analytics_gold.fct_trips_2022_06 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-06-01') TO ('2022-07-01');
CREATE TABLE analytics_gold.fct_trips_2022_07 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-07-01') TO ('2022-08-01');
CREATE TABLE analytics_gold.fct_trips_2022_08 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-08-01') TO ('2022-09-01');
CREATE TABLE analytics_gold.fct_trips_2022_09 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-09-01') TO ('2022-10-01');
CREATE TABLE analytics_gold.fct_trips_2022_10 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-10-01') TO ('2022-11-01');
CREATE TABLE analytics_gold.fct_trips_2022_11 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-11-01') TO ('2022-12-01');
CREATE TABLE analytics_gold.fct_trips_2022_12 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2022-12-01') TO ('2023-01-01');

-- 2023 Partitions
CREATE TABLE analytics_gold.fct_trips_2023_01 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE analytics_gold.fct_trips_2023_02 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
CREATE TABLE analytics_gold.fct_trips_2023_03 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
CREATE TABLE analytics_gold.fct_trips_2023_04 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');
CREATE TABLE analytics_gold.fct_trips_2023_05 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-05-01') TO ('2023-06-01');
CREATE TABLE analytics_gold.fct_trips_2023_06 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-06-01') TO ('2023-07-01');
CREATE TABLE analytics_gold.fct_trips_2023_07 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');
CREATE TABLE analytics_gold.fct_trips_2023_08 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');
CREATE TABLE analytics_gold.fct_trips_2023_09 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
CREATE TABLE analytics_gold.fct_trips_2023_10 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');
CREATE TABLE analytics_gold.fct_trips_2023_11 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');
CREATE TABLE analytics_gold.fct_trips_2023_12 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

-- 2024 Partitions
CREATE TABLE analytics_gold.fct_trips_2024_01 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE analytics_gold.fct_trips_2024_02 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE analytics_gold.fct_trips_2024_03 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE analytics_gold.fct_trips_2024_04 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE analytics_gold.fct_trips_2024_05 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE analytics_gold.fct_trips_2024_06 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE analytics_gold.fct_trips_2024_07 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE analytics_gold.fct_trips_2024_08 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE analytics_gold.fct_trips_2024_09 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE analytics_gold.fct_trips_2024_10 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE analytics_gold.fct_trips_2024_11 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE analytics_gold.fct_trips_2024_12 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- 2025 Partitions
CREATE TABLE analytics_gold.fct_trips_2025_01 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE analytics_gold.fct_trips_2025_02 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE analytics_gold.fct_trips_2025_03 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE analytics_gold.fct_trips_2025_04 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE analytics_gold.fct_trips_2025_05 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE analytics_gold.fct_trips_2025_06 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE analytics_gold.fct_trips_2025_07 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE analytics_gold.fct_trips_2025_08 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE analytics_gold.fct_trips_2025_09 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE analytics_gold.fct_trips_2025_10 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE analytics_gold.fct_trips_2025_11 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE analytics_gold.fct_trips_2025_12 PARTITION OF analytics_gold.fct_trips FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');


-- dim_payment_type - PARITION BY LIST (payment_type)
DROP TABLE IF EXISTS analytics_gold.dim_zone CASCADE;

CREATE TABLE analytics_gold.dim_zone (
    zone_key INT,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
) PARTITION BY HASH (zone_key);

-- Create exactly 4 partitions 
CREATE TABLE analytics_gold.dim_zone_p0 PARTITION OF analytics_gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE analytics_gold.dim_zone_p1 PARTITION OF analytics_gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE analytics_gold.dim_zone_p2 PARTITION OF analytics_gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE analytics_gold.dim_zone_p3 PARTITION OF analytics_gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- dim_service_type - PARITION BY LIST (service_type)
DROP TABLE IF EXISTS analytics_gold.dim_service_type CASCADE;

CREATE TABLE analytics_gold.dim_service_type (
    service_type VARCHAR(10),
    description TEXT
) PARTITION BY LIST (service_type);

CREATE TABLE analytics_gold.dim_service_yellow PARTITION OF analytics_gold.dim_service_type FOR VALUES IN ('yellow');
CREATE TABLE analytics_gold.dim_service_green PARTITION OF analytics_gold.dim_service_type FOR VALUES IN ('green');

-- dim_zone - PARTITION BY HASH (zone_key)
DROP TABLE IF EXISTS analytics_gold.dim_payment_type CASCADE;

CREATE TABLE analytics_gold.dim_payment_type (
    payment_type_key INT,
    payment_type_name VARCHAR(15)
) PARTITION BY LIST (payment_type_key);

CREATE TABLE analytics_gold.dim_payment_flex PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (0);
CREATE TABLE analytics_gold.dim_payment_card PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (1);
CREATE TABLE analytics_gold.dim_payment_cash PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (2);
CREATE TABLE analytics_gold.dim_payment_nocharge PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (3);
CREATE TABLE analytics_gold.dim_payment_dispute PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (4);
CREATE TABLE analytics_gold.dim_payment_unknown PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (5);
CREATE TABLE analytics_gold.dim_payment_voided PARTITION OF analytics_gold.dim_payment_type FOR VALUES IN (6);