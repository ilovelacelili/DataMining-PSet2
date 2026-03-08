{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}