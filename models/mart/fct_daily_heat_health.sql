{{ config(materialized = 'table', tags = ['mart']) }}

SELECT * FROM {{ ref('int_health_weather_join') }}