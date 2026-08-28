{{ config(
    materialized = 'table',
    tags = ['mart', 'reporting']
) }}

SELECT
  *,
  CURRENT_TIMESTAMP() AS updated_at
FROM {{ ref('int_health_weather_join') }}