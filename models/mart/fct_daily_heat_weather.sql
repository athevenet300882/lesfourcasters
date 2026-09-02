-- models/mart/fct_daily_heat_weather.sql

{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['mart', 'fact'],
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "month"
    }
) }}

WITH weather_data AS (
    SELECT
        CAST(time AS DATE) as date,
        nom_poi as code_insee,
        temperature_2m_mean as temperature_moyenne,
        relative_humidity_2m_mean as humidite_moyenne,
        precipitation_sum as precipitations_totales,
        wind_speed_10m_mean as vitesse_vent_moyenne,
        pressure_msl_mean as pression_moyenne,
        sunshine_duration as ensoleillement_duree,
        CURRENT_TIMESTAMP() as inserted_at,
        CURRENT_TIMESTAMP() as updated_at
    FROM {{ source('raw', 'raw_open_meteo') }}
    WHERE time IS NOT NULL
        AND nom_poi IS NOT NULL
)

SELECT
    date,
    code_insee,
    ROUND(temperature_moyenne, 2) as temperature_moyenne,
    ROUND(humidite_moyenne, 2) as humidite_moyenne,
    ROUND(precipitations_totales, 2) as precipitations_totales,
    ROUND(vitesse_vent_moyenne, 2) as vitesse_vent_moyenne,
    ROUND(pression_moyenne, 2) as pression_moyenne,
    ROUND(ensoleillement_duree, 2) as ensoleillement_duree,
    inserted_at,
    updated_at
FROM weather_data