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
        date,
        code_insee,
        ville,
        numero_departement,
        latitude,
        longitude,
        code_meteo,
        temperature_moyenne,
        temperature_min,
        temperature_max,
        temperature_ressentie_max,
        temperature_ressentie_moyenne,
        humidite_moyenne,
        precipitations_totales,
        vitesse_vent_moyenne,
        rafales_max,
        pression_moyenne,
        ensoleillement_duree
    FROM {{ ref('stg_open_meteo') }}
)

SELECT
    date,
    code_insee,
    ville,
    numero_departement,
    latitude,
    longitude,
    code_meteo,
    ROUND(temperature_moyenne, 2) AS temperature_moyenne,
    ROUND(temperature_min, 2) AS temperature_min,
    ROUND(temperature_max, 2) AS temperature_max,
    ROUND(temperature_ressentie_max, 2) AS temperature_ressentie_max,
    ROUND(temperature_ressentie_moyenne, 2) AS temperature_ressentie_moyenne,
    ROUND(humidite_moyenne, 2) AS humidite_moyenne,
    ROUND(precipitations_totales, 2) AS precipitations_totales,
    ROUND(vitesse_vent_moyenne, 2) AS vitesse_vent_moyenne,
    ROUND(rafales_max, 2) AS rafales_max,
    ROUND(pression_moyenne, 2) AS pression_moyenne,
    ROUND(ensoleillement_duree, 2) AS ensoleillement_duree,
    CURRENT_TIMESTAMP() AS inserted_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM weather_data