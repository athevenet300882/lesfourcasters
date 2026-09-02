-- models/staging/stg_open_meteo.sql
{{ config(
    materialized='view',
    schema='lesfourcasters_dbt',
    tags=['staging']
) }}

SELECT
    CAST(time AS DATE) as date,
    nom_poi as code_insee,
    weather_code as code_meteo,
    temperature_2m_mean as temperature_moyenne,
    relative_humidity_2m_mean as humidite_moyenne,
    precipitation_sum as precipitations_totales,
    wind_speed_10m_mean as vitesse_vent_moyenne,
    pressure_msl_mean as pression_moyenne,
    sunshine_duration as ensoleillement_duree,
    numero_departement,
    latitude_poi as latitude,
    longitude_poi as longitude,
    ville,
    department as departement,
    CURRENT_TIMESTAMP() as inserted_at
FROM {{ source('raw', 'raw_open_meteo') }}
WHERE time IS NOT NULL
    AND nom_poi IS NOT NULL