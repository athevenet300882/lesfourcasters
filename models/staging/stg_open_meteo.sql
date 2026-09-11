-- models/staging/stg_open_meteo.sql

{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['staging'],
    description='ERA5 weather data enrichie avec dim_commune'
) }}

WITH raw_data AS (
  SELECT *
  FROM {{ source('raw', 'raw_open_meteo') }}
  WHERE time IS NOT NULL
),

keyed AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(CAST(nom_poi AS STRING), r'^[0-9]+$')
        THEN LPAD(CAST(nom_poi AS STRING), 5, '0')
      WHEN REGEXP_CONTAINS(CAST(nom_poi AS STRING), r'^2[AB][0-9]{3}$')
        THEN CAST(nom_poi AS STRING)
    END AS key_code,

    CASE
      WHEN NOT REGEXP_CONTAINS(CAST(nom_poi AS STRING), r'^[0-9]')
        THEN REGEXP_REPLACE(
               REGEXP_REPLACE(NORMALIZE(UPPER(CAST(nom_poi AS STRING)), NFD), r'\pM', ''),
               r'[^A-Z0-9]', ''
             )
    END AS key_nom_poi,

    REGEXP_REPLACE(
      REGEXP_REPLACE(NORMALIZE(UPPER(CAST(ville AS STRING)), NFD), r'\pM', ''),
      r'[^A-Z0-9]', ''
    ) AS key_ville_raw

  FROM raw_data
),

communes_ref AS (
  SELECT
    code_insee,
    ville,
    numero_departement,
    latitude,
    longitude,
    REGEXP_REPLACE(
      REGEXP_REPLACE(NORMALIZE(UPPER(ville), NFD), r'\pM', ''),
      r'[^A-Z0-9]', ''
    ) AS ville_key
  FROM {{ ref('dim_commune') }}
),

enriched AS (
  SELECT
    CAST(r.time AS DATE) AS date,
    COALESCE(c1.code_insee, c2.code_insee, c3.code_insee) AS code_insee,
    COALESCE(c1.ville, c2.ville, c3.ville) AS ville,
    r.weather_code AS code_meteo,

    r.temperature_2m_mean AS temperature_moyenne,
    r.temperature_2m_min AS temperature_min,
    r.temperature_2m_max AS temperature_max,
    r.apparent_temperature_max AS temperature_ressentie_max,
    r.apparent_temperature_mean AS temperature_ressentie_moyenne,

    r.relative_humidity_2m_mean AS humidite_moyenne,
    r.precipitation_sum AS precipitations_totales,
    r.wind_speed_10m_mean AS vitesse_vent_moyenne,
    r.wind_gusts_10m_max AS rafales_max,
    r.pressure_msl_mean AS pression_moyenne,
    r.sunshine_duration AS ensoleillement_duree,

    COALESCE(c1.numero_departement, c2.numero_departement, c3.numero_departement) AS numero_departement,
    COALESCE(c1.latitude, c2.latitude, c3.latitude) AS latitude,
    COALESCE(c1.longitude, c2.longitude, c3.longitude) AS longitude,

    CURRENT_TIMESTAMP() AS inserted_at
  FROM keyed r
  LEFT JOIN communes_ref c1 ON r.key_code      = c1.code_insee
  LEFT JOIN communes_ref c2 ON r.key_nom_poi   = c2.ville_key
  LEFT JOIN communes_ref c3 ON r.key_ville_raw = c3.ville_key
),

deduped AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY date, code_insee
        ORDER BY
          CASE WHEN temperature_max IS NOT NULL THEN 0 ELSE 1 END,
          CASE WHEN temperature_ressentie_max IS NOT NULL THEN 0 ELSE 1 END
      ) AS row_num
    FROM enriched
    WHERE code_insee IS NOT NULL
  )
  WHERE row_num = 1
)

SELECT * FROM deduped