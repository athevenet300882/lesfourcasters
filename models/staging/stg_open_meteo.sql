-- models/staging/stg_open_meteo.sql
-- Staging ERA5 avec réconciliation des communes orphelines

{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['staging'],
    description='ERA5 weather data with commune reconciliation (3.5M rows)'
) }}

WITH raw_data AS (
  SELECT *
  FROM {{ source('raw', 'raw_open_meteo') }}
  WHERE time IS NOT NULL
),

communes_ref AS (
  SELECT DISTINCT
    UPPER(Commune) as commune_upper,
    `code INSEE` as code_insee_ref,
    Numero_Departement as numero_departement_ref
  FROM {{ source('raw', 'raw_communes_referentiel') }}
),

-- Réconcilier les lignes orphelines (nom_poi IS NULL) via ville
enriched AS (
  SELECT
    CAST(r.time AS DATE) as date,
    COALESCE(r.nom_poi, c.code_insee_ref) as code_insee,
    r.weather_code as code_meteo,
    r.temperature_2m_mean as temperature_moyenne,
    r.relative_humidity_2m_mean as humidite_moyenne,
    r.precipitation_sum as precipitations_totales,
    r.wind_speed_10m_mean as vitesse_vent_moyenne,
    r.pressure_msl_mean as pression_moyenne,
    r.sunshine_duration as ensoleillement_duree,
    COALESCE(r.numero_departement, c.numero_departement_ref) as numero_departement,
    r.latitude_poi as latitude,
    r.longitude_poi as longitude,
    r.ville,
    CURRENT_TIMESTAMP() as inserted_at,
    -- Tracer la source des données
    CASE 
      WHEN r.nom_poi IS NOT NULL THEN 'original'
      WHEN c.code_insee_ref IS NOT NULL THEN 'reconciliated_from_ville'
      ELSE 'unmatched'
    END as data_source
  FROM raw_data r
  LEFT JOIN communes_ref c
    ON UPPER(r.ville) = c.commune_upper
  WHERE r.nom_poi IS NOT NULL 
     OR c.code_insee_ref IS NOT NULL
     -- Garde les lignes avec nom_poi OU celles reconciliées via ville
)

SELECT * FROM enriched