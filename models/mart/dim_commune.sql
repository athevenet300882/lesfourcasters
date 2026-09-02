-- models/mart/dim_commune.sql
{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['mart', 'dimension']
) }}

WITH communes AS (
    SELECT DISTINCT
        nom_poi as code_insee,
        ville,
        departement as departement,
        numero_departement,
        latitude_poi as latitude,
        longitude_poi as longitude,
        CURRENT_TIMESTAMP() as inserted_at
    FROM {{ source('raw', 'raw_open_meteo') }}
    WHERE nom_poi IS NOT NULL
)

SELECT
    code_insee,
    ville,
    departement,
    numero_departement,
    latitude,
    longitude,
    inserted_at
FROM communes