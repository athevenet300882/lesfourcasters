-- models/mart/dim_commune.sql

{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['mart', 'dimension']
) }}

WITH communes AS (
    SELECT
        code_insee,
        ville,
        numero_departement,
        latitude,
        longitude,
        inserted_at
    FROM {{ ref('stg_communes') }}
)

SELECT
    code_insee,
    ville,
    numero_departement,
    latitude,
    longitude,
    inserted_at,
    CURRENT_TIMESTAMP() as dbt_loaded_at
FROM communes