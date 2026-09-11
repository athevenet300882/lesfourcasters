{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['mart', 'dimension']
) }}

SELECT DISTINCT
    region AS nom_region,
    REGEXP_REPLACE(NORMALIZE(UPPER(region), NFD), r'\pM', '') AS region_key,
    CURRENT_TIMESTAMP() AS inserted_at
FROM {{ ref('stg_communes') }}
WHERE region IS NOT NULL