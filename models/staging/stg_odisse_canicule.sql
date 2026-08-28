{{ config(
    materialized = 'table',
    description = 'Données sanitaires Odissé nettoyées',
    tags = ['staging', 'health']
) }}

SELECT
  annee,
  libgeo AS commune_name,
  dep AS dept_code,
  reg,
  nom_region,
  nb_j_can,
  CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'raw_odisse_canicule') }}
WHERE annee IS NOT NULL 
  AND libgeo IS NOT NULL
  AND nb_j_can IS NOT NULL