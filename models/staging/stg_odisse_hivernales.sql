{{ config(
    materialized = 'table',
    tags = ['staging', 'health']
) }}

SELECT
  date,
  reg,
  nom_region,
  REGEXP_REPLACE(NORMALIZE(UPPER(nom_region), NFD), r'\pM', '') AS region_key,
  sous_chapitre,
  theme,
  indicateur,
  valeur,
  CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'raw_odisse_epidemies_hivernales') }}
WHERE date IS NOT NULL 
  AND valeur IS NOT NULL