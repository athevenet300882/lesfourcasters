{{ config(
    materialized = 'table',
    tags = ['staging', 'health']
) }}

SELECT
  annee,
  libgeo AS departement_nom,
  dep AS departement_code,
  reg,
  nom_region,
  nb_j_can AS nb_jours_canicule,
  CURRENT_TIMESTAMP() AS loaded_at
FROM {{ source('raw', 'raw_odisse_canicule') }}
WHERE annee IS NOT NULL
  AND libgeo IS NOT NULL
  AND nb_j_can IS NOT NULL