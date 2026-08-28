{{ config(
    materialized = 'table',
    tags = ['staging', 'reference']
) }}

SELECT
  Commune,
  Departement,
  LPAD(Numero_Departement, 2, '0') AS numero_departement,
  Service,
  Latitude,
  Longitude,
  LPAD(`code INSEE`, 5, '0') AS code_insee,
  Centroide
FROM {{ source('raw', 'raw_communes_referentiel') }}
WHERE Commune IS NOT NULL