{{ config(
    materialized = 'table',
    tags = ['staging', 'reference']
) }}

SELECT
  TRIM(Commune) AS Commune,
  Departement,
  `Région` AS region,
  LPAD(CAST(Numero_Departement AS STRING), 2, '0') AS numero_departement,
  Service,
  Latitude,
  Longitude,
  LPAD(CAST(`code INSEE` AS STRING), 5, '0') AS code_insee,
  Centroide
FROM {{ source('raw', 'raw_communes_referentiel') }}
WHERE Commune IS NOT NULL