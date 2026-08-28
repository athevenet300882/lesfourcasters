{{ config(
    materialized = 'table',
    tags = ['intermediate']
) }}

WITH weather AS (
  SELECT
    date,
    code_meteo,
    temperature_moyenne,
    humidite_moyenne,
    precipitations_totales,
    latitude,
    longitude,
    ville,
    numero_departement
  FROM {{ ref('stg_open_meteo') }}
),

communes AS (
  SELECT DISTINCT
    LPAD(CAST(Numero_Departement AS STRING), 2, '0') AS numero_departement,
    Commune,
    Departement
  FROM {{ ref('stg_communes') }}
),

joined AS (
  SELECT
    w.date,
    w.code_meteo,
    w.temperature_moyenne,
    w.humidite_moyenne,
    w.precipitations_totales,
    w.latitude,
    w.longitude,
    w.ville,
    w.numero_departement,
    CASE
      WHEN c1.Departement IS NOT NULL THEN c1.Departement
      WHEN c2.Departement IS NOT NULL THEN c2.Departement
      ELSE NULL
    END AS departement,
    ROW_NUMBER() OVER (PARTITION BY w.date, w.ville, w.numero_departement ORDER BY c1.Departement DESC) AS rn
  FROM weather w
  LEFT JOIN communes c1
    ON w.numero_departement = c1.numero_departement
  LEFT JOIN communes c2
    ON LOWER(w.ville) = LOWER(c2.Commune)
    AND w.numero_departement IS NULL
)

SELECT
  date,
  code_meteo,
  temperature_moyenne,
  humidite_moyenne,
  precipitations_totales,
  latitude,
  longitude,
  ville,
  numero_departement,
  departement
FROM joined
WHERE rn = 1