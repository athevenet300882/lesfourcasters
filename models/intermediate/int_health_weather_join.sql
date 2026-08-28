{{ config(
    materialized = 'table',
    tags = ['intermediate']
) }}

WITH weather AS (
  SELECT
    date,
    code_meteo,
    temperature_moyenne,
    temperature_minimale,
    temperature_maximale,
    temperature_ressentie_moyenne,
    temperature_ressentie_minimale,
    temperature_ressentie_maximale,
    humidite_moyenne,
    humidite_minimale,
    humidite_maximale,
    point_de_rosee_moyen,
    precipitations_totales,
    pluie_totale,
    neige_totale,
    heures_de_precipitations,
    vitesse_vent_moyenne,
    vitesse_vent_maximale,
    rafale_vent_maximale,
    direction_vent_dominante,
    couverture_nuageuse_moyenne,
    pression_moyenne,
    duree_ensoleillement,
    rayonnement_solaire_total,
    evapotranspiration,
    deficit_pression_vapeur_maximal,
    humidite_sol_0_7cm,
    humidite_sol_7_28cm,
    humidite_sol_28_100cm,
    temperature_sol_0_7cm,
    ville,
    numero_departement,
    latitude,
    longitude,
    inserted_at
  FROM {{ ref('stg_open_meteo') }}
),

communes AS (
  SELECT DISTINCT
    Commune,
    numero_departement,
    Departement,
    region,
    code_insee
  FROM {{ ref('stg_communes') }}
),

joined AS (
  SELECT
    w.*,
    COALESCE(c2.Departement, c1.Departement) AS departement,
    COALESCE(c2.region, c1.region) AS region,
    c2.code_insee AS code_insee,
    ROW_NUMBER() OVER (
      PARTITION BY w.date, w.ville
      ORDER BY c2.code_insee DESC, c1.Departement DESC
    ) AS rn
  FROM weather w
  LEFT JOIN communes c1
    ON w.numero_departement = c1.numero_departement
  LEFT JOIN communes c2
    ON REGEXP_REPLACE(
         NORMALIZE(LOWER(TRIM(w.ville)), NFD), r"[\pM'’\-\s]", ''
       )
     = REGEXP_REPLACE(
         NORMALIZE(LOWER(TRIM(c2.Commune)), NFD), r"[\pM'’\-\s]", ''
       )
)

SELECT * EXCEPT(rn)
FROM joined
WHERE rn = 1