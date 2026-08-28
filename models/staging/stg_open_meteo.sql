{{ config(
    materialized = 'table',
    tags = ['staging', 'weather']
) }}

SELECT
  time AS date,
  weather_code AS code_meteo,
  temperature_2m_mean AS temperature_moyenne,
  temperature_2m_min AS temperature_minimale,
  temperature_2m_max AS temperature_maximale,
  apparent_temperature_mean AS temperature_ressentie_moyenne,
  apparent_temperature_min AS temperature_ressentie_minimale,
  apparent_temperature_max AS temperature_ressentie_maximale,
  relative_humidity_2m_mean AS humidite_moyenne,
  relative_humidity_2m_min AS humidite_minimale,
  relative_humidity_2m_max AS humidite_maximale,
  dew_point_2m_mean AS point_de_rosee_moyen,
  precipitation_sum AS precipitations_totales,
  rain_sum AS pluie_totale,
  snowfall_sum AS neige_totale,
  precipitation_hours AS heures_de_precipitations,
  wind_speed_10m_mean AS vitesse_vent_moyenne,
  wind_speed_10m_max AS vitesse_vent_maximale,
  wind_gusts_10m_max AS rafale_vent_maximale,
  wind_direction_10m_dominant AS direction_vent_dominante,
  cloud_cover_mean AS couverture_nuageuse_moyenne,
  pressure_msl_mean AS pression_moyenne,
  sunshine_duration AS duree_ensoleillement,
  shortwave_radiation_sum AS rayonnement_solaire_total,
  et0_fao_evapotranspiration AS evapotranspiration,
  vapour_pressure_deficit_max AS deficit_pression_vapeur_maximal,
  soil_moisture_0_to_7cm_mean AS humidite_sol_0_7cm,
  soil_moisture_7_to_28cm_mean AS humidite_sol_7_28cm,
  soil_moisture_28_to_100cm_mean AS humidite_sol_28_100cm,
  soil_temperature_0_to_7cm_mean AS temperature_sol_0_7cm,
  IFNULL(Ville, nom_poi) AS ville,
  LPAD(numero_departement, 2, '0') AS numero_departement,
  latitude_poi AS latitude,
  longitude_poi AS longitude,
  CURRENT_TIMESTAMP() AS inserted_at
FROM {{ source('raw', 'raw_open_meteo') }}
WHERE time IS NOT NULL
  AND temperature_2m_mean IS NOT NULL