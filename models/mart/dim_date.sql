-- models/mart/dim_date.sql

{{ config(
    materialized='table',
    schema='lesfourcasters_dbt',
    tags=['mart', 'dimension']
) }}

WITH date_list AS (
    SELECT DISTINCT
        CAST(time AS DATE) as date
    FROM {{ source('raw', 'raw_open_meteo') }}
    WHERE time IS NOT NULL
)

SELECT
    date,
    EXTRACT(YEAR FROM date) as annee,
    EXTRACT(MONTH FROM date) as mois,
    CASE EXTRACT(MONTH FROM date)
        WHEN 1 THEN 'Janv.'
        WHEN 2 THEN 'Févr.'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avr.'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juil.'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Sept.'
        WHEN 10 THEN 'Oct.'
        WHEN 11 THEN 'Nov.'
        WHEN 12 THEN 'Déc.'
    END as nom_mois,
    DATE_TRUNC(date, MONTH) as debut_mois,
    EXTRACT(DAYOFWEEK FROM date) as jour_semaine,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN true ELSE false END as est_weekend,
    CURRENT_TIMESTAMP() as inserted_at
FROM date_list
ORDER BY date