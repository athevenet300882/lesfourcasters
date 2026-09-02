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
    EXTRACT(DAYOFWEEK FROM date) as jour_semaine,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN true ELSE false END as est_weekend,
    CURRENT_TIMESTAMP() as inserted_at
FROM date_list
ORDER BY date