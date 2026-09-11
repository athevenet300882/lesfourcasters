{{ config(
    materialized='table',
    description='Staging des communes depuis le référentiel'
) }}

WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'raw_communes_referentiel') }}
),

cleaned AS (
    SELECT
        -- Clés - LPAD pour ajouter les zéros devant
        CASE 
            WHEN REGEXP_CONTAINS(CAST(`code INSEE` AS STRING), r'^2[AB]') THEN CAST(`code INSEE` AS STRING)
            ELSE LPAD(CAST(`code INSEE` AS STRING), 5, '0')
        END AS code_insee,
        
        -- Géographie
        TRIM(Commune) AS ville,
        TRIM(Departement) AS departement,
        TRIM(`Région`) AS region,
        CAST(Numero_Departement AS STRING) AS numero_departement,
        
        -- Localisation
        CAST(Latitude AS FLOAT64) AS latitude,
        CAST(Longitude AS FLOAT64) AS longitude,
        TRIM(Centroide) AS centroide,
        
        -- Administration
        TRIM(Service) AS service,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS inserted_at,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
        
    FROM source
    
    WHERE `code INSEE` IS NOT NULL
        AND Commune IS NOT NULL
        AND TRIM(Commune) != ''
        AND Latitude IS NOT NULL
        AND Longitude IS NOT NULL
),

deduped AS (
    SELECT
        * EXCEPT(row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY code_insee 
                ORDER BY inserted_at DESC
            ) AS row_num
        FROM cleaned
    )
    WHERE row_num = 1
)

SELECT * FROM deduped