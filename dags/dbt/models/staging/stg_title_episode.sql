{{ config(
    materialized='table',
    tags=['staging'],
    indexes=[
        {'columns': ['tconst'], 'unique': True},
        {'columns': ['parentTconst']}
    ]
) }}


WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_episode') }}
),

cleaned_data AS (
    SELECT
        -- Episode identifier
        tconst,
        
        -- Series identifier  
        parentTconst,
        
        -- Episode positioning
        CASE 
            WHEN seasonNumber::TEXT = '\N' THEN NULL
            ELSE CAST(seasonNumber AS INTEGER)
        END AS season_number,
        
        CASE 
            WHEN episodeNumber::TEXT = '\N' THEN NULL
            ELSE CAST(episodeNumber AS INTEGER)
        END AS episode_number,
        
        -- Data quality indicators
        CASE WHEN seasonNumber::TEXT = '\N' THEN TRUE ELSE FALSE END AS is_missing_season,
        CASE WHEN episodeNumber::TEXT = '\N' THEN TRUE ELSE FALSE END AS is_missing_episode,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source_data
    WHERE tconst IS NOT NULL 
      AND parentTconst IS NOT NULL
)

SELECT *
FROM cleaned_data

