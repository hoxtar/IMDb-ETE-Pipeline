{{ config(
    materialized='table',
    tags=['staging'],
    indexes=[
        {'columns': ['tconst']},
        {'columns': ['director_nconst']},
        {'columns': ['tconst', 'director_nconst'], 'unique': True},
        {'columns': ['director_order']}
    ]
) }}
-- Indexes:
    -- tconst: Title lookups
    -- director_nconst: Director lookups  
    -- tconst, director_nconst: Composite PK
    -- director_order: Primary director queries

WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_crew') }}
),

raw_data AS (
    SELECT
        tconst,
        directors,
        string_to_array(directors, ',') AS director_array
    FROM source_data
    WHERE directors IS NOT NULL 
      AND directors != '\N'  -- Handle IMDb null values
      AND directors != ''
),

normalized_directors AS (
    SELECT
        tconst,
        TRIM(unnest(director_array)) AS director_nconst
    FROM raw_data
),

cleaned_data AS (
    SELECT
        tconst,
        director_nconst,
        
        -- Data quality indicators
        CASE 
            WHEN director_nconst IS NULL OR director_nconst = '' OR director_nconst = '\N' 
            THEN TRUE 
            ELSE FALSE 
        END AS is_invalid_director,
        
        -- Position tracking (useful for "main director" analysis)
        ROW_NUMBER() OVER (PARTITION BY tconst ORDER BY director_nconst) AS director_order,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM normalized_directors
    WHERE director_nconst IS NOT NULL
      AND director_nconst != ''
      AND director_nconst != '\N'
)

SELECT *
FROM cleaned_data