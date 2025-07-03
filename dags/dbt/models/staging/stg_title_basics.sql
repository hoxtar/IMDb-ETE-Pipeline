{{ config(
    materialized = 'table',
    tags = ['staging'],
    indexes=[
        {'columns': ['tconst'], 'unique': True},
        {'columns': ['title_type']},
        {'columns': ['start_year']},
        {'columns': ['primary_genre']},
        {'columns': ['start_year', 'title_type']},
        {'columns': ['is_adult', 'title_type']}
    ]
) }}
-- Indexes:
    -- tconst: Primary key lookup
    -- title_type: Filter by type
    -- start_year: Time-based queries
    -- primary_genre: Genre analysis
    -- start_year, title_type: Composite for dashboards
    -- is_adult, title_type: Content filtering

WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_basics') }}
),

cleaned_data AS (
    SELECT
        -- Primary identifier
        tconst,
        --
        titletype AS title_type,

        CASE
            WHEN primarytitle LIKE 'Episode #%.%' THEN REGEXP_REPLACE(primarytitle, 'Episode #(\d+)\.(\d+)', 'S\1.E\2')
            WHEN primarytitle IS NULL OR primarytitle = '' THEN 'Unknown Title'
            ELSE primarytitle
        END AS primary_title,

        NULLIF(originalTitle, '') AS original_title,
        
        CASE isAdult
            WHEN '0' THEN FALSE
            WHEN '1' THEN TRUE
            ELSE NULL
        END AS is_adult,

        -- Year handling
        CASE 
            WHEN startYear::TEXT = '\N' THEN NULL
            WHEN CAST(startYear AS INTEGER) BETWEEN 1800 AND 2030 THEN CAST(startYear AS INTEGER)
            ELSE NULL
        END AS start_year,
        
        CASE 
            WHEN endYear::TEXT = '\N' THEN NULL
            WHEN CAST(endYear AS INTEGER) BETWEEN 1800 AND 2030 THEN CAST(endYear AS INTEGER)
            ELSE NULL
        END AS end_year,

        -- Runtime (with bounds checking)
        CASE 
            WHEN runtimeMinutes::TEXT = '\N' THEN NULL
            WHEN CAST(runtimeMinutes AS INTEGER) BETWEEN 1 AND 2000 THEN CAST(runtimeMinutes AS INTEGER)
            ELSE NULL
        END AS runtime_minutes,

        -- Genre normalization (keep raw for flexibility)
        NULLIF(genres, '\N') AS genres_raw,
        -- Split genres into individual columns 
        SPLIT_PART(genres, ',', 1) AS primary_genre,
        SPLIT_PART(genres, ',', 2) AS secondary_genre,
        SPLIT_PART(genres, ',', 3) AS third_genre,

        -- Data quality indicators
        CASE WHEN primaryTitle IS NULL OR primaryTitle = '' THEN TRUE ELSE FALSE END AS is_missing_title,
        CASE WHEN genres = '\N' OR genres IS NULL THEN TRUE ELSE FALSE END AS is_missing_genres,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at

    FROM source_data
    WHERE  tconst IS NOT NULL 
      AND tconst != ''
)

SELECT *
FROM cleaned_data


