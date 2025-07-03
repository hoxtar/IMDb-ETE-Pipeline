{{ config(
    materialized='table',
    tags=['staging'],
    indexes=[
        {'columns': ['nconst'], 'unique': True},
        {'columns': ['primary_name']},
        {'columns': ['first_profession']},
        {'columns': ['birth_year']},
        {'columns': ['is_deceased']}
    ]
) }}

-- Indexes:
    -- nconst: Primary key
    -- primary_name: Name searches
    -- first_profession: Career analysis
    -- birth_year: Age-based queries
    -- is_deceased: Living vs deceased


WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'name_basics') }}
),

cleaned_data AS (
    SELECT
        -- Primary identifier
        nconst,
        
        -- Core attributes
        COALESCE(NULLIF(primaryName, ''), 'Unknown Person') AS primary_name,
        
        -- Birth/death years (with validation)
        CASE 
            WHEN birthYear::TEXT = '\N' THEN NULL
            WHEN CAST(birthYear AS INTEGER) BETWEEN 1800 AND DATE_PART('year', CURRENT_DATE) THEN CAST(birthYear AS INTEGER)
            ELSE NULL
        END AS birth_year,
        
        CASE 
            WHEN deathYear::TEXT = '\N' THEN NULL
            WHEN CAST(deathYear AS INTEGER) BETWEEN 1800 AND DATE_PART('year', CURRENT_DATE) THEN CAST(deathYear AS INTEGER)
            ELSE NULL
        END AS death_year,
        
        -- Professions (keep raw + normalized)
        NULLIF(primaryProfession, '\N') AS professions_raw,
        
        -- Split professions (production pattern)
        SPLIT_PART(NULLIF(primaryProfession, '\N'), ',', 1) AS first_profession,
        NULLIF(SPLIT_PART(NULLIF(primaryProfession, '\N'), ',', 2), '') AS second_profession,
        NULLIF(SPLIT_PART(NULLIF(primaryProfession, '\N'), ',', 3), '') AS third_profession,
        
        -- Known for titles (keep raw for flexibility)
        NULLIF(knownForTitles, '\N') AS known_for_titles_raw,
        
        -- Split known for titles
        SPLIT_PART(NULLIF(knownForTitles, '\N'), ',', 1) AS first_title_known_for,
        NULLIF(SPLIT_PART(NULLIF(knownForTitles, '\N'), ',', 2), '') AS second_title_known_for,
        NULLIF(SPLIT_PART(NULLIF(knownForTitles, '\N'), ',', 3), '') AS third_title_known_for,
        NULLIF(SPLIT_PART(NULLIF(knownForTitles, '\N'), ',', 4), '') AS fourth_title_known_for,
        
        -- Calculated fields
        CASE 
            WHEN deathYear IS NOT NULL AND birthYear IS NOT NULL THEN deathYear - birthYear
            ELSE NULL
        END AS death_age,

        CASE 
            WHEN birthYear IS NOT NULL AND deathYear IS NULL THEN DATE_PART('year', CURRENT_DATE) - birthYear
            ELSE NULL
        END AS actual_age,
        
        CASE WHEN deathYear IS NOT NULL THEN TRUE ELSE FALSE END AS is_deceased,
        
        -- Data quality indicators
        CASE WHEN primaryName IS NULL OR primaryName = '' THEN TRUE ELSE FALSE END AS is_missing_name,
        CASE WHEN primaryProfession = '\N' OR primaryProfession IS NULL THEN TRUE ELSE FALSE END AS is_missing_professions,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source_data
    WHERE nconst IS NOT NULL 
      AND nconst != ''
)

SELECT *
FROM cleaned_data