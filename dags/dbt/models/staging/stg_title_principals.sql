{{ config(
    materialized='table',
    tags=['staging'],
    indexes=[
        {'columns': ['tconst']},
        {'columns': ['nconst']},
        {'columns': ['role_category']},
        {'columns': ['tconst', 'ordering']},
        {'columns': ['tconst', 'role_category']},
        {'columns': ['nconst', 'role_category']}
    ]
) }}
--Indexes:
    -- tconst: Title-based lookups
    -- nconst: Person-based lookups  
    -- role_category: Role filtering
    -- tconst, ordering: Credit ordering
    -- tconst, role_category: Title + role queries
    -- nconst, role_category: Person + role queries


WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_principals') }}
),

cleaned_data AS (
    SELECT
        -- Composite primary key
        tconst,
        CAST(ordering AS INTEGER) AS ordering,
        
        -- Person reference
        nconst,
        
        -- Role information
        LOWER(TRIM(category)) AS category,
        NULLIF(job, '\N') AS job_title,
        NULLIF(characters, '\N') AS characters,
        
        -- Standardized role categories (production standard)
        CASE 
            WHEN LOWER(category) IN ('actor', 'actress') THEN 'actor'
            WHEN LOWER(category) = 'director' THEN 'director'
            WHEN LOWER(category) IN ('writer') THEN 'writer'
            WHEN LOWER(category) = 'producer' THEN 'producer'
            WHEN LOWER(category) = 'cinematographer' THEN 'cinematographer'
            WHEN LOWER(category) = 'composer' THEN 'composer'
            WHEN LOWER(category) = 'editor' THEN 'editor'
            WHEN LOWER(category) = 'self' THEN 'self'
            ELSE 'other'
        END AS role_category,
        
        -- Data quality indicators
        CASE WHEN category IS NULL OR category = '' THEN TRUE ELSE FALSE END AS is_missing_category,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source_data
    WHERE tconst IS NOT NULL 
      AND nconst IS NOT NULL
      AND ordering IS NOT NULL
)

SELECT *
FROM cleaned_data