{{ config(
    materialized='table',
    tags=['staging'],
    indexes=[
        {'columns': ['tconst']},
        {'columns': ['writer_nconst']},
        {'columns': ['tconst', 'writer_nconst'], 'unique': True},
        {'columns': ['writer_order']}
    ]
) }}
-- Indexes:
    -- Index on tconst column for title lookups
    -- Index on writer_nconst column for writer lookups  
    -- Composite unique index on tconst, writer_nconst columns as composite PK
    -- Index on writer_order column for primary writer queries

WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_crew') }}
),

raw_data AS (
    SELECT
        tconst,
        writers,
        string_to_array(writers, ',') AS writer_array
    FROM source_data
    WHERE writers IS NOT NULL 
      AND writers != '\N'
      AND writers != ''
),

normalized_writers AS (
    SELECT
        tconst,
        TRIM(unnest(writer_array)) AS writer_nconst
    FROM raw_data
),

cleaned_data AS (
    SELECT
        tconst,
        writer_nconst,
        
        -- Data quality indicators
        CASE 
            WHEN writer_nconst IS NULL OR writer_nconst = '' OR writer_nconst = '\N' 
            THEN TRUE 
            ELSE FALSE 
        END AS is_invalid_writer,
        
        -- Position tracking
        ROW_NUMBER() OVER (PARTITION BY tconst ORDER BY writer_nconst) AS writer_order,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM normalized_writers
    WHERE writer_nconst IS NOT NULL
      AND writer_nconst != ''
      AND writer_nconst != '\N'
)

SELECT *
FROM cleaned_data