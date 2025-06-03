{{ config(materialized='view', tags=['intermediate']) }}

WITH title_basics AS (
    SELECT * FROM {{ ref('stg_title_basics') }}
),

-- Flatten the genres into separate rows
genres AS (
    SELECT
        tconst,
        primary_genre AS genre,
        1 AS genre_rank
    FROM title_basics
    WHERE primary_genre IS NOT NULL AND primary_genre != ''
    
    UNION ALL
    
    SELECT
        tconst,
        secondary_genre AS genre,
        2 AS genre_rank
    FROM title_basics
    WHERE secondary_genre IS NOT NULL AND secondary_genre != ''
    
    UNION ALL
    
    SELECT
        tconst,
        third_genre AS genre,
        3 AS genre_rank
    FROM title_basics
    WHERE third_genre IS NOT NULL AND third_genre != ''
)

-- Final normalized genres table
SELECT
    tconst,
    genre,
    genre_rank
FROM genres
