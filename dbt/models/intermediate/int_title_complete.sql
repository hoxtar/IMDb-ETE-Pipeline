{{ config(materialized='view', tags=['intermediate']) }}

WITH basics AS (
    SELECT * FROM {{ ref('stg_title_basics') }}
),

ratings AS (
    SELECT * FROM {{ ref('stg_title_ratings') }}
),

director_counts AS (
    SELECT 
        tconst,
        COUNT(*) AS director_count
    FROM {{ ref('stg_title_crew_directors') }}
    GROUP BY tconst
),

writer_counts AS (
    SELECT 
        tconst,
        COUNT(*) AS writer_count
    FROM {{ ref('stg_title_crew_writers') }}
    GROUP BY tconst
),

cast_counts AS (
    SELECT
        tconst,
        COUNT(*) AS cast_count
    FROM {{ ref('stg_title_principals') }}
    WHERE category IN ('actor', 'actress')
    GROUP BY tconst
)

-- Comprehensive title information
SELECT
    b.tconst,
    b.title_type,
    b.primary_title,
    b.original_title,
    b.is_adult,
    b.start_year,
    b.end_year,
    b.runtime_minutes,
    b.primary_genre,
    b.secondary_genre,
    b.third_genre,
    r.average_rating,
    r.num_votes,
    dc.director_count,
    wc.writer_count,
    cc.cast_count
FROM basics b
LEFT JOIN ratings r ON b.tconst = r.tconst
LEFT JOIN director_counts dc ON b.tconst = dc.tconst
LEFT JOIN writer_counts wc ON b.tconst = wc.tconst
LEFT JOIN cast_counts cc ON b.tconst = cc.tconst
