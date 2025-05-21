{{ config(
    materialized = 'table',
    tags = ['marts']
) }}

WITH title_complete AS (
    SELECT * FROM {{ ref('int_title_complete') }}
),

filmography AS (
    SELECT * FROM {{ ref('int_person_filmography') }}
)

SELECT 
    tc.tconst,
    tc.title_type,
    tc.primary_title,
    tc.start_year,
    tc.runtime_minutes,
    tc.primary_genre,
    tc.secondary_genre,
    tc.average_rating,
    tc.num_votes,
    tc.director_count,
    tc.writer_count,
    tc.cast_count,
    COUNT(DISTINCT CASE WHEN pf.role = 'actor' THEN pf.nconst END) AS male_actor_count,
    COUNT(DISTINCT CASE WHEN pf.role = 'actress' THEN pf.nconst END) AS female_actor_count,
    ROW_NUMBER() OVER (PARTITION BY tc.title_type ORDER BY tc.average_rating DESC, tc.num_votes DESC) AS rating_rank_in_type,
    ROW_NUMBER() OVER (PARTITION BY tc.primary_genre ORDER BY tc.average_rating DESC, tc.num_votes DESC) AS rating_rank_in_genre
FROM title_complete tc
LEFT JOIN filmography pf ON tc.tconst = pf.tconst
WHERE tc.num_votes > 1000  -- Minimum threshold for statistical significance
GROUP BY 
    tc.tconst,
    tc.title_type,
    tc.primary_title,
    tc.start_year,
    tc.runtime_minutes,
    tc.primary_genre,
    tc.secondary_genre,
    tc.average_rating,
    tc.num_votes,
    tc.director_count,
    tc.writer_count,
    tc.cast_count