{{ config(
    materialized = 'table',
    tags = ['marts']
) }}

WITH filmography AS (
    SELECT * FROM {{ ref('int_person_filmography') }}
),

title_ratings AS (
    SELECT * FROM {{ ref('int_title_with_ratings') }}
)

SELECT
    pf.nconst,
    pf.person_name,
    MIN(pf.start_year) AS career_start_year,
    MAX(pf.start_year) AS latest_work_year,
    GREATEST(MAX(pf.start_year) - MIN(pf.start_year), 0) AS career_span_years,
    
    -- Role counts
    COUNT(DISTINCT pf.tconst) AS total_credits,
    COUNT(DISTINCT CASE WHEN pf.role = 'actor' THEN pf.tconst END) AS actor_credits,
    COUNT(DISTINCT CASE WHEN pf.role = 'actress' THEN pf.tconst END) AS actress_credits,
    COUNT(DISTINCT CASE WHEN pf.role = 'director' THEN pf.tconst END) AS director_credits,
    COUNT(DISTINCT CASE WHEN pf.role = 'writer' THEN pf.tconst END) AS writer_credits,
    COUNT(DISTINCT CASE WHEN pf.role = 'producer' THEN pf.tconst END) AS producer_credits,
    
    -- Rating metrics
    AVG(tr.average_rating) AS avg_title_rating,
    MAX(tr.average_rating) AS highest_rated_work,
    MIN(tr.average_rating) AS lowest_rated_work,
    
    -- Genre focus (primary genres they work in most)
    MODE() WITHIN GROUP (ORDER BY pf.primary_genre) AS most_common_genre,
    
    -- Popularity metrics
    SUM(tr.num_votes) AS total_votes_across_works,
    AVG(tr.num_votes) AS avg_votes_per_work,
    
    -- Multi-role indicator
    CASE 
        WHEN COUNT(DISTINCT pf.role) > 1 THEN TRUE 
        ELSE FALSE 
    END AS has_multiple_roles
FROM filmography pf
LEFT JOIN title_ratings tr ON pf.tconst = tr.tconst
GROUP BY pf.nconst, pf.person_name
HAVING COUNT(DISTINCT pf.tconst) >= 3  -- Focus on people with at least 3 credits