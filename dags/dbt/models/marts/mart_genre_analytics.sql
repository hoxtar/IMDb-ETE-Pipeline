{{ config(
    materialized = 'table',
    tags = ['marts']
) }}

WITH genres AS (
    SELECT * FROM {{ ref('int_title_with_genres') }}
),

ratings AS (
    SELECT * FROM {{ ref('int_title_with_ratings') }}
),

title_basics AS (
    SELECT * FROM {{ ref('stg_title_basics') }}
)

SELECT
    g.genre,
    COUNT(DISTINCT g.tconst) AS title_count,
    AVG(r.average_rating) AS avg_rating,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.average_rating) AS median_rating,
    AVG(r.num_votes) AS avg_votes,
    SUM(r.num_votes) AS total_votes,
    
    -- Decade breakdowns
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 1950 AND 1959 THEN g.tconst END) AS titles_1950s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 1960 AND 1969 THEN g.tconst END) AS titles_1960s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 1970 AND 1979 THEN g.tconst END) AS titles_1970s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 1980 AND 1989 THEN g.tconst END) AS titles_1980s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 1990 AND 1999 THEN g.tconst END) AS titles_1990s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 2000 AND 2009 THEN g.tconst END) AS titles_2000s,
    COUNT(DISTINCT CASE WHEN tb.start_year BETWEEN 2010 AND 2019 THEN g.tconst END) AS titles_2010s,
    COUNT(DISTINCT CASE WHEN tb.start_year >= 2020 THEN g.tconst END) AS titles_2020s,
    
    -- Rating trends by decade
    AVG(CASE WHEN tb.start_year BETWEEN 2010 AND 2019 THEN r.average_rating END) AS avg_rating_2010s,
    AVG(CASE WHEN tb.start_year BETWEEN 2000 AND 2009 THEN r.average_rating END) AS avg_rating_2000s,
    AVG(CASE WHEN tb.start_year BETWEEN 1990 AND 1999 THEN r.average_rating END) AS avg_rating_1990s,
    
    -- Type analytics
    COUNT(DISTINCT CASE WHEN tb.title_type = 'movie' THEN g.tconst END) AS movie_count,
    COUNT(DISTINCT CASE WHEN tb.title_type = 'tvSeries' THEN g.tconst END) AS tvseries_count,
    
    -- High-quality titles
    COUNT(DISTINCT CASE WHEN r.average_rating >= 8.0 AND r.num_votes > 5000 THEN g.tconst END) AS high_rated_count
FROM genres g
LEFT JOIN ratings r ON g.tconst = r.tconst
LEFT JOIN title_basics tb ON g.tconst = tb.tconst
WHERE g.genre IS NOT NULL
GROUP BY g.genre