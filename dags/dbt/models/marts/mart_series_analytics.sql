-- models/marts/mart_series_analytics.sql
-- filepath: c:\Users\andry\OneDrive\Desktop\Projects\IMDb\apache-airflow-dev-server\dags\dbt\models\marts\mart_series_analytics.sql

{{ config(
    materialized = 'table',
    tags = ['marts']
) }}

WITH title_hierarchies AS (
    SELECT * FROM {{ ref('int_title_hierarchies') }}
),

title_complete AS (
    SELECT * FROM {{ ref('int_title_complete') }}
),

episode_data AS (
    SELECT
        th.series_tconst,
        th.episode_tconst,
        th.series_title,
        th.season_number,
        th.episode_number,
        th.episode_year,
        tc.average_rating,
        tc.num_votes
    FROM title_hierarchies th
    LEFT JOIN title_complete tc ON th.episode_tconst = tc.tconst
),

-- Pre-calculate max season per series
series_seasons AS (
    SELECT 
        series_tconst,
        MAX(season_number) AS max_season
    FROM episode_data
    GROUP BY series_tconst
),

-- Add max season info to episode data
enriched_episode_data AS (
    SELECT 
        ed.*,
        ss.max_season,
        CASE WHEN ed.season_number = ss.max_season THEN ed.average_rating END AS last_season_rating
    FROM episode_data ed
    LEFT JOIN series_seasons ss ON ed.series_tconst = ss.series_tconst
)

SELECT
    ed.series_tconst,
    ed.series_title,
    tc.start_year AS series_start_year,
    tc.end_year AS series_end_year,
    tc.average_rating AS series_rating,
    tc.num_votes AS series_votes,
    tc.primary_genre AS primary_genre,
    
    -- Episode counts
    COUNT(DISTINCT ed.episode_tconst) AS total_episodes,
    COUNT(DISTINCT ed.season_number) AS total_seasons,
    MAX(ed.season_number) AS max_season_number,
    
    -- Episode metrics
    AVG(ed.average_rating) AS avg_episode_rating,
    MAX(ed.average_rating) AS highest_episode_rating,
    MIN(ed.average_rating) FILTER (WHERE ed.average_rating IS NOT NULL) AS lowest_episode_rating,
    
    -- Popular episodes
    COUNT(DISTINCT CASE WHEN ed.average_rating > 8.0 THEN ed.episode_tconst END) AS highly_rated_episodes,
    
    -- Trend analysis (fixed)
    CASE
        WHEN CORR(ed.average_rating, ed.season_number) > 0.1 THEN 'Improving'
        WHEN CORR(ed.average_rating, ed.season_number) < -0.1 THEN 'Declining'
        ELSE 'Stable'
    END AS rating_trend,
    
    -- First season vs. last season comparison (fixed)
    AVG(CASE WHEN ed.season_number = 1 THEN ed.average_rating END) AS first_season_avg_rating,
    AVG(ed.last_season_rating) AS last_season_avg_rating  -- Fixed: no more window function in aggregate

FROM enriched_episode_data ed
LEFT JOIN title_complete tc ON ed.series_tconst = tc.tconst
GROUP BY 
    ed.series_tconst,
    ed.series_title,
    tc.start_year,
    tc.end_year,
    tc.average_rating,
    tc.num_votes,
    tc.primary_genre
HAVING COUNT(DISTINCT ed.episode_tconst) >= 5  -- Only include series with at least 5 episodes