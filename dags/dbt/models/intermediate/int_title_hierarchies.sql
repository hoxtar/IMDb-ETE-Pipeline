{{ config(materialized='view', tags=['intermediate']) }}

WITH episodes AS (
    SELECT * FROM {{ ref('stg_title_episode') }}
),

series_titles AS (
    SELECT
        tconst,
        primary_title AS series_title,
        start_year AS series_start_year,
        end_year AS series_end_year
    FROM {{ ref('stg_title_basics') }}
    WHERE title_type = 'tvSeries'
),

episode_titles AS (
    SELECT
        tconst,
        primary_title AS episode_title,
        start_year AS episode_year
    FROM {{ ref('stg_title_basics') }}
    WHERE title_type = 'tvEpisode'
)

-- Connect episodes to their parent series with titles
SELECT
    e.tconst AS episode_tconst,
    e.parent_tconst AS series_tconst,
    et.episode_title,
    st.series_title,
    e.season_number,
    e.episode_number,
    et.episode_year,
    st.series_start_year,
    st.series_end_year
FROM episodes e
JOIN series_titles st ON e.parent_tconst = st.tconst
JOIN episode_titles et ON e.tconst = et.tconst
