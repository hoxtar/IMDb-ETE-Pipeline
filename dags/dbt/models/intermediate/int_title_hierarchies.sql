{{ config(
    materialized='view',
    tags=['intermediate']
) }}

-- LEARNING NOTE: Materialized as view for intermediate layer best practices  
-- Views provide: fast development iteration, real-time data freshness, minimal storage
-- Complex business logic performed efficiently leveraging indexed staging tables

-- LEARNING OBJECTIVE: Advanced TV series analytics with hierarchical data relationships
-- Demonstrates complex analytical engineering: statistical analysis, business logic, and dimensional modeling
-- Showcases understanding of parent-child data relationships and aggregation patterns

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
),

-- LEARNING: Get rating data for quality analysis
episode_ratings AS (
    SELECT
        tconst,
        average_rating,
        num_votes
    FROM {{ ref('stg_title_ratings') }}
),

-- LEARNING: Enhanced base relationships with quality metrics
base_hierarchy AS (
    SELECT
        e.tconst AS episode_tconst,
        e.parenttconst AS series_tconst,
        et.episode_title,
        st.series_title,
        e.season_number,
        e.episode_number,
        et.episode_year,
        st.series_start_year,
        st.series_end_year,
        er.average_rating AS episode_rating,
        er.num_votes AS episode_votes,
        
        -- LEARNING: Calculate series duration for longevity analysis
        COALESCE(st.series_end_year, EXTRACT(YEAR FROM CURRENT_DATE)) - st.series_start_year + 1 AS series_duration_years
        
    FROM episodes e
    JOIN series_titles st ON e.parenttconst = st.tconst
    JOIN episode_titles et ON e.tconst = et.tconst
    LEFT JOIN episode_ratings er ON e.tconst = er.tconst
),

-- LEARNING: Series-level analytics for comprehensive insights (FIXED AGGREGATION)
series_analytics AS (
    SELECT
        series_tconst,
        series_title,
        series_start_year,
        series_end_year,
        MAX(series_duration_years) AS series_duration_years,  -- FIXED: Use MAX to get single value
        
        -- Episode volume metrics
        COUNT(DISTINCT episode_tconst) AS total_episodes,
        COUNT(DISTINCT season_number) AS total_seasons,
        
        -- LEARNING: Calculate average episodes per season properly (FIXED)
        ROUND(COUNT(DISTINCT episode_tconst)::DECIMAL / GREATEST(COUNT(DISTINCT season_number), 1), 1) AS avg_episodes_per_season,
        
        -- Quality metrics (learning: aggregation patterns)
        AVG(episode_rating) AS avg_series_rating,
        STDDEV(episode_rating) AS rating_consistency,
        MAX(episode_rating) AS highest_episode_rating,
        MIN(episode_rating) AS lowest_episode_rating,
        
        -- Audience engagement metrics
        AVG(episode_votes) AS avg_episode_votes,
        SUM(episode_votes) AS total_audience_engagement,
        
        -- Season analysis
        MAX(season_number) AS latest_season
        
    FROM base_hierarchy
    WHERE episode_rating IS NOT NULL  -- Focus on rated content
    GROUP BY series_tconst, series_title, series_start_year, series_end_year  -- FIXED: Removed calculated field
),

-- LEARNING: Create business-ready classifications for dashboard use
enhanced_hierarchy AS (
    SELECT
        bh.*,
        sa.total_episodes,
        sa.total_seasons,
        sa.avg_series_rating,
        sa.rating_consistency,
        sa.highest_episode_rating,
        sa.lowest_episode_rating,
        sa.avg_episode_votes,
        sa.total_audience_engagement,
        sa.avg_episodes_per_season,
        
        -- LEARNING: Series longevity classification (dashboard-ready dimension)
        CASE 
            WHEN sa.series_duration_years >= 15 THEN 'Long-Running (15+ years)'
            WHEN sa.series_duration_years >= 10 THEN 'Established (10-14 years)'
            WHEN sa.series_duration_years >= 5 THEN 'Mid-Length (5-9 years)'
            WHEN sa.series_duration_years >= 3 THEN 'Short-Run (3-4 years)'
            ELSE 'Limited Series (1-2 years)'
        END AS series_longevity_class,
        
        -- LEARNING: Series performance tier (learning: percentile-based classification)
        CASE 
            WHEN sa.avg_series_rating >= 8.5 AND sa.total_episodes >= 20 THEN 'Premium Series'
            WHEN sa.avg_series_rating >= 8.0 AND sa.total_episodes >= 10 THEN 'High Quality'
            WHEN sa.avg_series_rating >= 7.5 THEN 'Good Series'
            WHEN sa.avg_series_rating >= 7.0 THEN 'Average Series'
            WHEN sa.avg_series_rating IS NOT NULL THEN 'Below Average'
            ELSE 'Unrated'
        END AS series_performance_tier,
        
        -- LEARNING: Episode quality classification relative to series average
        CASE 
            WHEN bh.episode_rating IS NULL THEN 'Unrated'
            WHEN bh.episode_rating >= sa.avg_series_rating + 0.5 THEN 'Standout Episode'
            WHEN bh.episode_rating >= sa.avg_series_rating + 0.2 THEN 'Above Average'
            WHEN bh.episode_rating >= sa.avg_series_rating - 0.2 THEN 'Typical'
            WHEN bh.episode_rating >= sa.avg_series_rating - 0.5 THEN 'Below Average'
            ELSE 'Poor Episode'
        END AS episode_quality_class,
        
        -- LEARNING: Season position analysis (useful for trend analysis)
        CASE 
            WHEN bh.season_number = 1 THEN 'Premiere Season'
            WHEN bh.season_number = sa.latest_season THEN 'Latest Season'
            WHEN bh.season_number <= 3 THEN 'Early Seasons'
            WHEN bh.season_number >= sa.latest_season - 2 THEN 'Recent Seasons'
            ELSE 'Middle Seasons'
        END AS season_position,
        
        -- LEARNING: Consistency indicator for series quality
        CASE 
            WHEN sa.rating_consistency <= 0.3 THEN 'Very Consistent'
            WHEN sa.rating_consistency <= 0.5 THEN 'Consistent'
            WHEN sa.rating_consistency <= 0.8 THEN 'Moderately Variable'
            WHEN sa.rating_consistency <= 1.2 THEN 'Variable'
            ELSE 'Highly Variable'
        END AS series_consistency_class,
        
        -- LEARNING: Engagement level based on voting patterns
        CASE 
            WHEN sa.avg_episode_votes >= 5000 THEN 'High Engagement'
            WHEN sa.avg_episode_votes >= 1000 THEN 'Moderate Engagement'
            WHEN sa.avg_episode_votes >= 100 THEN 'Low Engagement'
            WHEN sa.avg_episode_votes IS NOT NULL THEN 'Minimal Engagement'
            ELSE 'No Engagement Data'
        END AS audience_engagement_level
        
    FROM base_hierarchy bh
    LEFT JOIN series_analytics sa ON bh.series_tconst = sa.series_tconst
)

SELECT * FROM enhanced_hierarchy
ORDER BY series_tconst, season_number, episode_number