{{ config(
    materialized = 'table',
    tags = ['marts'],
    indexes=[
        {'columns': ['series_tconst'], 'unique': True},
        {'columns': ['series_performance_tier']},
        {'columns': ['longevity_class']},
        {'columns': ['consistency_rating']}
    ]
) }}

-- LEARNING OBJECTIVE: TV series-specific analytics for entertainment industry insights
-- This mart demonstrates hierarchical data analysis and television business intelligence
-- Showcases: season analysis, series lifecycle patterns, audience retention metrics

-- BUSINESS PURPOSE: Enable TV industry analysis and series development decisions
-- Key Use Cases:
-- 1. Series development and renewal decisions
-- 2. Audience retention and engagement analysis
-- 3. Industry benchmarking for TV content strategy
-- 4. Season planning and optimal series length analysis

WITH series_metrics AS (
    -- LEARNING: Aggregation from hierarchical intermediate model
    -- Focus on series-level insights from episode-level data
    SELECT
        series_tconst,
        series_title,
        series_start_year,
        series_end_year,
        series_duration_years,
        
        -- LEARNING: Episode volume and structure analysis
        COUNT(DISTINCT episode_tconst) AS total_episodes,
        COUNT(DISTINCT season_number) AS total_seasons,
        ROUND(COUNT(DISTINCT episode_tconst)::DECIMAL / GREATEST(COUNT(DISTINCT season_number), 1), 1) AS avg_episodes_per_season,
        
        -- LEARNING: Quality and audience metrics
        AVG(episode_rating) AS avg_series_rating,
        STDDEV(episode_rating) AS rating_consistency,
        MIN(episode_rating) AS lowest_episode_rating,
        MAX(episode_rating) AS highest_episode_rating,
        
        -- LEARNING: Audience engagement patterns
        AVG(episode_votes) AS avg_episode_votes,
        SUM(episode_votes) AS total_series_votes,
        
        -- LEARNING: Season performance analysis
        MAX(season_number) AS final_season,
        COUNT(DISTINCT CASE WHEN episode_rating >= 8.0 THEN episode_tconst END) AS excellent_episodes,
        COUNT(DISTINCT CASE WHEN episode_rating <= 6.0 THEN episode_tconst END) AS poor_episodes
        
    FROM {{ ref('int_title_hierarchies') }}
    WHERE episode_rating IS NOT NULL  -- Focus on rated content
    GROUP BY 
        series_tconst, series_title, series_start_year, 
        series_end_year, series_duration_years
    HAVING COUNT(DISTINCT episode_tconst) >= 6  -- Meaningful series only
),

-- LEARNING: Business intelligence layer for TV industry insights
series_insights AS (
    SELECT 
        *,
        
        -- LEARNING: Series longevity classification for industry analysis
        CASE 
            WHEN series_duration_years >= 20 THEN 'Generational (20+ years)'
            WHEN series_duration_years >= 10 THEN 'Long-Running (10-19 years)'
            WHEN series_duration_years >= 5 THEN 'Established (5-9 years)'
            WHEN series_duration_years >= 3 THEN 'Multi-Season (3-4 years)'
            ELSE 'Short-Run (1-2 years)'
        END AS longevity_class,
        
        -- LEARNING: Performance tier for content strategy
        CASE 
            WHEN avg_series_rating >= 8.5 AND total_episodes >= 50 THEN 'Premium Franchise'
            WHEN avg_series_rating >= 8.0 AND total_episodes >= 20 THEN 'Quality Series'
            WHEN avg_series_rating >= 7.5 AND total_episodes >= 10 THEN 'Strong Series'
            WHEN avg_series_rating >= 7.0 THEN 'Solid Series'
            WHEN avg_series_rating >= 6.5 THEN 'Average Series'
            ELSE 'Below Average'
        END AS series_performance_tier,
        
        -- LEARNING: Consistency rating for quality management
        CASE 
            WHEN rating_consistency <= 0.5 THEN 'Very Consistent'
            WHEN rating_consistency <= 0.8 THEN 'Consistent'
            WHEN rating_consistency <= 1.2 THEN 'Moderately Variable'
            WHEN rating_consistency <= 1.5 THEN 'Variable'
            ELSE 'Highly Variable'
        END AS consistency_rating,
        
        -- LEARNING: Episode quality distribution for content analysis
        CASE 
            WHEN excellent_episodes::float / total_episodes >= 0.5 THEN 'Excellence Driven'
            WHEN excellent_episodes::float / total_episodes >= 0.3 THEN 'Quality Focused'
            WHEN poor_episodes::float / total_episodes <= 0.1 THEN 'Consistent Quality'
            WHEN poor_episodes::float / total_episodes >= 0.3 THEN 'Quality Issues'
            ELSE 'Mixed Quality'
        END AS quality_pattern,
        
        -- LEARNING: Series scale for production planning
        CASE 
            WHEN total_episodes >= 200 THEN 'Epic Scale (200+)'
            WHEN total_episodes >= 100 THEN 'Large Scale (100-199)'
            WHEN total_episodes >= 50 THEN 'Medium Scale (50-99)'
            WHEN total_episodes >= 20 THEN 'Standard Scale (20-49)'
            ELSE 'Limited Scale (6-19)'
        END AS production_scale,
        
        -- LEARNING: Audience engagement level for marketing insights
        CASE 
            WHEN avg_episode_votes >= 10000 THEN 'High Engagement'
            WHEN avg_episode_votes >= 5000 THEN 'Strong Engagement'
            WHEN avg_episode_votes >= 1000 THEN 'Moderate Engagement'
            WHEN avg_episode_votes >= 100 THEN 'Limited Engagement'
            ELSE 'Minimal Engagement'
        END AS engagement_level
        
    FROM series_metrics
)

-- LEARNING: Final output optimized for TV industry analysis
SELECT 
    series_tconst,
    series_title,
    series_start_year,
    series_end_year,
    series_duration_years,
    total_episodes,
    total_seasons,
    avg_episodes_per_season,
    avg_series_rating,
    rating_consistency,
    lowest_episode_rating,
    highest_episode_rating,
    avg_episode_votes,
    total_series_votes,
    final_season,
    excellent_episodes,
    poor_episodes,
    longevity_class,
    series_performance_tier,
    consistency_rating,
    quality_pattern,
    production_scale,
    engagement_level,
    
    -- LEARNING: Business KPIs for quick filtering
    CASE WHEN series_performance_tier IN ('Premium Franchise', 'Quality Series') THEN TRUE ELSE FALSE END AS is_premium_series,
    CASE WHEN consistency_rating IN ('Very Consistent', 'Consistent') THEN TRUE ELSE FALSE END AS is_consistent_quality,
    CASE WHEN longevity_class IN ('Generational (20+ years)', 'Long-Running (10-19 years)') THEN TRUE ELSE FALSE END AS is_long_running

FROM series_insights
ORDER BY avg_series_rating DESC, total_episodes DESC

-- BUSINESS VALUE DELIVERED:
-- 1. Series Development: Optimal episode counts and season planning insights
-- 2. Quality Management: Consistency patterns and quality control benchmarks  
-- 3. Industry Analysis: Longevity trends and production scale optimization
-- 4. Audience Insights: Engagement patterns and retention analysis
-- 5. Investment Decisions: Performance tiers and franchise potential assessment