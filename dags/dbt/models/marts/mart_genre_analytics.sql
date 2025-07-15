{{ config(
    materialized='table',
    tags=['marts', 'analytics'],
    indexes=[
        {'columns': ['genre']},
        {'columns': ['market_share']},
        {'columns': ['avg_rating']},
        {'columns': ['investment_priority']},
        {'columns': ['production_trend']}
    ]
) }}

-- LEARNING: Genre market intelligence for strategic content planning
-- This mart provides comprehensive genre analytics including market share,
-- quality trends, production patterns, and investment recommendations

WITH genre_data AS (
    -- Get genre data from the genres intermediate model
    SELECT 
        tc.tconst,
        tc.primary_title,
        tc.title_type,
        tc.start_year,
        tc.average_rating,
        tc.num_votes,
        twg.genre
    FROM {{ ref('int_title_complete') }} tc
    INNER JOIN {{ ref('int_title_with_genres') }} twg 
        ON tc.tconst = twg.tconst
    WHERE twg.genre IS NOT NULL
      AND tc.start_year >= 1950  -- Focus on modern era
),

genre_stats AS (
    SELECT 
        genre,
        COUNT(*) as total_titles,
        COUNT(CASE WHEN average_rating >= 7.0 THEN 1 END) as high_quality_titles,
        COUNT(CASE WHEN num_votes >= 10000 THEN 1 END) as popular_titles,
        AVG(average_rating) as avg_rating,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY average_rating) as median_rating,
        STDDEV(average_rating) as rating_stddev,
        SUM(num_votes) as total_votes,
        AVG(num_votes) as avg_votes,
        
        -- Era distribution
        COUNT(CASE WHEN start_year BETWEEN 1950 AND 1959 THEN 1 END) as titles_1950s,
        COUNT(CASE WHEN start_year BETWEEN 1960 AND 1969 THEN 1 END) as titles_1960s,
        COUNT(CASE WHEN start_year BETWEEN 1970 AND 1979 THEN 1 END) as titles_1970s,
        COUNT(CASE WHEN start_year BETWEEN 1980 AND 1989 THEN 1 END) as titles_1980s,
        COUNT(CASE WHEN start_year BETWEEN 1990 AND 1999 THEN 1 END) as titles_1990s,
        COUNT(CASE WHEN start_year BETWEEN 2000 AND 2009 THEN 1 END) as titles_2000s,
        COUNT(CASE WHEN start_year BETWEEN 2010 AND 2019 THEN 1 END) as titles_2010s,
        COUNT(CASE WHEN start_year BETWEEN 2020 AND 2029 THEN 1 END) as titles_2020s,
        
        -- Quality by era (for trend analysis)
        AVG(CASE WHEN start_year BETWEEN 1990 AND 1999 THEN average_rating END) as avg_rating_1990s,
        AVG(CASE WHEN start_year BETWEEN 2000 AND 2009 THEN average_rating END) as avg_rating_2000s,
        AVG(CASE WHEN start_year BETWEEN 2010 AND 2019 THEN average_rating END) as avg_rating_2010s,
        AVG(CASE WHEN start_year BETWEEN 2020 AND 2029 THEN average_rating END) as avg_rating_2020s,
        
        -- Content type breakdown
        COUNT(CASE WHEN title_type = 'movie' THEN 1 END) as movie_count,
        COUNT(CASE WHEN title_type = 'tvSeries' THEN 1 END) as tv_series_count,
        COUNT(CASE WHEN title_type = 'tvMovie' THEN 1 END) as tv_movie_count,
        
        -- Premium content (high rating AND high vote count)
        COUNT(CASE WHEN average_rating >= 8.0 AND num_votes >= 50000 THEN 1 END) as premium_titles,
        COUNT(CASE WHEN average_rating >= 7.5 AND num_votes >= 100000 THEN 1 END) as blockbuster_titles
        
    FROM genre_data
    GROUP BY genre
),

-- Calculate market share and totals
market_totals AS (
    SELECT 
        SUM(total_titles) as grand_total_titles,
        SUM(total_votes) as grand_total_votes
    FROM genre_stats
),

market_analysis AS (
    SELECT 
        gs.*,
        mt.grand_total_titles,
        mt.grand_total_votes,
        
        -- Calculate market_share column
        ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) as market_share,
        ROUND(gs.total_votes::numeric / mt.grand_total_votes::numeric, 4) as vote_share,
        
        -- Production trend analysis
        CASE 
            WHEN gs.titles_2020s > gs.titles_2010s THEN 'Growing'
            WHEN gs.titles_2020s < gs.titles_2010s * 0.7 THEN 'Declining'
            ELSE 'Stable'
        END as production_trend,
        
        -- Quality trend analysis
        CASE 
            WHEN gs.avg_rating_2020s > gs.avg_rating_2010s + 0.2 THEN 'Quality Improving'
            WHEN gs.avg_rating_2020s < gs.avg_rating_2010s - 0.2 THEN 'Quality Declining'
            ELSE 'Quality Stable'
        END as quality_trend,
        
        -- Market position classification
        CASE 
            WHEN ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.1 THEN 'Dominant Genre'
            WHEN ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.05 THEN 'Major Genre'
            WHEN ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.02 THEN 'Established Genre'
            ELSE 'Niche Genre'
        END as market_position,
        
        -- Investment priority scoring
        CASE 
            WHEN gs.avg_rating >= 7.0 AND ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.05 
                 AND gs.titles_2020s >= gs.titles_2010s THEN 'High Priority'
            WHEN gs.avg_rating >= 6.5 AND ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.02 THEN 'Medium Priority'
            WHEN gs.avg_rating >= 6.0 THEN 'Low Priority'
            ELSE 'Not Recommended'
        END as investment_priority,
        
        -- Content strategy recommendation
        CASE 
            WHEN gs.premium_titles >= 10 AND gs.avg_rating >= 7.5 THEN 'Premium Content Focus'
            WHEN gs.blockbuster_titles >= 5 AND gs.avg_votes >= 50000 THEN 'Blockbuster Strategy'
            WHEN gs.high_quality_titles >= 100 THEN 'Quality-First Approach'
            WHEN ROUND(gs.total_titles::numeric / mt.grand_total_titles::numeric, 4) >= 0.05 THEN 'Volume Strategy'
            ELSE 'Niche Targeting'
        END as content_strategy,
        
        -- Platform fit analysis
        CASE 
            WHEN gs.tv_series_count > gs.movie_count THEN 'Streaming Platform'
            WHEN gs.blockbuster_titles >= 3 THEN 'Theater Release'
            WHEN gs.avg_votes < 10000 THEN 'Art House / Festival'
            ELSE 'Multi-Platform'
        END as platform_fit
        
    FROM genre_stats gs
    CROSS JOIN market_totals mt
)

SELECT 
    genre,
    
    -- Core metrics
    total_titles,
    market_share,
    vote_share,
    avg_rating,
    median_rating,
    rating_stddev,
    total_votes,
    avg_votes,
    high_quality_titles,
    popular_titles,

    -- Era distribution for historical analysis
    titles_1950s,
    titles_1960s,
    titles_1970s,
    titles_1980s,
    titles_1990s,
    titles_2000s,
    titles_2010s,
    titles_2020s,

    -- Quality evolution tracking
    avg_rating_1990s,
    avg_rating_2000s,
    avg_rating_2010s,
    avg_rating_2020s,

    -- Content type breakdown
    movie_count,
    tv_series_count,
    tv_movie_count,

    -- Premium content metrics
    premium_titles,
    blockbuster_titles,
    
    -- Strategic classifications
    production_trend,
    quality_trend,
    market_position,
    investment_priority,
    content_strategy,
    platform_fit,

    -- LEARNING: Business KPIs for quick decision-making
    CASE WHEN investment_priority IN ('High Priority', 'Medium Priority') THEN TRUE ELSE FALSE END AS is_investment_target,
    CASE WHEN production_trend = 'Growing' AND quality_trend IN ('Quality Improving', 'Quality Stable') THEN TRUE ELSE FALSE END AS is_growth_opportunity,
    CASE WHEN market_position IN ('Dominant Genre', 'Major Genre') THEN TRUE ELSE FALSE END AS is_core_market

FROM market_analysis
ORDER BY market_share DESC, avg_rating DESC

-- BUSINESS VALUE DELIVERED:
-- 1. Portfolio Strategy: Market share analysis and investment prioritization
-- 2. Trend Intelligence: Production and quality trends for strategic planning  
-- 3. Competitive Analysis: Market positioning and audience share insights
-- 4. Platform Strategy: Content type distribution for platform optimization
-- 5. Risk Assessment: Growth opportunities vs declining markets identification