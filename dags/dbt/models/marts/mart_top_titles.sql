{{ config(
    materialized = 'table',
    tags = ['marts'],
    indexes=[
        {'columns': ['tconst'], 'unique': True},
        {'columns': ['performance_tier']},
        {'columns': ['popularity_segment']},
        {'columns': ['title_type', 'decade']}
    ]
) }}

-- LEARNING OBJECTIVE: Title performance analytics for content strategy and discovery
-- This mart demonstrates advanced ranking algorithms and market segmentation
-- Showcases: multi-dimensional scoring, statistical analysis, business intelligence

-- BUSINESS PURPOSE: Enable content performance analysis and recommendation systems
-- Key Use Cases:
-- 1. Content discovery and recommendation engines
-- 2. Performance benchmarking across genres and eras
-- 3. Investment and content acquisition decisions
-- 4. Market analysis and competitive intelligence

WITH title_performance AS (
    -- LEARNING: Advanced performance scoring combining multiple signals
    -- Not just ratings - includes popularity, longevity, and engagement
    SELECT 
        tc.*,
        
        -- LEARNING: Weighted scoring algorithm for realistic rankings
        -- Combines rating quality with audience reach for balanced scoring
        CASE 
            WHEN tc.num_votes >= 100000 THEN tc.average_rating * 1.0  -- High-confidence ratings
            WHEN tc.num_votes >= 10000 THEN tc.average_rating * 0.95   -- Moderate confidence
            WHEN tc.num_votes >= 1000 THEN tc.average_rating * 0.9     -- Lower confidence
            ELSE tc.average_rating * 0.8                              -- Low confidence
        END AS weighted_score,
        
        -- LEARNING: Popularity segmentation based on statistical distribution
        -- Uses percentiles to create meaningful segments across all content
        PERCENT_RANK() OVER (ORDER BY tc.num_votes) AS popularity_percentile,
        PERCENT_RANK() OVER (ORDER BY tc.average_rating) AS quality_percentile,
        
        -- LEARNING: Title type specific rankings for fair comparison
        -- Compare movies to movies, series to series, etc.
        ROW_NUMBER() OVER (
            PARTITION BY tc.title_type 
            ORDER BY 
                CASE 
                    WHEN tc.num_votes >= 100000 THEN tc.average_rating * 1.0
                    WHEN tc.num_votes >= 10000 THEN tc.average_rating * 0.95
                    WHEN tc.num_votes >= 1000 THEN tc.average_rating * 0.9
                    ELSE tc.average_rating * 0.8
                END DESC, 
                tc.num_votes DESC
        ) AS type_rank,
        
        -- LEARNING: Genre-specific rankings for recommendation systems
        ROW_NUMBER() OVER (
            PARTITION BY tc.primary_genre 
            ORDER BY 
                CASE 
                    WHEN tc.num_votes >= 100000 THEN tc.average_rating * 1.0
                    WHEN tc.num_votes >= 10000 THEN tc.average_rating * 0.95
                    WHEN tc.num_votes >= 1000 THEN tc.average_rating * 0.9
                    ELSE tc.average_rating * 0.8
                END DESC, 
                tc.num_votes DESC
        ) AS genre_rank,
        
        -- LEARNING: Era-adjusted rankings for historical context
        ROW_NUMBER() OVER (
            PARTITION BY tc.decade 
            ORDER BY 
                CASE 
                    WHEN tc.num_votes >= 100000 THEN tc.average_rating * 1.0
                    WHEN tc.num_votes >= 10000 THEN tc.average_rating * 0.95
                    WHEN tc.num_votes >= 1000 THEN tc.average_rating * 0.9
                    ELSE tc.average_rating * 0.8
                END DESC, 
                tc.num_votes DESC
        ) AS decade_rank
        
    FROM {{ ref('int_title_complete') }} tc
    WHERE tc.average_rating IS NOT NULL 
      AND tc.num_votes >= 50  -- Minimum threshold for statistical relevance
),

-- LEARNING: Business classification layer for strategic decision-making
performance_segments AS (
    SELECT 
        *,
        
        -- LEARNING: Performance tier based on dual criteria (quality + popularity)
        CASE 
            WHEN quality_percentile >= 0.95 AND popularity_percentile >= 0.9 THEN 'Masterpiece'
            WHEN quality_percentile >= 0.9 AND popularity_percentile >= 0.7 THEN 'Blockbuster Hit'
            WHEN quality_percentile >= 0.85 AND popularity_percentile >= 0.5 THEN 'Critical Success'
            WHEN quality_percentile >= 0.6 AND popularity_percentile >= 0.85 THEN 'Popular Hit'
            WHEN quality_percentile >= 0.7 THEN 'Quality Content'
            WHEN popularity_percentile >= 0.7 THEN 'Mainstream Appeal'
            WHEN quality_percentile >= 0.5 OR popularity_percentile >= 0.5 THEN 'Above Average'
            ELSE 'Standard Content'
        END AS performance_tier,
        
        -- LEARNING: Market positioning for content strategy
        CASE 
            WHEN popularity_percentile >= 0.9 THEN 'Mass Market'
            WHEN popularity_percentile >= 0.7 THEN 'Broad Appeal'
            WHEN popularity_percentile >= 0.4 THEN 'Moderate Reach'
            WHEN popularity_percentile >= 0.2 THEN 'Niche Appeal'
            ELSE 'Limited Audience'
        END AS popularity_segment,
        
        -- LEARNING: Investment category for business decisions
        CASE 
            WHEN weighted_score >= 8.5 AND num_votes >= 50000 THEN 'Premium Investment'
            WHEN weighted_score >= 8.0 AND num_votes >= 10000 THEN 'Strong Investment'
            WHEN weighted_score >= 7.5 OR num_votes >= 100000 THEN 'Solid Investment'
            WHEN weighted_score >= 7.0 OR num_votes >= 10000 THEN 'Moderate Investment'
            ELSE 'High Risk Investment'
        END AS investment_category,
        
        -- LEARNING: Discovery potential for recommendation engines
        CASE 
            WHEN quality_percentile >= 0.8 AND popularity_percentile <= 0.3 THEN 'Hidden Gem'
            WHEN quality_percentile >= 0.7 AND popularity_percentile <= 0.5 THEN 'Underrated'
            WHEN quality_percentile <= 0.3 AND popularity_percentile >= 0.7 THEN 'Overrated'
            WHEN quality_percentile >= 0.6 AND popularity_percentile >= 0.6 THEN 'Well-Deserved Success'
            ELSE 'Standard Discovery'
        END AS discovery_category,
        
        -- LEARNING: Longevity indicator for evergreen content
        CASE 
            WHEN start_year <= 2000 AND popularity_percentile >= 0.6 THEN 'Timeless Classic'
            WHEN start_year <= 2010 AND quality_percentile >= 0.8 THEN 'Enduring Quality'
            WHEN start_year >= 2020 AND popularity_percentile >= 0.7 THEN 'Recent Hit'
            WHEN start_year >= 2015 AND quality_percentile >= 0.8 THEN 'Modern Classic'
            ELSE 'Standard Longevity'
        END AS longevity_indicator
        
    FROM title_performance
)

-- LEARNING: Final mart optimized for business intelligence and content strategy
SELECT 
    tconst,
    title_type,
    primary_title,
    start_year,
    decade,
    runtime_minutes,
    primary_genre,
    secondary_genre,
    average_rating,
    num_votes,
    weighted_score,
    quality_percentile,
    popularity_percentile,
    type_rank,
    genre_rank,
    decade_rank,
    performance_tier,
    popularity_segment,
    investment_category,
    discovery_category,
    longevity_indicator,
    is_adult,
    
    -- LEARNING: Ready-to-use business flags for filtering
    CASE WHEN type_rank <= 100 THEN TRUE ELSE FALSE END AS is_top_100_in_type,
    CASE WHEN genre_rank <= 50 THEN TRUE ELSE FALSE END AS is_top_50_in_genre,
    CASE WHEN decade_rank <= 25 THEN TRUE ELSE FALSE END AS is_top_25_in_decade,
    CASE WHEN performance_tier IN ('Masterpiece', 'Blockbuster Hit', 'Critical Success') THEN TRUE ELSE FALSE END AS is_premium_content

FROM performance_segments
ORDER BY weighted_score DESC, num_votes DESC

-- BUSINESS VALUE DELIVERED:
-- 1. Content Discovery: Advanced recommendation algorithm with multi-dimensional scoring
-- 2. Performance Analysis: Statistical benchmarking across genres, types, and eras
-- 3. Investment Insights: Risk-adjusted content evaluation for acquisition decisions
-- 4. Market Intelligence: Popularity trends and audience engagement patterns
-- 5. Strategic Planning: Content portfolio optimization and gap analysis