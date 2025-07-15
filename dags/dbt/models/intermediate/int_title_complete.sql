{{ config(
    materialized='view',
    tags=['intermediate']
) }}

-- LEARNING NOTE: Materialized as view for intermediate layer best practices
-- Views provide: fast development iteration, real-time data freshness, minimal storage
-- Performance optimized through indexed staging tables that this view leverages

WITH title_basics AS (
    SELECT * FROM {{ ref('stg_title_basics') }}
),

title_ratings AS (
    SELECT * FROM {{ ref('stg_title_ratings') }}
),

enhanced_titles AS (
    SELECT
        -- Core identifiers
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
        
        -- Rating information
        r.average_rating,
        r.num_votes,
        
        -- BUSINESS LOGIC: Calculated dimensions for analytics
        
        -- Era categorization (dashboard-friendly)
        CASE 
            WHEN b.start_year >= 2020 THEN '2020s'
            WHEN b.start_year >= 2010 THEN '2010s'
            WHEN b.start_year >= 2000 THEN '2000s'
            WHEN b.start_year >= 1990 THEN '1990s'
            WHEN b.start_year >= 1980 THEN '1980s'
            WHEN b.start_year >= 1970 THEN '1970s'
            WHEN b.start_year >= 1960 THEN '1960s'
            ELSE 'Old Classic'
        END AS decade,
        
         -- Quality classification based on rating and vote count distribution
        -- Logic derived from percentile analysis: 
        -- P50 (median) = 26, P60 = 40, P70 = 71, P80 = 149
        -- This allows us to reward high-quality titles with few votes as "Hidden Gems"
        CASE 
            WHEN r.average_rating >= 8.5 AND r.num_votes < 26 THEN 'Hidden Gem'
            WHEN r.average_rating >= 8.5 AND r.num_votes >= 26 THEN 'Acclaimed'
            WHEN r.average_rating >= 7.0 THEN 'Solid'
            WHEN r.average_rating >= 6.0 THEN 'Watchable'
            WHEN r.average_rating < 6.0 THEN 'Weak'
            ELSE 'Low rated'
        END AS quality_tier,
        
        -- Popularity segmentation based on percentiles of num_votes:
        -- P10 = 7, P20 = 10, P30 = 13, P40 = 18, P50 = 26, P60 = 40, P70 = 71, P80 = 149, P90 = 478
        -- This tier helps distinguish ultra-niche content from widely known titles
        CASE
            WHEN r.num_votes IS NULL THEN 'Unknown'
            WHEN r.num_votes <= 7 THEN 'Bottom 10%'      -- Ultra niche
            WHEN r.num_votes <= 10 THEN 'Bottom 20%'
            WHEN r.num_votes <= 13 THEN 'Bottom 30%'
            WHEN r.num_votes <= 18 THEN 'Bottom 40%'
            WHEN r.num_votes <= 26 THEN 'Bottom 50%'     -- Median = 26
            WHEN r.num_votes <= 40 THEN 'Top 50–40%'
            WHEN r.num_votes <= 71 THEN 'Top 40–30%'
            WHEN r.num_votes <= 149 THEN 'Top 30–20%'
            WHEN r.num_votes <= 478 THEN 'Top 20–10%'
            ELSE 'Top 10%'                                -- > P90
        END AS popularity_tier,
        
        -- Content categorization
        CASE 
            WHEN b.runtime_minutes IS NULL THEN 'Unknown'
            WHEN b.runtime_minutes < 30 THEN 'Short'
            WHEN b.runtime_minutes < 90 THEN 'Standard'
            WHEN b.runtime_minutes < 180 THEN 'Long'
            ELSE 'Epic'
        END AS runtime_category,
        
        -- Commercial potential indicators
        CASE 
            WHEN b.primary_genre IN ('Action', 'Adventure', 'Sci-Fi', 'Fantasy') THEN TRUE
            ELSE FALSE
        END AS is_blockbuster_genre,
        
        -- Data quality metrics
        CASE WHEN r.average_rating IS NOT NULL THEN TRUE ELSE FALSE END AS has_ratings,

        -- Voting volume flags based on distribution
        CASE WHEN r.num_votes >= 500 THEN TRUE ELSE FALSE END AS has_reliable_votes,  -- P85+
        CASE WHEN r.num_votes >= 1000 THEN TRUE ELSE FALSE END AS has_high_votes,     -- P95+
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM title_basics b
    LEFT JOIN title_ratings r ON b.tconst = r.tconst
    WHERE b.tconst IS NOT NULL
)

SELECT *
FROM enhanced_titles