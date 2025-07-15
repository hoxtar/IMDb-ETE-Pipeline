{{ config(
    materialized='view',
    tags=['intermediate']
) }}

-- LEARNING NOTE: No indexes needed for views as they don't store data
-- Views inherit performance characteristics from underlying staging tables  
-- This materialization choice optimized for: fast execution, real-time freshness, minimal storage


-- LEARNING OBJECTIVE: Data normalization patterns for analytical flexibility
-- Demonstrates converting denormalized arrays into normalized relational structure
-- This is a foundational pattern in data engineering for flexible analytics and BI tools

-- BUSINESS CONTEXT: IMDb stores up to 3 genres per title in a denormalized format
-- (e.g., primary_genre="Action", secondary_genre="Adventure", third_genre="Sci-Fi")
-- For analytics, we need each genre as a separate row to enable:
-- 1. Flexible filtering in BI tools (WHERE genre = 'Action')
-- 2. Multi-genre analysis (COUNT titles per genre)
-- 3. Genre ranking analysis (primary vs secondary genre popularity)
-- 4. Cross-genre analysis (titles spanning multiple genres)

-- LEARNING: Why materialized as 'view' instead of 'table'?
-- Views are appropriate for lightweight transformations like this normalization because:
-- - Fast transformation (simple UNION ALL operations)
-- - Leverages existing staging table indexes for performance
-- - Maintains data freshness automatically without additional storage
-- - Avoids data duplication while providing structural transformation

-- PERFORMANCE CONSIDERATION: This view will be fast because:
-- 1. Source staging table (stg_title_basics) is already indexed on tconst
-- 2. UNION ALL is more efficient than UNION (no duplicate removal needed)
-- 3. Simple filtering operations with NULL checks are highly optimized
-- 4. No complex joins or aggregations that would slow execution

WITH title_basics AS (
    SELECT * FROM {{ ref('stg_title_basics') }}
),

-- LEARNING: Data normalization using UNION ALL pattern
-- This is the standard SQL approach for "unpivoting" or flattening denormalized columns
-- Each UNION ALL represents one genre position, preserving hierarchical importance
genres AS (
    -- Primary genre normalization (most important genre, rank 1)
    -- LEARNING: Primary genres represent the main thematic classification
    -- Business rule: Every title should have at least a primary genre
    SELECT
        tconst,                      -- Title identifier for joining to other models
        primary_genre AS genre,      -- Genre name normalized from denormalized structure
        1 AS genre_rank             -- Rank preserves genre importance for analytics
    FROM title_basics
    WHERE primary_genre IS NOT NULL AND primary_genre != ''  -- Data quality: exclude missing genres
    
    UNION ALL
    
    -- Secondary genre normalization (second most important, rank 2)
    -- LEARNING: Secondary genres add nuance and cross-genre appeal analysis
    -- Business insight: Titles with secondary genres often have broader audience appeal
    SELECT
        tconst,
        secondary_genre AS genre,
        2 AS genre_rank             -- Rank 2 indicates secondary importance
    FROM title_basics
    WHERE secondary_genre IS NOT NULL AND secondary_genre != ''
    
    UNION ALL
    
    -- Tertiary genre normalization (third most important, rank 3)
    -- LEARNING: Tertiary genres capture additional thematic elements
    -- Analytics value: Useful for identifying niche combinations and detailed categorization
    SELECT
        tconst,
        third_genre AS genre,
        3 AS genre_rank             -- Rank 3 indicates tertiary importance
    FROM title_basics
    WHERE third_genre IS NOT NULL AND third_genre != ''
)

-- LEARNING: Final normalized output enables sophisticated genre analysis patterns:
-- 1. Genre popularity rankings: SELECT genre, COUNT(*) FROM this_model GROUP BY genre
-- 2. Multi-genre titles: SELECT tconst FROM this_model GROUP BY tconst HAVING COUNT(*) > 1
-- 3. Genre combinations: Common secondary genres for each primary genre
-- 4. Genre evolution: Trends in genre usage over decades when joined with title years
-- 5. Cross-genre performance: Average ratings by genre combination

-- ANALYTICS ENABLEMENT: This normalized structure supports dashboard features like:
-- - Genre filter dropdowns (SELECT DISTINCT genre ORDER BY genre)
-- - Multi-select genre filtering (WHERE tconst IN (SELECT tconst WHERE genre IN (...)))
-- - Genre hierarchy analysis (GROUP BY genre_rank to see primary vs secondary preferences)
-- - Genre diversity metrics (COUNT(DISTINCT genre) per title, decade, etc.)

-- PERFORMANCE NOTE: Using view materialization because:
-- - Transformation is computationally light (UNION ALL + filtering)
-- - Source staging tables are already indexed and optimized
-- - Real-time data freshness without storage overhead
-- - Query performance remains excellent due to staging layer optimization

-- DATA ENGINEERING PATTERN DEMONSTRATED:
-- This model exemplifies the "normalization in intermediate layer" pattern where:
-- 1. Staging preserves source structure (denormalized for loading efficiency)
-- 2. Intermediate normalizes for analytics flexibility (this model)
-- 3. Marts aggregate normalized data for specific business questions

SELECT
    tconst,        -- Title identifier for joining to other intermediate/mart models
    genre,         -- Individual genre name (normalized from denormalized structure)
    genre_rank     -- Importance ranking (1=primary, 2=secondary, 3=tertiary)
FROM genres

-- LEARNING: Why no ORDER BY in the intermediate model?
-- We let consuming models (marts) handle sorting based on their specific analytical needs
-- This keeps the intermediate model focused on data structure transformation, not presentation
-- Different marts may want different sorting (by tconst, by genre, by rank, etc.)
-- This separation of concerns is a key data engineering best practice

-- DOWNSTREAM USAGE: This model serves as the foundation for:
-- - mart_genre_analytics: Genre performance metrics and trends
-- - Any dashboard requiring genre-based filtering or analysis
-- - Cross-genre analysis when joined with int_title_complete
-- - Multi-dimensional analysis combining genre with ratings, decades, etc.

-- BUSINESS VALUE DELIVERED:
-- 1. Enables flexible genre-based analytics in BI tools
-- 2. Supports complex multi-genre filtering and analysis
-- 3. Preserves genre hierarchy for nuanced business insights
-- 4. Provides foundation for genre trend analysis and recommendations
-- 5. Enables cross-genre performance benchmarking and optimization