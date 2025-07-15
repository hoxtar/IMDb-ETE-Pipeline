{{ config(
    materialized = 'table',
    tags = ['marts'],
    indexes=[
        {'columns': ['nconst'], 'unique': True},
        {'columns': ['career_stage']},
        {'columns': ['dominant_role']},
        {'columns': ['productivity_tier']}
    ]
) }}

-- LEARNING OBJECTIVE: Advanced person career analytics for decision-making
-- This mart demonstrates proper aggregation patterns and business intelligence metrics
-- Showcases: statistical analysis, career classification, and business-ready dimensions

-- BUSINESS PURPOSE: Enable talent analysis, career insights, and industry benchmarking
-- Key Use Cases: 
-- 1. Talent acquisition and representation decisions
-- 2. Career trajectory analysis and mentorship
-- 3. Industry productivity and quality benchmarking
-- 4. Cross-role collaboration pattern analysis

WITH career_aggregates AS (
    -- LEARNING: Mart-level aggregation from intermediate relationships
    -- Heavy lifting happens here, not in intermediate layer
    SELECT
        nconst,
        person_name,
        
        -- LEARNING: Career timeline metrics
        MIN(start_year) AS career_start_year,
        MAX(start_year) AS latest_work_year,
        MAX(start_year) - MIN(start_year) AS career_span_years,
        
        -- LEARNING: Productivity analytics
        COUNT(DISTINCT tconst) AS total_credits,
        COUNT(DISTINCT start_year) AS active_years,
        ROUND(COUNT(DISTINCT tconst)::DECIMAL / GREATEST(COUNT(DISTINCT start_year), 1), 2) AS avg_credits_per_year,
        
        -- LEARNING: Role distribution analysis
        COUNT(DISTINCT CASE WHEN role = 'actor' THEN tconst END) AS actor_credits,
        COUNT(DISTINCT CASE WHEN role = 'director' THEN tconst END) AS director_credits,
        COUNT(DISTINCT CASE WHEN role = 'writer' THEN tconst END) AS writer_credits,
        COUNT(DISTINCT CASE WHEN role = 'producer' THEN tconst END) AS producer_credits,
        
        -- LEARNING: Quality and impact metrics
        AVG(CASE WHEN average_rating IS NOT NULL THEN average_rating END) AS avg_project_rating,
        MAX(average_rating) AS highest_rated_work,
        SUM(num_votes) AS total_audience_reach,
        
        -- LEARNING: Era analysis for trend insights
        COUNT(DISTINCT CASE WHEN work_decade = '2020s' THEN tconst END) AS works_2020s,
        COUNT(DISTINCT CASE WHEN work_decade = '2010s' THEN tconst END) AS works_2010s,
        COUNT(DISTINCT CASE WHEN work_decade = '2000s' THEN tconst END) AS works_2000s,
        COUNT(DISTINCT CASE WHEN work_decade = '1990s' THEN tconst END) AS works_1990s,
        
        -- LEARNING: Quality distribution analysis
        COUNT(DISTINCT CASE WHEN work_quality_tier = 'High Quality' THEN tconst END) AS high_quality_works,
        COUNT(DISTINCT CASE WHEN work_quality_tier = 'Good Quality' THEN tconst END) AS good_quality_works
        
    FROM {{ ref('int_person_filmography') }}
    GROUP BY nconst, person_name
    HAVING COUNT(DISTINCT tconst) >= 3  -- Focus on meaningful careers
),

-- LEARNING: Business classification layer for dashboard consumption
career_insights AS (
    SELECT
        *,
        
        -- LEARNING: Career stage classification (dashboard-ready dimension)
        CASE 
            WHEN career_span_years >= 40 THEN 'Veteran (40+ years)'
            WHEN career_span_years >= 20 THEN 'Established (20-39 years)'
            WHEN career_span_years >= 10 THEN 'Experienced (10-19 years)'
            WHEN career_span_years >= 5 THEN 'Mid-Career (5-9 years)'
            ELSE 'Emerging (0-4 years)'
        END AS career_stage,
        
        -- LEARNING: Productivity classification for benchmarking
        CASE 
            WHEN total_credits >= 100 THEN 'Highly Prolific (100+)'
            WHEN total_credits >= 50 THEN 'Very Active (50-99)'
            WHEN total_credits >= 20 THEN 'Active (20-49)'
            WHEN total_credits >= 10 THEN 'Moderate (10-19)'
            ELSE 'Limited Activity (3-9)'
        END AS productivity_tier,
        
        -- LEARNING: Primary role identification for specialization analysis
        CASE 
            WHEN actor_credits >= director_credits AND actor_credits >= writer_credits AND actor_credits >= producer_credits THEN 'Actor'
            WHEN director_credits >= writer_credits AND director_credits >= producer_credits THEN 'Director'
            WHEN writer_credits >= producer_credits THEN 'Writer'
            WHEN producer_credits > 0 THEN 'Producer'
            ELSE 'Other'
        END AS dominant_role,
        
        -- LEARNING: Multi-talent indicator for versatility analysis
        CASE 
            WHEN (CASE WHEN actor_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN director_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN writer_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN producer_credits > 0 THEN 1 ELSE 0 END) >= 3 
            THEN 'Multi-Talented' 
            WHEN (CASE WHEN actor_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN director_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN writer_credits > 0 THEN 1 ELSE 0 END +
                  CASE WHEN producer_credits > 0 THEN 1 ELSE 0 END) = 2 
            THEN 'Dual-Role' 
            ELSE 'Specialist'
        END AS versatility_level,
        
        -- LEARNING: Quality reputation indicator
        CASE 
            WHEN avg_project_rating >= 7.5 AND high_quality_works >= 5 THEN 'Quality Specialist'
            WHEN avg_project_rating >= 7.0 AND total_credits >= 20 THEN 'Consistent Quality'
            WHEN avg_project_rating >= 6.5 THEN 'Good Track Record'
            WHEN avg_project_rating IS NOT NULL THEN 'Mixed Results'
            ELSE 'Unrated Work'
        END AS quality_reputation,
        
        -- LEARNING: Era activity pattern for trend analysis
        CASE 
            WHEN works_2020s >= works_2010s AND works_2020s >= works_2000s THEN 'Current Era Active'
            WHEN works_2010s >= works_2000s AND works_2010s >= works_1990s THEN 'Peak 2010s'
            WHEN works_2000s >= works_1990s THEN 'Peak 2000s'
            WHEN works_1990s > 0 THEN 'Legacy Era'
            ELSE 'Varied Activity'
        END AS activity_pattern
        
    FROM career_aggregates
)

-- LEARNING: Final output optimized for business intelligence consumption
-- All metrics are dashboard-ready with clear business context
SELECT 
    nconst,
    person_name,
    career_start_year,
    latest_work_year,
    career_span_years,
    total_credits,
    active_years,
    avg_credits_per_year,
    actor_credits,
    director_credits,
    writer_credits,
    producer_credits,
    avg_project_rating,
    highest_rated_work,
    total_audience_reach,
    works_2020s,
    works_2010s,
    works_2000s,
    works_1990s,
    high_quality_works,
    good_quality_works,
    career_stage,
    productivity_tier,
    dominant_role,
    versatility_level,
    quality_reputation,
    activity_pattern

FROM career_insights
ORDER BY total_credits DESC, avg_project_rating DESC

-- BUSINESS VALUE DELIVERED:
-- 1. Talent Analysis: Identify high-performing professionals by productivity and quality
-- 2. Career Benchmarking: Compare productivity tiers and career progression patterns
-- 3. Industry Insights: Track activity patterns across decades and quality trends
-- 4. Collaboration Planning: Find versatile professionals for complex projects
-- 5. Trend Analysis: Understand how career patterns evolve across generations