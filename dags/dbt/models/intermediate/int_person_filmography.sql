{{ config(
    materialized='view',
    tags=['intermediate']
) }}

-- LEARNING OBJECTIVE: Business logic layer for person-title relationships
-- This model showcases proper intermediate layer design: business logic without aggregation
-- Demonstrates: relationship building, data enrichment, business rule application

-- ARCHITECTURAL DECISION: Changed to view from table
-- Intermediate layer should focus on business logic and relationships, not heavy aggregation
-- Heavy analytics belong in marts layer for specific business questions

WITH filmography_base AS (
    -- LEARNING: Core business logic - person-title relationship enrichment
    -- Join person data with their film/TV work, enriching with ratings
    SELECT 
        nb.nconst,
        nb.primary_name AS person_name,
        tp.tconst,
        tp.role_category AS role,
        tb.start_year,
        tb.title_type,
        tb.primary_title,
        tr.average_rating,
        tr.num_votes,
        
        -- LEARNING: Business logic - career timeline classification
        -- Add business context to raw relationships
        CASE 
            WHEN tb.start_year >= 2020 THEN '2020s'
            WHEN tb.start_year >= 2010 THEN '2010s'
            WHEN tb.start_year >= 2000 THEN '2000s'
            WHEN tb.start_year >= 1990 THEN '1990s'
            ELSE 'Legacy'
        END AS work_decade,
        
        -- LEARNING: Quality context for each work
        CASE 
            WHEN tr.average_rating >= 8.0 THEN 'High Quality'
            WHEN tr.average_rating >= 7.0 THEN 'Good Quality'
            WHEN tr.average_rating >= 6.0 THEN 'Average Quality'
            WHEN tr.average_rating IS NOT NULL THEN 'Below Average'
            ELSE 'Unrated'
        END AS work_quality_tier
        
    FROM {{ ref('stg_name_basics') }} nb
    INNER JOIN {{ ref('stg_title_principals') }} tp ON nb.nconst = tp.nconst
    INNER JOIN {{ ref('stg_title_basics') }} tb ON tp.tconst = tb.tconst
    LEFT JOIN {{ ref('stg_title_ratings') }} tr ON tb.tconst = tr.tconst
    WHERE tb.start_year IS NOT NULL
)

-- LEARNING: Clean output for marts consumption
-- Intermediate layer provides enriched relationships, not aggregated metrics
SELECT 
    nconst,
    person_name,
    tconst,
    primary_title,
    role,
    start_year,
    title_type,
    average_rating,
    num_votes,
    work_decade,
    work_quality_tier
FROM filmography_base

-- LEARNING: Why no complex aggregations here?
-- Intermediate layer focuses on business logic and relationships
-- Heavy aggregations and specific analytics belong in mart layer
-- This separation allows multiple marts to use this foundation differently