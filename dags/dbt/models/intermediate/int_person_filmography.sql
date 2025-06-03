{{ config(materialized='view', tags=['intermediate']) }}

WITH principals AS (
    SELECT * FROM {{ ref('stg_title_principals') }}
),

directors AS (
    SELECT
        tconst,
        director AS nconst,
        'director' AS role
    FROM {{ ref('stg_title_crew_directors') }}
),

writers AS (
    SELECT
        tconst,
        writer AS nconst,
        'writer' AS role
    FROM {{ ref('stg_title_crew_writers') }}
),

-- Combine all people who worked on titles
all_contributors AS (
    -- Cast and crew from principals
    SELECT
        tconst,
        nconst,
        category AS role,
        characters
    FROM principals
    
    UNION ALL
    
    -- Directors
    SELECT
        tconst,
        nconst,
        role,
        NULL AS characters
    FROM directors
    
    UNION ALL
    
    -- Writers
    SELECT
        tconst,
        nconst,
        role,
        NULL AS characters
    FROM writers
)

-- Final combined person-title relationships
SELECT
    ac.tconst,
    ac.nconst,
    ac.role,
    ac.characters,
    b.primary_title AS title,
    b.start_year,
    b.primary_genre,
    n.primary_name AS person_name
FROM all_contributors ac
JOIN {{ ref('stg_title_basics') }} b ON ac.tconst = b.tconst
JOIN {{ ref('stg_name_basics') }} n ON ac.nconst = n.nconst
