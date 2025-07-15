{{ config(
    materialized='incremental',
    unique_key='composite_key',
    incremental_strategy='delete+insert',
    tags=['staging'],
    indexes=[
        {'columns': ['tconst']},
        {'columns': ['nconst']},
        {'columns': ['role_category']},
        {'columns': ['composite_key'], 'unique': True},
        {'columns': ['loaded_at']}
    ]
) }}

WITH source_data AS (
    SELECT 
        *,
        tconst || '_' || nconst || '_' || COALESCE(ordering::text, '__MISSING__') as composite_key
    FROM {{ source('imdb', 'title_principals') }}
    
    {% if is_incremental() %}
        -- LEARNING: File-based incremental strategy
        -- Since IMDb replaces entire files, we check if we've processed 
        -- the current file version by comparing total row counts
        
        {% set current_source_count %}
            SELECT COUNT(*) as count
            FROM {{ source('imdb', 'title_principals') }}
        {% endset %}
        
        {% if execute %}
            {% set source_rows = run_query(current_source_count).columns[0].values()[0] %}
            
            {% set existing_count %}
                SELECT COUNT(*) as count
                FROM {{ this }}
            {% endset %}
            
            {% set existing_rows = run_query(existing_count).columns[0].values()[0] %}
            
            {{ log("Source table has " ~ source_rows ~ " rows", info=true) }}
            {{ log("Target table has " ~ existing_rows ~ " rows", info=true) }}
            
            {% if source_rows == existing_rows %}
                -- Same row count = same file version, skip processing
                {{ log("Row counts match - skipping incremental load", info=true) }}
                WHERE 1=0  -- Don't process any rows
            {% else %}
                -- Different row count = new file version, process all
                {{ log("Row counts differ - processing changed data (" ~ source_rows ~ " vs " ~ existing_rows ~ ")", info=true) }}
                WHERE 1=1  -- Process all rows (delete+insert will handle replacement)
            {% endif %}
        {% else %}
            WHERE 1=1  -- Initial load - process all rows
        {% endif %}
    {% endif %}
),

cleaned_data AS (
    SELECT
        composite_key,
        tconst,
        CAST(ordering AS INTEGER) AS ordering,
        nconst,
        
        -- Role information
        LOWER(TRIM(category)) AS category,
        NULLIF(job, '\N') AS job_title,
        NULLIF(characters, '\N') AS characters,
        
        -- Standardized role categories
        CASE 
            WHEN LOWER(category) IN ('actor', 'actress') THEN 'actor'
            WHEN LOWER(category) = 'director' THEN 'director'
            WHEN LOWER(category) IN ('writer') THEN 'writer'
            WHEN LOWER(category) = 'producer' THEN 'producer'
            WHEN LOWER(category) = 'cinematographer' THEN 'cinematographer'
            WHEN LOWER(category) = 'composer' THEN 'composer'
            WHEN LOWER(category) = 'editor' THEN 'editor'
            WHEN LOWER(category) = 'self' THEN 'self'
            ELSE 'other'
        END AS role_category,
        
        CASE WHEN category IS NULL OR category = '' THEN TRUE ELSE FALSE END AS is_missing_category,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at,
        '{{ ds }}' as batch_date,
        '{{ run_id }}' as airflow_run_id
        
    FROM source_data
    WHERE tconst IS NOT NULL 
      AND nconst IS NOT NULL
      AND ordering IS NOT NULL
)

SELECT * FROM cleaned_data