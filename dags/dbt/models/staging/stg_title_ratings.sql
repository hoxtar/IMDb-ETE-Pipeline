{{ config(
    materialized = 'table',
    tags = ['staging'],
    indexes=[
        {'columns': ['tconst'], 'unique': True},
        {'columns': ['average_rating']},
        {'columns': ['num_votes']},
        {'columns': ['average_rating', 'num_votes']}
    ]
) }}
--Indexes:
    -- tconst: Primary key + joins
    -- average_rating: Ranking queries
    -- num_votes: Popularity sorting
    -- average_rating, num_votes: Combined sorting


WITH source_data AS (
    SELECT * FROM {{ source('imdb', 'title_ratings') }}
),

cleaned_data AS (
    SELECT
        -- Primary key
        tconst,

        -- Ratings
        CASE 
            WHEN averageRating::TEXT = '\N' THEN NULL
            ELSE CAST(averageRating AS DECIMAL(3,1))
        END AS average_rating,
        
        CASE 
            WHEN numVotes::TEXT = '\N' THEN NULL
            ELSE CAST(numVotes AS INTEGER)
        END AS num_votes,

        -- Data quality indicators
        CASE WHEN averageRating::TEXT = '\N' THEN TRUE ELSE FALSE END AS is_missing_rating,
        CASE WHEN numVotes::TEXT = '\N' THEN TRUE ELSE FALSE END AS is_missing_votes,
        
        -- Business logic flags (minimal in staging)
        CASE 
            WHEN CAST(numVotes AS INTEGER) >= 1000 THEN TRUE 
            ELSE FALSE 
        END AS has_significant_votes,
        
        -- Metadata
        CURRENT_TIMESTAMP AS loaded_at
        
    FROM source_data
    WHERE tconst IS NOT NULL
)

SELECT *
FROM cleaned_data