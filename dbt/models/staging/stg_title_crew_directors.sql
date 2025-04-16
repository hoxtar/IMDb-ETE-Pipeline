{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


WITH raw_data AS (
    SELECT
        tconst,
        string_to_array(directors, ',') AS director_array
    FROM {{ source('imdb', 'title_crew') }}
    WHERE directors IS NOT NULL
)
SELECT
    tconst,
    TRIM(unnest(director_array)) AS director
FROM raw_data
