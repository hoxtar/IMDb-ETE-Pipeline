{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


WITH raw_data AS (
    SELECT
        tconst,
        string_to_array(writers, ',') AS writers_array
    FROM {{ source('imdb', 'title_crew') }}
    WHERE writers IS NOT NULL
)
SELECT
    tconst,
    TRIM(unnest(writers_array)) AS writer
FROM raw_data
