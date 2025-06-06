{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


SELECT
    tconst,
    CAST(averageRating AS INTEGER) AS average_Rating,
    CAST(numVotes AS INTEGER) AS num_Votes


FROM {{ source('imdb', 'title_ratings') }}
WHERE
    tconst IS NOT NULL