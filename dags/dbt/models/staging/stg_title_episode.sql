{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


SELECT
    tconst,
    parentTconst AS parent_tconst,

    CAST(seasonNumber AS INTEGER) AS season_number,
    CAST(episodeNumber AS INTEGER) AS episode_number

FROM {{ source('imdb', 'title_episode') }}
WHERE
    seasonNumber IS NOT NULL
    AND episodeNumber IS NOT NULL

