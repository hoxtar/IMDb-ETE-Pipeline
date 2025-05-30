{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


SELECT
    tconst,
    titletype AS title_type,

    CASE
        WHEN primarytitle LIKE 'Episode #%.%' THEN REGEXP_REPLACE(primarytitle, 'Episode #(\d+)\.(\d+)', 'S\1.E\2')
        ELSE primarytitle
    END AS primary_title,

    originaltitle AS original_title,
    
    CASE isAdult
        WHEN '0' THEN FALSE
        WHEN '1' THEN TRUE
        ELSE NULL
    END AS is_adult,

    CAST(startyear AS INTEGER) AS start_year,
    CAST(endyear AS INTEGER) AS end_year,
    CAST(runtimeminutes AS INTEGER) AS runtime_minutes,

    SPLIT_PART(genres, ',', 1) AS primary_genre,
    SPLIT_PART(genres, ',', 2) AS secondary_genre,
    SPLIT_PART(genres, ',', 3) AS third_genre

FROM {{ source('imdb', 'title_basics') }}
WHERE
    primarytitle IS NOT NULL
    AND startyear IS NOT NULL
    AND genres IS NOT NULL
