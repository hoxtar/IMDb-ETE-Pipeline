{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


SELECT
    nconst,
    primaryName AS primary_name,
    
    CAST(birthYear AS INTEGER) AS birth_year,
    CAST(deathYear AS INTEGER) AS death_year,

    SPLIT_PART(primaryProfession, ',', 1) AS first_profession,
    SPLIT_PART(primaryProfession, ',', 2) AS second_profession,
    SPLIT_PART(primaryProfession, ',', 3) AS third_profession,

    SPLIT_PART(knownForTitles, ',', 1) AS first_title_known_for,
    SPLIT_PART(knownForTitles, ',', 2) AS second_title_known_for,
    SPLIT_PART(knownForTitles, ',', 3) AS third_title_known_for,
    SPLIT_PART(knownForTitles, ',', 4) AS fourth_title_known_for

FROM {{ source('imdb', 'name_basics') }}
WHERE
    nconst IS NOT NULL
