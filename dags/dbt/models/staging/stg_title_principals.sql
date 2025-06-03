{{ config(
    materialized = 'view',
    tags = ['staging']
) }}


SELECT
    tconst,
    CAST(ordering AS INTEGER) AS ordering,
    nconst,
    category,  
    job,
    characters

FROM {{ source('imdb', 'title_principals') }}
WHERE
    tconst IS NOT NULL AND
    ordering IS NOT NULL AND
    nconst IS NOT NULL
    

