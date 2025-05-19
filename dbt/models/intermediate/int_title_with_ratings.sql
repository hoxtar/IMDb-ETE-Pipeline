{{ config(materialized='view', tags=['intermediate']) }}

with basics as (
    select * from {{ ref('stg_title_basics') }}
),

ratings as (
    select * from {{ ref('stg_title_ratings') }}
)

select
    b.tconst,
    b.title_type,
    b.primary_title,
    b.runtime_minutes,
    b.primary_genre,
    b.secondary_genre,
    b.third_genre,
    b.is_adult,
    b.start_year,
    r.average_rating,
    r.num_votes
from basics b
left join ratings r
    on b.tconst = r.tconst
