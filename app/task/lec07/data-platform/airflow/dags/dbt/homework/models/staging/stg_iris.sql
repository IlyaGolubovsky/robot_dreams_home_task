with source as (
    select * from {{ source('analytics', 'iris_dataset') }}
)

select
    sepal_length::numeric as sepal_length,
    sepal_width::numeric as sepal_width,
    petal_length::numeric as petal_length,
    petal_width::numeric as petal_width,
    species
from source