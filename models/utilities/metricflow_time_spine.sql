{{
    config(
        materialized='table',
    )
}}

with days as (
    select range::date as date_day
    from range(
        date '2019-01-01',
        date_trunc('year', current_date) + interval '3 years',
        interval '1 day'
    )
)

select date_day from days
