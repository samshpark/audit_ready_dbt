{{
    config(
        materialized='table',
    )
}}

with days as (
    select range::date as date_day
    from range(date '2019-01-01', date '2027-01-01', interval '1 day')
)

select date_day from days
