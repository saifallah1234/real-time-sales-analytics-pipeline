with dates as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2031-01-01' as date)"
    ) }}

)

select
    {{ date_key('date_day') }}                       as date_key,
    cast(date_day as date)                           as date_day,
    extract(year from date_day)                      as year,
    extract(quarter from date_day)                   as quarter,
    extract(month from date_day)                     as month_number,
    trim(to_char(date_day, 'Month'))                 as month_name,
    cast(to_char(date_day, 'IW') as integer)         as week_number,
    extract(day from date_day)                       as day_of_month,
    to_char(date_day, 'Dy')                          as day_name

from dates
