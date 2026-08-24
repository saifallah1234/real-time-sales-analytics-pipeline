with dates as (

    select
        dateadd(
            day,
            row_number() over (order by seq4()) - 1,
            '2020-01-01'::date
        ) as date_day

    from table(generator(rowcount => 4018))

)

select
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
    date_day,
    year(date_day)                          as year,
    quarter(date_day)                       as quarter,
    month(date_day)                         as month_number,
    trim(to_char(date_day, 'MMMM'))         as month_name,
    weekiso(date_day)                       as week_number,
    day(date_day)                           as day_of_month,
    trim(to_char(date_day, 'DY'))           as day_name

from dates