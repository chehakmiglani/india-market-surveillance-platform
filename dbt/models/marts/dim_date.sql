-- Calendar with NSE trading-day flags and Indian fiscal year (April-March; FY2026 = Apr 2025 - Mar 2026)
with days as (
    select d::date as date_day
    from generate_series(date '2023-01-01', date '2027-12-31', interval '1 day') as g(d)
)
select
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
    date_day,
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Mon') as month_name,
    to_char(date_day, 'YYYY-MM') as year_month,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'Dy') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend,
    h.holiday_date is not null as is_holiday,
    h.occasion as holiday_name,
    extract(isodow from date_day) not in (6, 7) and h.holiday_date is null as is_trading_day,
    case when extract(month from date_day) >= 4 then extract(year from date_day)::int + 1
         else extract(year from date_day)::int end as fiscal_year,
    ((extract(month from date_day)::int + 8) % 12) / 3 + 1 as fiscal_quarter
from days
left join {{ ref('nse_holidays') }} h on h.holiday_date = days.date_day
