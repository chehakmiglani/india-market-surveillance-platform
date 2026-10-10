-- Grain: one row per scheme per NAV date
select
    cast(to_char(nav_date, 'YYYYMMDD') as integer) as date_key,
    nav_date,
    scheme_code,
    nav,
    nav / nullif(lag(nav) over (partition by scheme_code order by nav_date), 0) - 1 as daily_return
from {{ ref('stg_fund_nav') }}
