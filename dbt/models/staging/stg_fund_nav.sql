-- AMFI snapshots can repeat the same NAV date (holidays); keep the latest snapshot per scheme and NAV date
with ranked as (
    select
        scheme_code,
        isin_growth,
        scheme_name,
        amc,
        category,
        nav,
        nav_date,
        row_number() over (partition by scheme_code, nav_date order by snapshot_date desc) as rn
    from {{ source('raw', 'fund_nav') }}
)
select scheme_code, isin_growth, scheme_name, amc, category, nav, nav_date
from ranked
where rn = 1
