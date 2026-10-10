-- Point-to-point returns, annualised volatility, max drawdown and Sharpe ratio per scheme
with nav as (
    select scheme_code, nav_date, nav, daily_return from {{ ref('fact_fund_nav') }}
),

latest as (
    select distinct on (scheme_code) scheme_code, nav_date as as_of, nav as nav_now
    from nav order by scheme_code, nav_date desc
),

-- NAV on (or just before) a date N months back
past as (
    select l.scheme_code, m.months,
           (select n.nav from nav n
             where n.scheme_code = l.scheme_code and n.nav_date <= l.as_of - (m.months || ' months')::interval
             order by n.nav_date desc limit 1) as nav_then
    from latest l
    cross join (values (1), (3), (6), (12)) as m(months)
),

risk as (
    select
        n.scheme_code,
        stddev_samp(n.daily_return) * sqrt(252) as ann_volatility,
        avg(n.daily_return) * 252 as ann_mean_return,
        count(*) as observations
    from nav n
    join latest l using (scheme_code)
    where n.nav_date > l.as_of - interval '12 months'
    group by n.scheme_code
),

drawdown as (
    select scheme_code, min(nav / running_peak - 1) as max_drawdown
    from (
        select scheme_code, nav, max(nav) over (partition by scheme_code order by nav_date) as running_peak
        from nav
    ) x
    group by scheme_code
)

select
    l.scheme_code,
    l.as_of,
    l.nav_now,
    max(case when p.months = 1 then l.nav_now / p.nav_then - 1 end) as return_1m,
    max(case when p.months = 3 then l.nav_now / p.nav_then - 1 end) as return_3m,
    max(case when p.months = 6 then l.nav_now / p.nav_then - 1 end) as return_6m,
    max(case when p.months = 12 then l.nav_now / p.nav_then - 1 end) as return_1y,
    max(r.ann_volatility) as ann_volatility,
    max(d.max_drawdown) as max_drawdown,
    max((r.ann_mean_return - {{ var('risk_free_rate') }}) / nullif(r.ann_volatility, 0)) as sharpe_1y,
    max(r.observations) as observations_1y
from latest l
left join past p using (scheme_code)
left join risk r using (scheme_code)
left join drawdown d using (scheme_code)
group by l.scheme_code, l.as_of, l.nav_now
