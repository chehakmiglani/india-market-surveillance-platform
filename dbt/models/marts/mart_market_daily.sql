-- Market breadth and sector performance per day (equal-weighted across the tracked universe)
select
    f.date_key,
    f.trade_date,
    coalesce(s.sector, 'Unclassified') as sector,
    count(*) as stocks,
    sum(case when f.direction = 1 then 1 else 0 end) as advances,
    sum(case when f.direction = -1 then 1 else 0 end) as declines,
    sum(case when f.direction = 0 then 1 else 0 end) as unchanged,
    avg(f.adj_return) as avg_return,
    sum(f.turnover) as turnover,
    sum(f.volume) as volume
from {{ ref('fact_stock_daily') }} f
left join {{ ref('dim_stock') }} s on s.stock_sk = f.stock_sk
group by f.date_key, f.trade_date, coalesce(s.sector, 'Unclassified')
