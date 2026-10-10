-- Data-health page: latest run per step, freshness of the core fact
select
    r.step,
    r.business_date,
    r.status,
    r.rows,
    r.started_at,
    (select max(trade_date) from {{ ref('fact_stock_daily') }}) as latest_trade_date
from (
    select *, row_number() over (partition by step order by started_at desc) as rn
    from {{ source('raw', 'pipeline_runs') }}
) r
where r.rn = 1
