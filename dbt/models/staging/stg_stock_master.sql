-- Current attributes per symbol: latest ISIN and latest non-null company name from the bhavcopy,
-- sector/industry from the reference seed. Feeds the SCD2 snapshot (snap_stock).
with latest as (
    select
        symbol,
        isin,
        company_name,
        row_number() over (partition by symbol order by trade_date desc) as rn_any,
        row_number() over (partition by symbol order by (company_name is null), trade_date desc) as rn_named
    from {{ ref('stg_equity_daily') }}
)
select
    l.symbol,
    max(case when l.rn_any = 1 then l.isin end) as isin,
    max(case when l.rn_named = 1 then l.company_name end) as company_name,
    coalesce(max(m.sector), 'Unclassified') as sector,
    coalesce(max(m.industry), 'Unclassified') as industry
from latest l
left join {{ ref('stock_master') }} m on m.symbol = l.symbol
where l.rn_any = 1 or l.rn_named = 1
group by l.symbol
