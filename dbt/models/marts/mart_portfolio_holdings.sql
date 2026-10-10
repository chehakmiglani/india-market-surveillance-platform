-- Current holdings per owner: quantity, cost, market value, unrealised P&L and weight
with positions as (
    select
        owner,
        instrument,
        asset_type,
        sum(signed_quantity) as quantity,
        sum(case when signed_quantity > 0 then signed_quantity * adj_price end)
            / nullif(sum(case when signed_quantity > 0 then signed_quantity end), 0) as avg_cost
    from {{ ref('int_portfolio_positions') }}
    group by owner, instrument, asset_type
),

last_equity as (
    select distinct on (symbol) symbol as instrument, close as last_price, trade_date as price_date
    from {{ ref('stg_equity_daily') }}
    order by symbol, trade_date desc
),

last_nav as (
    select distinct on (scheme_code) cast(scheme_code as text) as instrument, nav as last_price, nav_date as price_date
    from {{ ref('stg_fund_nav') }}
    order by scheme_code, nav_date desc
),

valued as (
    select
        p.*,
        coalesce(e.last_price, n.last_price) as last_price,
        coalesce(e.price_date, n.price_date) as price_date,
        p.quantity * p.avg_cost as invested,
        p.quantity * coalesce(e.last_price, n.last_price) as market_value
    from positions p
    left join last_equity e on p.asset_type = 'equity' and e.instrument = p.instrument
    left join last_nav n on p.asset_type = 'mutual_fund' and n.instrument = p.instrument
    where p.quantity > 0.000001
)

select
    owner,
    instrument,
    asset_type,
    quantity,
    avg_cost,
    last_price,
    price_date,
    invested,
    market_value,
    market_value - invested as unrealised_pnl,
    (market_value - invested) / nullif(invested, 0) as unrealised_pnl_pct,
    market_value / nullif(sum(market_value) over (partition by owner), 0) as weight
from valued
