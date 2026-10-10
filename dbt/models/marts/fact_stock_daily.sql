-- Grain: one row per symbol per trading day, joined to the stock version valid on that date (SCD2).
select
    cast(to_char(a.trade_date, 'YYYYMMDD') as integer) as date_key,
    a.trade_date,
    a.symbol,
    s.stock_sk,
    a.open, a.high, a.low, a.close, a.prev_close,
    a.volume, a.turnover, a.trades,
    a.deliv_qty, a.deliv_per,
    a.cum_factor,
    a.adj_open, a.adj_high, a.adj_low, a.adj_close, a.adj_volume,
    a.raw_return,
    a.adj_return,
    case when a.adj_return > 0 then 1 when a.adj_return < 0 then -1 else 0 end as direction
from {{ ref('int_equity_adjusted') }} a
left join {{ ref('dim_stock') }} s
    on s.symbol = a.symbol
   and a.trade_date >= s.valid_from
   and a.trade_date < s.valid_to
