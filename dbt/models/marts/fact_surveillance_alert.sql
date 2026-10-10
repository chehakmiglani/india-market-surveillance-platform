-- Grain: one row per (trade date, symbol, rule) that fired. Written by imp.surveillance; modelled here.
select
    cast(to_char(a.trade_date, 'YYYYMMDD') as integer) as date_key,
    a.trade_date,
    a.symbol,
    s.stock_sk,
    a.rule,
    a.score,
    a.reason,
    a.metrics,
    max(a.score) over (partition by a.trade_date, a.symbol) as symbol_day_max_score
from {{ source('raw', 'surveillance_alerts') }} a
left join {{ ref('dim_stock') }} s
    on s.symbol = a.symbol and a.trade_date >= s.valid_from and a.trade_date < s.valid_to
