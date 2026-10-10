{#
  Split/bonus-adjusted prices. For each trade date the cumulative factor is the product of
  every adjustment factor with an ex-date AFTER that date:  adj_price = price / factor,
  adj_volume = volume * factor. exp(sum(ln(f))) is the SQL way to multiply within a group.
  History is recomputed when a new corporate action arrives, which is why the downstream
  fact is a full table rather than an incremental model.
#}
with factors as (
    select
        e.symbol,
        e.trade_date,
        coalesce(exp(sum(ln(ca.adj_factor))), 1.0) as cum_factor
    from {{ ref('stg_equity_daily') }} e
    left join {{ ref('stg_corp_actions') }} ca
        on ca.symbol = e.symbol
       and ca.ex_date > e.trade_date
    group by e.symbol, e.trade_date
),

adjusted as (
    select
        e.*,
        f.cum_factor,
        round((e.open / f.cum_factor)::numeric, 4)::double precision as adj_open,
        round((e.high / f.cum_factor)::numeric, 4)::double precision as adj_high,
        round((e.low / f.cum_factor)::numeric, 4)::double precision as adj_low,
        round((e.close / f.cum_factor)::numeric, 4)::double precision as adj_close,
        e.volume * f.cum_factor as adj_volume
    from {{ ref('stg_equity_daily') }} e
    join factors f using (symbol, trade_date)
)

select
    *,
    adj_close / nullif(lag(adj_close) over (partition by symbol order by trade_date), 0) - 1 as adj_return
from adjusted
