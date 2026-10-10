{#
  Transactions restated in today's share terms: quantity x (splits/bonuses after the trade date),
  price / same factor. A 2024 purchase of 30 SBIN at 610 becomes 150 shares at 122 after a 5:1 split.
#}
with factors as (
    select
        t.owner, t.txn_date, t.instrument, t.txn_type, t.quantity, t.price, t.asset_type,
        coalesce(exp(sum(ln(ca.adj_factor))), 1.0) as factor
    from {{ ref('stg_portfolio_txn') }} t
    left join {{ ref('stg_corp_actions') }} ca
        on t.asset_type = 'equity'
       and ca.symbol = t.instrument
       and ca.ex_date > t.txn_date
    group by t.owner, t.txn_date, t.instrument, t.txn_type, t.quantity, t.price, t.asset_type
)
select
    owner,
    txn_date,
    instrument,
    asset_type,
    txn_type,
    case when txn_type = 'sell' then -1 else 1 end * quantity * factor as signed_quantity,
    price / factor as adj_price,
    case when txn_type = 'sell' then -1 else 1 end * quantity * price as signed_amount
from factors
