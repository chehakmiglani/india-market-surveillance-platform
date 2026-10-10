-- Grain: one row per portfolio transaction (quantities restated for later splits/bonuses)
select
    md5(owner || '|' || txn_date || '|' || instrument || '|' || txn_type || '|' || signed_quantity) as txn_id,
    cast(to_char(txn_date, 'YYYYMMDD') as integer) as date_key,
    owner,
    txn_date,
    instrument,
    asset_type,
    txn_type,
    signed_quantity,
    adj_price,
    signed_amount,
    -signed_amount as cash_flow   -- money out is negative (XIRR convention)
from {{ ref('int_portfolio_positions') }}
