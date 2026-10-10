-- All corporate actions (dividends included) for the stock deep-dive page
select
    cast(to_char(ex_date, 'YYYYMMDD') as integer) as date_key,
    ex_date,
    symbol,
    action_type,
    adj_factor,
    subject
from {{ source('raw', 'corp_actions') }}
