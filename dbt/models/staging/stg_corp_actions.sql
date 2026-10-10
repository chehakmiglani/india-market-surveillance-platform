-- Only actions that change the share count (splits, bonuses) affect price history
select distinct
    symbol,
    ex_date,
    action_type,
    adj_factor,
    subject
from {{ source('raw', 'corp_actions') }}
where action_type in ('split', 'bonus')
  and adj_factor > 0
  and adj_factor <> 1
