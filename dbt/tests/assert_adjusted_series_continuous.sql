-- After split/bonus adjustment no daily move should look like a corporate action.
-- A failure usually means a split/bonus is missing from raw.corp_actions.
select trade_date, symbol, raw_return, adj_return, cum_factor
from {{ ref('fact_stock_daily') }}
where abs(adj_return) > {{ var('max_abs_adjusted_return') }}
