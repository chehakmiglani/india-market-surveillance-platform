-- Rows dated on an exchange holiday or weekend point to a wrong date parse or a bad holiday seed
select f.trade_date, f.symbol
from {{ ref('fact_stock_daily') }} f
join {{ ref('dim_date') }} d using (date_key)
where not d.is_trading_day
