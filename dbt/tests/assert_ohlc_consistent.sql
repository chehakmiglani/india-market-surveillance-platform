-- Every bar must satisfy low <= open, close <= high and positive prices
select trade_date, symbol, open, high, low, close
from {{ ref('fact_stock_daily') }}
where low <= 0
   or high < low
   or open > high or open < low
   or close > high or close < low
