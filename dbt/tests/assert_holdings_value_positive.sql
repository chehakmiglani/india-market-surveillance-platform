-- Open positions must have a positive quantity and a market value
select owner, instrument, quantity, market_value
from {{ ref('mart_portfolio_holdings') }}
where quantity <= 0 or market_value is null or market_value <= 0
