-- Equity series only (BE = trade-for-trade segment is excluded from analytics)
select
    trade_date,
    symbol,
    isin,
    company_name,
    open,
    high,
    low,
    close,
    prev_close,
    volume,
    turnover,
    trades,
    deliv_qty,
    deliv_per,
    case when prev_close > 0 then close / prev_close - 1 end as raw_return
from {{ source('raw', 'equity_daily') }}
where series = 'EQ'
