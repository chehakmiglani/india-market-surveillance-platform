# Data dictionary (schema `marts`)

| Table | Grain | Key columns | What it's for |
|---|---|---|---|
| `dim_date` | one row per calendar day, 2023–2027 | `date_key` (YYYYMMDD) | `is_trading_day` (NSE holidays seed), Indian `fiscal_year` (Apr–Mar), `fiscal_quarter` |
| `dim_stock` | one row per version of a stock's attributes (SCD2) | `stock_sk` | `symbol`, `isin`, `company_name`, `sector`, `industry`, `valid_from`, `valid_to`, `is_current` |
| `dim_fund` | one row per AMFI scheme | `scheme_code` | `amc`, `category`, `asset_class`, `plan` (Direct/Regular), `option_type` |
| `fact_stock_daily` | symbol × trading day | `date_key`, `stock_sk` | Raw OHLCV, turnover, trades, delivery qty/%, `cum_factor`, split/bonus-adjusted `adj_*`, `raw_return`, `adj_return`, `direction` |
| `fact_fund_nav` | scheme × NAV date | `date_key`, `scheme_code` | `nav`, `daily_return` |
| `fact_portfolio_txn` | one transaction | `txn_id` | `signed_quantity` and `adj_price` restated for later splits/bonuses; `cash_flow` (invested = negative) |
| `fact_corporate_action` | one corporate action | `date_key` | `action_type` (split/bonus/other), `adj_factor`, `subject` |
| `fact_surveillance_alert` | trade date × symbol × rule | `date_key`, `stock_sk` | `score` 0–100, `reason`, `metrics` (JSON), `symbol_day_max_score` |
| `mart_portfolio_holdings` | owner × instrument | `owner`, `instrument` | Quantity, average cost, last price, invested, market value, unrealised P&L (₹ and %), weight |
| `mart_fund_performance` | one row per scheme | `scheme_code` | 1M/3M/6M/1Y returns, annualised volatility, max drawdown, Sharpe (risk-free rate from the `risk_free_rate` var) |
| `mart_market_daily` | trading day × sector | `date_key`, `sector` | Advances, declines, unchanged, equal-weighted average return, turnover, volume |
| `mart_pipeline_health` | latest run per pipeline step | `step` | Status, rows, start time, latest loaded trade date |

## Surveillance rules (`imp/surveillance/rules.py`)

All baselines use the **previous** 20 trading days, so today's value is never part of its own average.

| Rule | Fires when | Score |
|---|---|---|
| `volume_spike` | volume ≥ 5× its 20-day average, or log-volume z ≥ 3 | 50–100 by ratio |
| `price_shock` | \|return\| > 3σ **and** return minus the market return > 2σ | 50–100 by σ |
| `pump` | 5 straight up days, +15% over 5 days, 5-day volume ≥ 2× average, delivery down ≥ 10 pts (when available) | 60–100 |
| `dump_after_pump` | −15% over 3 days within 5 days after a pump | 90 |
| `delivery_anomaly` | delivery % below its mean − 2σ on ≥ 2× volume | 60 |
| `circuit_streak` | 2nd straight close at a 5/10/20% band, at the day's high (up) or low (down) | 70 |
| `gap` | open more than 5% away from the previous close | 40–100 by gap size |

These are statistical signals for research, not evidence of manipulation.
