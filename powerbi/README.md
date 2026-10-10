# Power BI layer

The warehouse is built so the Power BI model is a direct import of the `marts` schema. A
`.pbix` can't be generated outside Power BI Desktop, so this folder has everything needed to
build it in about 30–45 minutes: the measures (`measures.dax`), the theme (`theme.json`), and
the model and page specifications below. Save it as a **PBIP project** (*File → Save as →
Power BI project*) so the model is stored as text and can be committed to git.

## 1. Connect

1. Start the stack (`make up`) and load data (`make demo` for synthetic data, or let the Airflow DAG
   backfill real data).
2. Power BI Desktop: *Get data → PostgreSQL database*. Server `localhost:5432`, database `market`,
   **Import** mode.
3. Select these tables from schema **marts**:
   `dim_date`, `dim_stock`, `dim_fund`, `fact_stock_daily`, `fact_fund_nav`, `fact_portfolio_txn`,
   `mart_portfolio_holdings`, `mart_fund_performance`, `mart_market_daily`, `fact_surveillance_alert`,
   `fact_corporate_action`,
   `mart_pipeline_health`.
4. *View → Themes → Browse for themes →* `theme.json`.

Scheduled refresh in the Power BI Service needs a gateway to your local Postgres. Desktop refresh
works without one.

## 2. Model

| From (many) | To (one) | Notes |
|---|---|---|
| fact_stock_daily[date_key] | dim_date[date_key] | |
| fact_stock_daily[stock_sk] | dim_stock[stock_sk] | SCD2: each fact points at the stock version valid that day |
| fact_fund_nav[date_key] | dim_date[date_key] | inactive, or use a role-playing copy of dim_date |
| fact_fund_nav[scheme_code] | dim_fund[scheme_code] | |
| mart_fund_performance[scheme_code] | dim_fund[scheme_code] | |
| fact_surveillance_alert[stock_sk] | dim_stock[stock_sk] | |
| fact_surveillance_alert[date_key] | dim_date[date_key] | inactive. Activate with USERELATIONSHIP when needed. |
| mart_market_daily[date_key] | dim_date[date_key] | inactive |
| fact_portfolio_txn[owner], mart_portfolio_holdings[owner] | Owners[owner] | calculated table (see end of `measures.dax`) |

- Mark `dim_date` as the date table (*Table tools → Mark as date table → `date_day`*).
- Sort `dim_date[month_name]` by `month`, and `day_name` by `day_of_week`.
- Hide keys and technical columns: `*_key`, `stock_sk`, `cum_factor`, `recorded_at`, `metrics`.
- Set data categories: `dim_stock[symbol]` is uncategorised; amounts get the format `₹ #,##0`.
- **Row-level security:** create the role **Sample only** with the filter `Owners[owner] = "sample"`.
  Test it with *Modeling → View as*. For real users, map email to owner in a `UserMap` table and use
  the `USERPRINCIPALNAME()` rule in `measures.dax`.

## 3. Measures

Create an empty table `_Measures` and paste each measure from `measures.dax`. Use the bracketed
display folders: Market, Portfolio, Mutual funds, Surveillance, Data health.

## 4. Pages

| Page | Visuals |
|---|---|
| **Market overview** | KPI cards: Turnover (Cr), Advances, Declines, A/D ratio, Latest Trade Date. Line: Avg Daily Return by date. Matrix heat-map: sector × month of `avg_return` from mart_market_daily. Bar: top 10 / bottom 10 by Period Return %. Date-range slicer at the top. |
| **Stock deep-dive** | Symbol slicer (single select). Line of `adj_close` with `close` as a dashed comparison, to show the split adjustment. Columns: `adj_volume`. Line: Avg Delivery %. Cards: Last Close, Period Return %, FYTD Return %, Volatility 20D. Table of corporate actions. Drill-through target from every other page (keep `symbol`). |
| **My portfolio** | Owner slicer (RLS-filtered). Cards: Invested, Market Value, Unrealised P&L, P&L %, Portfolio XIRR. Donut: weight by `asset_type`. Bar: P&L by instrument. Table of holdings. Line of cumulative invested amount. |
| **Mutual fund analyzer** | Scatter: Fund Volatility (x) vs Fund Return 1Y (y), bubble size = Latest NAV, legend = asset_class (3 categories max). Table: 1M/3M/6M/1Y returns, Sharpe, max drawdown. Line: NAV by date for the selected funds. Slicers: AMC, plan, category. |
| **Surveillance center** | Cards: Alerts (Latest Day), Flagged Stocks, High-Risk Alerts. Table sorted by score: date, symbol, rule, score, reason, with conditional formatting on score. Bar: alerts by rule. Line: alerts per day. Drill-through to Stock deep-dive. Footer text: "Statistical signals, not evidence of wrongdoing." |
| **Data health** | Table: mart_pipeline_health (step, status, rows, started_at). Card: Data Age (days), with red if > 3. Card: Failed Steps. |

**Design rules:**
- One accent colour per chart.
- Status colours (green/red) only for good/bad.
- No dual-axis charts.
- Slicers in a single row at the top of each page.
- Every chart gets a tooltip.
