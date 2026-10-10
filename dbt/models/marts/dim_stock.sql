-- SCD Type 2 stock dimension built from the snapshot. One row per version of a stock's attributes.
-- The snapshot only starts recording when the pipeline first runs, so each symbol's FIRST version is
-- back-dated to 1900-01-01: it describes all history before the first observed change.
select
    dbt_scd_id as stock_sk,
    symbol,
    isin,
    company_name,
    sector,
    industry,
    case when row_number() over (partition by symbol order by dbt_valid_from) = 1
         then date '1900-01-01' else dbt_valid_from::date end as valid_from,
    coalesce(dbt_valid_to::date, date '9999-12-31') as valid_to,
    dbt_valid_to is null as is_current,
    dbt_valid_from as recorded_at
from {{ ref('snap_stock') }}
