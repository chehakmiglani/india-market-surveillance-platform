{#
  SCD Type 2 history of stock attributes. When a company is renamed, moves sector or changes ISIN,
  the old row is closed (dbt_valid_to set) and a new current row is opened, so facts can be joined
  to the attributes that were true on each trade date.
#}
{% snapshot snap_stock %}
{{ config(unique_key='symbol', strategy='check',
          check_cols=['isin', 'company_name', 'sector', 'industry'], invalidate_hard_deletes=False) }}
select * from {{ ref('stg_stock_master') }}
{% endsnapshot %}
