-- Sample portfolio from the seed by default; set var use_private_portfolio=true to use raw.portfolio_txn
{% if var('use_private_portfolio') %}
select owner, txn_date, instrument, asset_type, txn_type, quantity, price
from {{ source('raw', 'portfolio_txn') }}
{% else %}
select owner, txn_date, cast(instrument as text) as instrument, asset_type, txn_type, quantity, price
from {{ ref('sample_portfolio_txn') }}
{% endif %}
