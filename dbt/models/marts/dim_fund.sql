-- Mutual fund schemes with plan/option/asset class parsed from AMFI names and categories
with latest as (
    select *, row_number() over (partition by scheme_code order by nav_date desc) as rn
    from {{ ref('stg_fund_nav') }}
)
select
    scheme_code,
    scheme_name,
    isin_growth,
    amc,
    regexp_replace(category, '^.*\((.*)\)$', '\1') as category,
    case
        when category ilike '%equity scheme%' then 'Equity'
        when category ilike '%debt scheme%' then 'Debt'
        when category ilike '%hybrid scheme%' then 'Hybrid'
        when category ilike '%solution%' then 'Solution Oriented'
        else 'Other'
    end as asset_class,
    case when scheme_name ilike '%direct%' then 'Direct' else 'Regular' end as plan,
    case when scheme_name ilike '%idcw%' or scheme_name ilike '%dividend%' then 'IDCW' else 'Growth' end as option_type
from latest
where rn = 1
