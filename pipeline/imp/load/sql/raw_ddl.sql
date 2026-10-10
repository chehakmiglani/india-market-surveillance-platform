-- Raw schema: Silver data landed in Postgres, one table per source. dbt reads from here.
create schema if not exists raw;

create table if not exists raw.equity_daily (
    trade_date   date        not null,
    symbol       text        not null,
    isin         text,
    series       text        not null,
    company_name text,
    open         double precision not null,
    high         double precision not null,
    low          double precision not null,
    close        double precision not null,
    prev_close   double precision,
    volume       bigint      not null,
    turnover     double precision,
    trades       bigint,
    deliv_qty    bigint,
    deliv_per    double precision,
    loaded_at    timestamptz not null default now(),
    primary key (trade_date, symbol, series)
);

create table if not exists raw.fund_nav (
    snapshot_date date not null,
    scheme_code   integer not null,
    isin_growth   text,
    scheme_name   text not null,
    nav           double precision not null,
    nav_date      date not null,
    amc           text,
    category      text,
    loaded_at     timestamptz not null default now(),
    primary key (snapshot_date, scheme_code)
);

create table if not exists raw.corp_actions (
    symbol       text not null,
    isin         text,
    company_name text,
    subject      text,
    ex_date      date not null,
    action_type  text not null,
    adj_factor   double precision not null,
    loaded_at    timestamptz not null default now()
);

create table if not exists raw.portfolio_txn (
    owner      text not null,
    txn_date   date not null,
    instrument text not null,         -- NSE symbol or AMFI scheme code
    asset_type text not null,         -- 'equity' | 'mutual_fund'
    txn_type   text not null,         -- 'buy' | 'sell' | 'sip'
    quantity   double precision not null,
    price      double precision not null,
    loaded_at  timestamptz not null default now()
);

create table if not exists raw.surveillance_alerts (
    trade_date  date not null,
    symbol      text not null,
    rule        text not null,
    score       integer not null,
    reason      text not null,
    metrics     jsonb,
    created_at  timestamptz not null default now(),
    primary key (trade_date, symbol, rule)
);

create table if not exists raw.pipeline_runs (
    run_id      bigserial primary key,
    step        text not null,
    business_date date,
    status      text not null,
    rows        bigint,
    details     jsonb,
    started_at  timestamptz not null default now()
);
