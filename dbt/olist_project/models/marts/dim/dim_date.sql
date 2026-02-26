-- Step 1: Extract unique dates from orders and reviews
with unique_dates as (
    select distinct date_trunc('day', order_purchase_ts)::date as full_date
    from {{ ref('stg_orders') }}

    union

    select distinct date_trunc('day', review_creation_date)::date as full_date
    from {{ ref('stg_order_reviews') }}
),

-- Step 2: Add standard date attributes
date_attrs as (
    select
        full_date,
        extract(year from full_date)::int as year,
        extract(month from full_date)::int as month,
        extract(day from full_date)::int as day,
        extract(dow from full_date)::int as weekday, -- 0=Sunday, 6=Saturday
        extract(quarter from full_date)::int as quarter,
        to_char(full_date, 'YYYY-MM-DD') as date_str
    from unique_dates
    order by full_date
),

-- Step 3: Add formatted surrogate key
date_with_key as (
    select
        'D' || lpad(row_number() over (order by full_date)::text, 8, '0') as date_key,
        *
    from date_attrs
)

-- Step 4: Final select
select *
from date_with_key