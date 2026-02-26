-- Step 1: Aggregate geolocation per ZIP code (2 decimals)
with geo_agg as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        round(avg(geolocation_lat)::numeric, 2) as geolocation_lat,
        round(avg(geolocation_lng)::numeric, 2) as geolocation_lng
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
),

-- Step 2: Join with staged customers
customer_base as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix as zip_code,
        g.geolocation_lat,
        g.geolocation_lng
    from {{ ref('stg_customers') }} c
    left join geo_agg g
        on c.customer_zip_code_prefix = g.zip_code_prefix
),

-- Step 3: Assign formatted surrogate key
customer_with_key as (
    select
        'C' || lpad(row_number() over (order by customer_unique_id)::text, 8, '0') as customer_key,
        customer_id,
        customer_unique_id,
        zip_code,
        geolocation_lat,
        geolocation_lng
    from customer_base
)

-- Step 4: Select final columns
select *
from customer_with_key