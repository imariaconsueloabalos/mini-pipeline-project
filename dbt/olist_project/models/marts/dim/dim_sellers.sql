with geo_agg as (
    select
        geolocation_zip_code_prefix as zip_code_prefix,
        round(avg(geolocation_lat)::numeric, 2) as geolocation_lat,
        round(avg(geolocation_lng)::numeric, 2) as geolocation_lng
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
),

seller_base as (
    select
        s.seller_id,
        s.seller_zip_code_prefix as zip_code,
        g.geolocation_lat,
        g.geolocation_lng
    from {{ ref('stg_sellers') }} s
    left join geo_agg g
        on s.seller_zip_code_prefix = g.zip_code_prefix
),

seller_with_key as (
    select
        'S' || lpad(row_number() over (order by seller_id)::text, 8, '0') as seller_key,
        seller_id,
        zip_code,
        geolocation_lat,
        geolocation_lng
    from seller_base
)

select *
from seller_with_key