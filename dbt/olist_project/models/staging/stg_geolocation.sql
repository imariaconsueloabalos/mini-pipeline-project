with raw_geo as (
    select
        lpad(geolocation_zip_code_prefix::text, 5, '0') as geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        initcap(geolocation_city) as geolocation_city,
        upper(geolocation_state) as geolocation_state
    from {{ source('olist_raw', 'geolocation_dataset') }}
)

select * from raw_geo