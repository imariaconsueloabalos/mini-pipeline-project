with source as (
    select *
    from {{ source('olist_raw', 'products_dataset') }}
),
products as (
    select
        product_id,
        product_category_name,
        product_name_lenght::int as product_name_length,
        product_description_lenght::int as product_description_length,
        product_photos_qty::int,
        product_weight_g::numeric,
        product_length_cm::numeric,
        product_height_cm::numeric,
        product_width_cm::numeric
    from source
)

select * from products