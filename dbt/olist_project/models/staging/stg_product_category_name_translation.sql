with source as (
    select *
    from {{ source('olist_raw', 'product_category_name_translation') }}
),

categories as (
    select
        coalesce(product_category_name, 'unknown') as product_category_name,
        coalesce(product_category_name_english, 'unknown') as product_category_name_english
    from source
)

select * from categories