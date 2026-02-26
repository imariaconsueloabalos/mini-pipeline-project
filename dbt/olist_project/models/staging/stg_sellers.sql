with source as (
    select *
    from {{ source('olist_raw', 'sellers_dataset') }}
),
sellers as (
    select
        seller_id,
        lpad(seller_zip_code_prefix::text, 5, '0') as seller_zip_code_prefix,
        initcap(seller_city) as seller_city,
        upper(trim(seller_state)) as seller_state
    from source
)

select * from sellers