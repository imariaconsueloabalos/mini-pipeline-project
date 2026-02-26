with source as (

    select *
    from {{ source('olist_raw', 'customers_dataset') }}

),

cleaned as (

    select
        customer_id,
        customer_unique_id,
        lpad(customer_zip_code_prefix::text, 5, '0') as customer_zip_code_prefix, -- to keep the zipcodes that starts with zero
        initcap(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state

    from source

)

select * from cleaned