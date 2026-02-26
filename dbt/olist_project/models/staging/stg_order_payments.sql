with source as (
    select *
    from {{ source('olist_raw', 'order_payments_dataset') }}
),

payments as (
    select
        order_id,
        payment_sequential,
        lower(payment_type) as payment_type,
        payment_installments,
        payment_value
    from source
)

select * from payments