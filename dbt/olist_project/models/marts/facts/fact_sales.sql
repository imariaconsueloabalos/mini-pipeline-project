-- Step 0: Combine order items with payments and order info
with order_items as (
    select
        o.order_id,
        o.customer_id,
        i.seller_id,
        i.order_item_id,
        i.product_id,
        i.price,
        i.freight_value,
        p.payment_value,
        o.order_purchase_ts
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_order_items') }} i
        on o.order_id = i.order_id
    left join (
        -- aggregate payments per order in case there are multiple payment rows
        select
            order_id,
            sum(payment_value) as payment_value
        from {{ ref('stg_order_payments') }}
        group by order_id
    ) p
        on o.order_id = p.order_id
),

-- Step 1: Join dimension surrogate keys
fact_base as (
    select
        c.customer_key,
        s.seller_key,
        p.product_key,
        d.date_key,
        oi.order_id,
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        oi.payment_value,
        oi.order_purchase_ts
    from order_items oi
    left join {{ ref('dim_customers') }} c
        on oi.customer_id = c.customer_id
    left join {{ ref('dim_sellers') }} s
        on oi.seller_id = s.seller_id
    left join {{ ref('dim_products') }} p
        on oi.product_id = p.product_id
    left join {{ ref('dim_date') }} d
        on date_trunc('day', oi.order_purchase_ts)::date = d.full_date
),

-- Step 2: Allocate payment value per item proportionally
fact_allocated as (
    select
        *,
        case 
            when sum(price) over (partition by order_id) > 0
            then price / sum(price) over (partition by order_id) * payment_value
            else 0
        end as payment_allocated
    from fact_base
)

select *
from fact_allocated