-- Step 0: Base reviews
with review_base as (
    select
        r.review_id,
        r.order_id,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,
        r.review_creation_date,
        o.customer_id,
        i.seller_id
    from {{ ref('stg_order_reviews') }} r
    join {{ ref('stg_orders') }} o
        on r.order_id = o.order_id
    join {{ ref('stg_order_items') }} i
        on r.order_id = i.order_id
),

-- Step 1: Join dimension surrogate keys
fact_base as (
    select
        r.review_id,
        c.customer_key,
        s.seller_key,
        d.date_key,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message
    from review_base r
    left join {{ ref('dim_customers') }} c
        on r.customer_id = c.customer_id
    left join {{ ref('dim_sellers') }} s
        on r.seller_id = s.seller_id
    left join {{ ref('dim_date') }} d
        on date_trunc('day', r.review_creation_date)::date = d.full_date
)

select *
from fact_base