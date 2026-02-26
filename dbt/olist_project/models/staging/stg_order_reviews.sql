with source as (

    select *
    from {{ source('olist_raw', 'order_reviews_dataset') }}

),
reviews as (
    select
        review_id,
        order_id,
        review_score,
        coalesce(review_comment_title, 'No Comment') as review_comment_title,
        coalesce(review_comment_message, 'No Comment') as review_comment_message,
        review_creation_date::timestamp as review_creation_date,
        review_answer_timestamp::timestamp as review_answer_timestamp
    from source
)

select * from reviews