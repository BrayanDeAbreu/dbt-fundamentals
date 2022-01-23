with

orders as (
    select * from {{ ref('int_orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}

),

---------------------

customer_orders as (

    select 

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        -- customer level aggregation
        min(orders.order_date) over (
            partition by orders.customer_id
        )  as costumer_first_order_date,

        min(orders.valid_order_date) over (
            partition by orders.customer_id
        ) as costumer_first_non_returned_order_date,

        max(orders.valid_order_date) over (
            partition by orders.customer_id
        ) as costumer_most_recent_non_returned_order_date,

        count(*) over (
            partition by orders.customer_id
        ) as costumer_order_count,

        sum(nvl2(orders.valid_order_date,1,0)) over (
            partition by orders.customer_id
        ) as costumer_non_returned_order_count,

        sum(
            nvl2(orders.valid_order_date,
            orders.order_value_dollars,
            0)
            ) over (
            partition by orders.customer_id
        ) as costumer_total_lifetime_value,

        array_agg(distinct orders.order_id) over (
            partition by orders.customer_id
        ) as costumer_order_ids

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id

),

add_avg_orders_values as (

    select

        *,
        costumer_total_lifetime_value / costumer_non_returned_order_count as costumer_avg_non_returned_order_value

    from customer_orders 

),

-- Final CTEs
final as (
    select 

        order_id,
        customer_id,
        surname,
        givenname,
        costumer_first_order_date as first_order_date,
        costumer_order_count as order_count,
        costumer_total_lifetime_value as total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status

    from add_avg_orders_values
)

-- Simple select statement
select * from final

