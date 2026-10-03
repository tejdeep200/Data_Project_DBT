{{
    config(
        materialized='table',
        schema='gold'
    )
}}

SELECT
    -- Order information
    o.order_id,
    o.order_timestamp AS order_date,
    o.customer_id,
    o.store_id,

    -- Customer information
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.phone,

    -- Order item information
    oi.order_item_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,

    -- Product information
    p.product_name,
    p.category,
    p.price AS product_price,

    -- Store information
    s.store_name,
    s.city AS store_city,
    s.province AS store_state,

    -- Employee information
    CONCAT(e.first_name, ' ', e.last_name) AS employee_name,

    -- Calculated value
    (oi.quantity * oi.unit_price) AS line_total,

    -- Processing timestamp
    current_timestamp() AS processed_at

FROM {{ ref('orders_t') }} o

LEFT JOIN {{ ref('customers_t') }} c
    ON o.customer_id = c.customer_id

LEFT JOIN {{ ref('orders_items_t') }} oi
    ON o.order_id = oi.order_id

LEFT JOIN {{ ref('products_t') }} p
    ON oi.product_id = p.product_id

LEFT JOIN {{ ref('stores_t') }} s
    ON o.store_id = s.store_id

LEFT JOIN {{ ref('employees_t') }} e
    ON o.store_id = e.store_id