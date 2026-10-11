
-- 1. Inspect all tables in the database.
SHOW TABLES IN ecommerce_data_platform;

-- 2. Inspect the refined orders schema.
DESCRIBE ecommerce_data_platform.refined_orders;

-- 3. Inspect the refined customers schema.
DESCRIBE ecommerce_data_platform.refined_customers;

-- 4. Inspect the refined products schema.
DESCRIBE ecommerce_data_platform.refined_products;

-- 5. Inspect the refined payments schema.
DESCRIBE ecommerce_data_platform.refined_payments;

-- 6. Inspect the refined events schema.
DESCRIBE ecommerce_data_platform.refined_events;

-- 7. Verify actual order statuses.
SELECT
    order_status,
    COUNT(*) AS record_count
FROM ecommerce_data_platform.refined_orders
GROUP BY order_status
ORDER BY record_count DESC;

-- 8. Verify order data-quality statuses.
SELECT
    order_data_quality_status,
    COUNT(*) AS record_count
FROM ecommerce_data_platform.refined_orders
GROUP BY order_data_quality_status
ORDER BY record_count DESC;

-- 9. Verify payment outcomes.
SELECT
    payment_result,
    COUNT(*) AS record_count
FROM ecommerce_data_platform.refined_payments
GROUP BY payment_result
ORDER BY record_count DESC;

-- 10. Check for duplicate order IDs.
SELECT
    order_id,
    COUNT(*) AS row_count
FROM ecommerce_data_platform.refined_orders
WHERE order_id IS NOT NULL
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC
LIMIT 100;

-- 11. Check for duplicate payment IDs.
SELECT
    payment_id,
    COUNT(*) AS row_count
FROM ecommerce_data_platform.refined_payments
WHERE payment_id IS NOT NULL
GROUP BY payment_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC
LIMIT 100;

-- 12. Check for duplicate event IDs.
SELECT
    event_id,
    COUNT(*) AS row_count
FROM ecommerce_data_platform.refined_events
WHERE event_id IS NOT NULL
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC
LIMIT 100;

--Daily sales and revenue

SELECT
    CAST(order_date AS DATE) AS order_date,

    COUNT(DISTINCT order_id) AS all_distinct_orders,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(order_status)) = 'COMPLETED'
        THEN order_id
    END) AS completed_orders,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(order_status)) = 'COMPLETED'
         AND UPPER(TRIM(order_data_quality_status)) = 'VALID'
         AND order_amount_valid = TRUE
        THEN order_id
    END) AS eligible_completed_orders,

    SUM(CASE
        WHEN UPPER(TRIM(order_status)) = 'COMPLETED'
         AND UPPER(TRIM(order_data_quality_status)) = 'VALID'
         AND order_amount_valid = TRUE
        THEN order_amount
        ELSE 0
    END) AS eligible_revenue,

    SUM(CASE
        WHEN UPPER(TRIM(order_status)) = 'COMPLETED'
         AND UPPER(TRIM(order_data_quality_status)) = 'VALID'
         AND order_amount_valid = TRUE
        THEN quantity
        ELSE 0
    END) AS eligible_units_sold,

    SUM(CASE
        WHEN UPPER(TRIM(order_status)) = 'COMPLETED'
         AND (
             UPPER(TRIM(order_data_quality_status)) <> 'VALID'
             OR order_data_quality_status IS NULL
             OR order_amount_valid = FALSE
             OR order_amount_valid IS NULL
         )
        THEN 1
        ELSE 0
    END) AS completed_rows_excluded,

    SUM(CASE
        WHEN order_amount_valid = FALSE
        THEN 1
        ELSE 0
    END) AS amount_mismatch_rows

FROM ecommerce_data_platform.refined_orders
GROUP BY CAST(order_date AS DATE)
ORDER BY order_date;

--Top products by revenue

SELECT
    o.product_id,
    p.product_name,
    p.category,
    p.brand,

    SUM(o.order_amount) AS eligible_revenue,
    SUM(o.quantity) AS units_sold,
    COUNT(DISTINCT o.order_id) AS completed_orders

FROM ecommerce_data_platform.refined_orders o

LEFT JOIN ecommerce_data_platform.refined_products p
    ON o.product_id = p.product_id

WHERE UPPER(TRIM(o.order_status)) = 'COMPLETED'
  AND UPPER(TRIM(o.order_data_quality_status)) = 'VALID'
  AND o.order_amount_valid = TRUE

GROUP BY
    o.product_id,
    p.product_name,
    p.category,
    p.brand

ORDER BY eligible_revenue DESC
LIMIT 10;

--Payment performance

SELECT
    CAST(payment_date AS DATE) AS payment_date,
    payment_method,

    COUNT(DISTINCT payment_id) AS payment_attempts,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(payment_result)) = 'SUCCESS'
        THEN payment_id
    END) AS successful_payments,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(payment_result)) = 'FAILED'
        THEN payment_id
    END) AS failed_payments,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(payment_result)) = 'PENDING'
        THEN payment_id
    END) AS pending_payments,

    COUNT(DISTINCT CASE
        WHEN UPPER(TRIM(payment_result))
             IN ('SUCCESS', 'FAILED', 'REFUNDED')
        THEN payment_id
    END) AS known_final_outcomes,

    SUM(CASE
        WHEN UPPER(TRIM(payment_result)) = 'SUCCESS'
         AND UPPER(TRIM(payment_data_quality_status)) = 'VALID'
        THEN payment_amount
        ELSE 0
    END) AS valid_success_payment_amount

FROM ecommerce_data_platform.refined_payments

GROUP BY
    CAST(payment_date AS DATE),
    payment_method

ORDER BY payment_date, payment_method;

--Customer performance

WITH eligible_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_amount,
        quantity
    FROM ecommerce_data_platform.refined_orders
    WHERE UPPER(TRIM(order_status)) = 'COMPLETED'
      AND UPPER(TRIM(order_data_quality_status)) = 'VALID'
      AND order_amount_valid = TRUE
      AND order_id IS NOT NULL
),
customer_sales AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS completed_orders,
        SUM(order_amount) AS eligible_revenue,
        SUM(quantity) AS units_sold,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM eligible_orders
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.name,
    c.city,
    c.state,
    c.country,
    c.signup_date,

    COALESCE(s.completed_orders, 0) AS completed_orders,
    COALESCE(s.eligible_revenue, 0) AS eligible_revenue,
    COALESCE(s.units_sold, 0) AS units_sold,
    s.first_order_date,
    s.last_order_date,

    CASE
        WHEN COALESCE(s.completed_orders, 0) = 0
            THEN 'NO_ELIGIBLE_PURCHASE'
        WHEN s.completed_orders = 1
            THEN 'ONE_ELIGIBLE_PURCHASE'
        ELSE 'REPEAT_CUSTOMER'
    END AS customer_behavior_segment

FROM ecommerce_data_platform.refined_customers c

LEFT JOIN customer_sales s
    ON c.customer_id = s.customer_id;

--Event activity

SELECT
    CAST(event_date AS DATE) AS event_date,
    event_type,
    event_category,
    source,

    COUNT(DISTINCT event_id) AS event_count,
    COUNT(DISTINCT customer_id) AS identified_customers,
    COUNT(DISTINCT order_id) AS identified_orders,
    COUNT(DISTINCT product_id) AS identified_products,

    SUM(CASE
        WHEN UPPER(TRIM(event_data_quality_status)) <> 'VALID'
          OR event_data_quality_status IS NULL
        THEN 1
        ELSE 0
    END) AS events_needing_review

FROM ecommerce_data_platform.refined_events

GROUP BY
    CAST(event_date AS DATE),
    event_type,
    event_category,
    source

ORDER BY event_date, event_count DESC;

