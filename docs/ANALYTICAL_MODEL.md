
# E-Commerce Curated Analytical Model

## 1. Purpose

The curated layer converts refined e-commerce datasets into a
business-ready analytical model for Athena SQL queries and Streamlit
dashboards.

The model contains:
- Dimensions for descriptive analysis.
- Facts that preserve source-level detail.
- Integrated reporting tables for related customer, product, order,
  and event information.
- Aggregate tables for dashboard performance.

## 2. S3 Output Structure

```text
curated/
├── dimensions/
│   ├── dim_customers/
│   ├── dim_products/
│   └── dim_date/
├── facts/
│   ├── fact_orders/
│   ├── fact_payments/
│   └── fact_events/
├── integrated/
│   ├── integrated_orders/
│   └── integrated_customer_activity/
└── aggregates/
    ├── agg_sales_daily/
    ├── agg_product_performance/
    ├── agg_customer_performance/
    ├── agg_payment_performance/
    └── agg_event_activity_daily/
```

## 3. Table Grain and Keys

| Table | Grain | Expected key |
|---|---|---|
| dim_customers | One row per customer | customer_id |
| dim_products | One row per product | product_id |
| dim_date | One row per calendar date | calendar_date |
| fact_orders | One row per source order | order_id |
| fact_payments | One row per payment attempt | payment_id |
| fact_events | One row per source event | event_id |
| integrated_orders | One row per source order | order_id |
| integrated_customer_activity | One row per customer | customer_id |
| agg_sales_daily | One row per order date | order_date |
| agg_product_performance | One row per product | product_id |
| agg_customer_performance | One row per customer | customer_id |
| agg_payment_performance | One row per payment date and method | payment_date, payment_method |
| agg_event_activity_daily | One row per date, event type, category and source | composite grouping |

These grains are intended contracts. The job must validate uniqueness
against actual source data before publishing the model.

## 4. Relationships

### Orders to customers

Join:

```sql
orders.customer_id = dim_customers.customer_id
```

Use a LEFT JOIN so orders with missing customer records remain visible.
Expose a customer match status.

### Orders to products

Join:

```sql
orders.product_id = dim_products.product_id
```

Use a LEFT JOIN so orders with missing product records remain visible.
Expose a product match status.

### Dates

Use the following date relationships:
- customers.signup_date -> dim_date.calendar_date
- orders.order_date -> dim_date.calendar_date
- payments.payment_date -> dim_date.calendar_date
- events.event_date -> dim_date.calendar_date

### Events to orders and customers

Events contain customer_id, order_id and product_id.
Keep individual events at event grain. Aggregate events by order or
customer before joining to an order-level or customer-level table.

Never join raw events directly to orders for revenue reporting:
one order can have multiple events, which would multiply its revenue.

### Payments to orders

The current refined payments schema does not include order_id or
customer_id. Do not join payments to orders using payment amount,
timestamp, or transaction_reference unless a reliable relationship
is established and validated.

Payment reporting must remain independent until then.

## 5. Business Rules

### Revenue eligibility

An order contributes to eligible revenue only when all conditions hold:
- order_status is COMPLETED.
- order_data_quality_status is VALID.
- order_amount_valid is TRUE.
- order_id is non-null and nonblank.

Eligible revenue is the sum of order_amount for eligible orders.
Orders excluded from eligible revenue remain in detailed facts and
should be reported separately.

The exact status values must be verified from actual refined data.

### Completed orders

Count distinct order_id values that meet the agreed completed-order
status rule. For the eligible completed-order KPI, apply the revenue
eligibility rules as well.

### Average order value

AOV = eligible revenue / eligible completed orders.

Return NULL when the denominator is zero.

### Units sold

Sum quantity only for eligible completed orders.

### Product performance

Report eligible revenue, eligible units sold, eligible completed orders,
category, brand and current inventory information.

Inventory exposure may be estimated as current product price multiplied
by current stock quantity. Label this as an estimate, not historical
inventory valuation.

### Customer performance

Report eligible completed orders, eligible revenue, units sold,
first eligible order date, last eligible order date and repeat-purchase
segment.

A repeat customer has at least two distinct eligible completed orders.

Do not confuse customer tenure with purchasing behavior.

### Payment performance

Report payment attempts by date and method, success, failure, pending,
refund outcomes and data-quality exceptions.

Payment success rate:
successful attempts / attempts with a known final outcome.

The numerator and denominator must use the same payment-attempt grain.

Do not label successful payment amounts as order revenue while the
payment-to-order relationship remains unverified.

### Event activity

Report event counts by date, event type, category and source.
Include distinct identified customers, orders and products.

A conversion funnel requires verified event semantics, identity rules
and event sequence ordering. Do not infer a funnel solely from event
counts.

## 6. Dashboard Outputs

1. KPI cards: eligible revenue, eligible completed orders and AOV.
2. Daily/monthly revenue trend.
3. Order status and data-quality exclusions.
4. Top products by eligible revenue.
5. Category revenue and current inventory.
6. Customer geographic and signup analysis.
7. Repeat customers and customer revenue.
8. Payment outcomes by method.
9. Event activity by date, type and source.
10. Order-related event activity.

## 7. Data Quality and Reconciliation

Before publication, validate:
- Required columns exist.
- Dimension business keys are unique and nonblank.
- Order, payment and event IDs meet their expected uniqueness rules.
- Unmatched customer and product keys are measured.
- Integrated joins do not multiply source orders.
- REVIEW and invalid records remain available for investigation.
- Aggregate revenue reconciles with eligible order-level revenue.
- Product and customer aggregate totals reconcile with the same eligible
  order population.
- Payment outcome totals reconcile with payment-attempt counts.
- Event aggregates reconcile with event-level records.
- Published Parquet row counts match the source DataFrames.

If a uniqueness or reconciliation test fails, stop publication and
investigate rather than silently dropping duplicate records.

## 8. Publication and Refresh

The Glue job writes Parquet outputs to S3 and registers or updates
corresponding Glue Catalog tables.

The deployment workflow must execute curated processing only after
the refined layer succeeds.

Use a controlled refresh and validate outputs before exposing them to
the Streamlit dashboard. Do not run concurrent jobs that overwrite the
same curated S3 prefixes.

## 9. Known Limitations

- Actual status values still require validation against refined data.
- Source key uniqueness must be tested against the current datasets.
- Payments cannot currently be attributed to orders without a verified
  relationship.
- Current product stock is not historical stock at the time of sale.
- A customer journey funnel is not approved until event semantics and
  sequence are verified.
