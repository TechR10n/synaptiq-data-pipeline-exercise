# Data Dictionary — Pipeline Exercise

You have been given three source files that a (fictional) retail system drops into storage.

## Order drop files
New order files arrive **once per day**. You have two consecutive days:

- `orders_2024-01-01.csv`
- `orders_2024-01-02.csv`

> These are periodic drops from an upstream order system — one file per day.

| column | meaning |
|---|---|
| `order_id` | Identifier assigned to the order by the source system. |
| `order_date` | Date the order was placed. |
| `customer_id` | Customer who placed the order. |
| `product_id` | Product ordered; joins to `products.csv`. |
| `quantity` | Units ordered. |
| `unit_price` | Price per unit at time of sale. |
| `region` | Sales region. |

## Product reference
- `products.csv` — reference data describing each product.

| column | meaning |
|---|---|
| `product_id` | Product identifier. |
| `product_name` | Display name. |
| `category` | Product category (used for reporting). |
| `list_price` | Catalog list price. |

## The task
For what to build, the ground rules, and what to submit, see `candidate_brief.md`. This file documents the input data only.
