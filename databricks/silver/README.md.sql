
# Silver Layer README (`01_transform.py` + `silver_checks.sql`)

This README documents the Silver-layer implementation for the AdventureWorks lakehouse pipeline.

Silver is the conformed, cleaned layer between Bronze (raw-ish Delta) and Gold (analytics/star schema).  
It is designed to make downstream modeling stable, consistent, and auditable.

---

## 1. Scope

This layer is implemented with two assets:

- `01_transform.py`  
  Transforms Bronze tables into curated Silver tables.

- `silver_checks.sql`  
  Validates Silver outputs for completeness and quality.

---

## 2. Objectives of Silver

The Silver layer in this project is responsible for:

- standardizing datatypes and text fields
- removing invalid/null key records
- deduplicating records by business grain
- applying minimal business-rule filters
- writing clean Delta tables for Gold consumption
- proving quality through repeatable SQL checks

---

## 3. Source and Target Design

### Source (Bronze tables)

- `bronze.sales`
- `bronze.dim_calendar`
- `bronze.dim_customers`
- `bronze.dim_product_categories`
- `bronze.dim_product_subcategories`
- `bronze.dim_products`
- `bronze.dim_territories`

### Target (Silver tables)

- `silver.calendar`
- `silver.customers`
- `silver.product_categories`
- `silver.product_subcategories`
- `silver.products`
- `silver.territories`
- `silver.fact_sales`

### Storage location (ADLS)

- Base: `abfss://silver@storagedatalake9105.dfs.core.windows.net/adventureworks`
- Schema location: `abfss://silver@storagedatalake9105.dfs.core.windows.net/adventureworks/_metastore/silver.db`

---

## 4. Why Explicit Silver Database LOCATION Is Used

Instead of relying on default metastore paths (which may point to blocked DBFS root), the script creates Silver schema with explicit ADLS location.

This avoids errors like DBFS-root access denied and keeps schema metadata aligned with governed cloud storage.

---

## 5. `01_transform.py` Logic (Detailed)

## 5.1 Profiling before cleaning

For each Bronze table, profiling runs first to print:

- schema
- total row count
- null counts for key columns
- duplicate key-group count
- sample records
- for fact: invalid quantity count and date min/max

This is intentionally done **before** transformations so cleaning decisions are transparent.

## 5.2 Common cleaning rule: string trim

All string columns are trimmed to remove leading/trailing spaces, preventing join/key mismatches and inconsistent categories.

## 5.3 Dimension conformance rules

Applied per table:

- drop rows where primary business key is null
- deduplicate by business key
- keep conformed column types
- write clean Delta output
- register external Silver table

Key columns used:

- `calendar` -> `Date`
- `customers` -> `CustomerKey`
- `product_categories` -> `ProductCategoryKey`
- `product_subcategories` -> `ProductSubcategoryKey`
- `products` -> `ProductKey`
- `territories` -> `SalesTerritoryKey`

Additional normalization:

- `customers.AnnualIncome` converted from formatted currency string to numeric (`double`)

## 5.4 Fact (`silver.fact_sales`) rules

Fact-specific transformations:

1. read from `bronze.sales`
2. trim string fields
3. drop ingestion audit fields from business table:
   - `_ingested_at`
   - `_source_file`
   - `_batch_id`
4. cast `OrderDate` to date
5. filter out invalid rows:
   - keep `OrderQuantity > 0`
6. enforce required non-null fields:
   - `OrderDate`, `OrderNumber`, `ProductKey`, `CustomerKey`, `TerritoryKey`
7. deduplicate by transactional grain:
   - `OrderNumber`, `OrderLineItem`
8. add `sales_year = year(OrderDate)` for partitioning
9. write Delta partitioned by `sales_year`
10. register as `silver.fact_sales`

## 5.5 Write strategy

All Silver tables are written with:

- overwrite mode
- overwriteSchema enabled

This gives deterministic rebuild behavior per run.

---

## 6. `silver_checks.sql` Logic (Detailed)

The validation SQL covers structural, data-quality, and business-integrity checks.

## 6.1 Structural checks

- list all tables in `silver` schema

## 6.2 Count checks

- row counts for all core Silver tables:
  - calendar
  - customers
  - products
  - territories
  - fact_sales

## 6.3 Null checks on fact critical fields

Counts nulls in:

- `OrderDate`
- `OrderNumber`
- `OrderLineItem`
- `ProductKey`
- `CustomerKey`
- `TerritoryKey`

Expected: all zero.

## 6.4 Duplicate grain check

Detects duplicates for fact grain:

- `(OrderNumber, OrderLineItem)`

Expected: zero duplicate groups.

## 6.5 Business validity check

- counts rows where `OrderQuantity <= 0`

Expected: zero.

## 6.6 Date coverage check

Returns `MIN(OrderDate)` and `MAX(OrderDate)` to verify source-period completeness.

## 6.7 Referential checks (fact -> dimensions)

- missing customers
- missing products
- missing territories
- missing dates

Expected: all zero.

## 6.8 Expected baseline PASS/FAIL check

Compares actual counts against expected baseline:

- calendar: `912`
- customers: `18148`
- products: `293`
- territories: `10`
- fact_sales: `56046`

Returns PASS/FAIL per table.

---

## 7. Run Instructions

1. Run `01_transform.py`
2. Run `silver_checks.sql`
3. Confirm all quality checks pass

---

## 8. Current Verified Results (from your run)

- `silver.calendar = 912`
- `silver.customers = 18148`
- `silver.products = 293`
- `silver.territories = 10`
- `silver.fact_sales = 56046`
- expected-count checks: all PASS

This indicates Silver is ready for Gold.

---

## 9. Operational Notes

- If Bronze tables are missing, register them first.
- Run SQL in `%sql` cells (not Python cells).
- Keep checks in source control; do not rely only on notebook output.
- Use these Silver checks as mandatory gate before Gold processing.

---

