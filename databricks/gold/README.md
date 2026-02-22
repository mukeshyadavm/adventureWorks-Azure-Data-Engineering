# AdventureWorks Lakehouse Pipeline - Execution Summary (Bronze -> Silver -> Gold + DQ)

This README documents the completed execution of the Azure Databricks lakehouse pipeline using ADLS Gen2 and Delta Lake.

## 1. Scope of This Step

This step covers the full pipeline run and validation:

1. Bronze ingestion completed
2. Silver transformation completed
3. Gold star schema built
4. KPI tables generated
5. Gold data quality checks executed and passed

---

## 2. Environment

- Platform: Azure Databricks
- Storage: ADLS Gen2
- Storage Account: `storagedatalake9105`
- Containers:
  - `raw`
  - `bronze`
  - `silver`
  - `gold`
- Data format: Delta Lake

---

## 3. Data Flow Implemented

### Bronze Layer
- Raw CSV files ingested to Delta in Bronze:
  - `raw/fact/sales_*.csv` -> `bronze/fact/sales`
  - `raw/dim/*.csv` -> `bronze/dim/dim_*`
- Metadata columns added:
  - `_ingested_at`
  - `_source_file`
  - `_batch_id`
- Fact ingestion uses incremental-safe logic (watermark/key dedupe fallback).

### Silver Layer
- Bronze tables cleaned and standardized into Silver:
  - `silver.calendar`
  - `silver.customers`
  - `silver.product_categories`
  - `silver.product_subcategories`
  - `silver.products`
  - `silver.territories`
  - `silver.fact_sales`
- Applied:
  - string trim
  - null key filtering
  - deduplication by business keys
  - fact business checks (`OrderQuantity > 0`)
  - `sales_year` partitioning for fact

### Gold Layer
- Star schema created:
  - `gold.dim_date`
  - `gold.dim_customer`
  - `gold.dim_product`
  - `gold.dim_territory`
  - `gold.fact_sales`
- Enriched view:
  - `gold.vw_sales_enriched`
- KPI marts created:
  - `gold.kpi_monthly`
  - `gold.kpi_region`
  - `gold.kpi_top_products`

---

## 4. Final Validated Counts

### Gold star schema counts
- `gold.dim_date = 912`
- `gold.dim_customer = 18148`
- `gold.dim_product = 293`
- `gold.dim_territory = 10`
- `gold.fact_sales = 56046`

### KPI count sample
- `gold.kpi_top_products = 130`

---

## 5. Gold Data Quality Result (Passed)

`01_gold_checks.py` output:

- `invalid_quantity = 0`
- `missing_customers = 0`
- `missing_dates = 0`
- `missing_products = 0`
- `missing_territories = 0`
- `negative_profit = 0`
- `negative_revenue = 0`
- `null_customer = 0`
- `null_date = 0`
- `null_product = 0`

Run timestamp:
- `2026-02-22T14:40:14.321994+00:00`

This confirms referential integrity and core business quality checks passed.

---

## 6. Files Used in This Run

- `databricks/bronze/01_ingest.py`
- `databricks/silver/01_transform.py`
- `databricks/validation/silver_checks.sql`
- `databricks/gold/01_build_star_schema.py`
- `databricks/gold/02_build_kpis.sql`
- `databricks/data_quality/01_gold_checks.py`

---

## 7. Run Order (Reference)

1. Bronze ingestion (`01_ingest.py`)
2. Silver transform (`01_transform.py`)
3. Silver validation (`silver_checks.sql`)
4. Gold star schema build (`01_build_star_schema.py`)
5. KPI builds (`02_build_kpis.sql`)
6. Gold DQ checks (`01_gold_checks.py`)

---

## 8. Operational Notes

- Schemas (`bronze`, `silver`, `gold`) are created with explicit ADLS `LOCATION` to avoid DBFS-root restrictions.
- Delta table registration uses external table `LOCATION` paths.
- Notebook output is not required for Git commits; source code and SQL checks are the source of truth.

---

## 9. Conclusion

The full lakehouse pipeline has been implemented and validated successfully.  
Data is now ready for dashboarding, BI consumption, and downstream analytics use cases from Gold tables and KPI marts.
