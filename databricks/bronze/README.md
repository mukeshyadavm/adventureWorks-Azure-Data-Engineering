# Bronze Layer README (`databricks/bronze`)

This folder contains Bronze ingestion logic for the AdventureWorks lakehouse project on Azure Databricks.

The Bronze layer is responsible for:

- Landing raw source CSV data into Delta format
- Preserving source-level fidelity with minimal transformation
- Adding ingestion metadata for auditability
- Registering Bronze tables for SQL access
- Supporting rerun-safe ingestion behavior

---

## 1. Scope of This Layer

Bronze handles two ingestion streams:

1. Fact sales ingestion
2. Dimension ingestion

Both are implemented in a single script:

- `01_ingest.py`

---

## 2. Design Principles

- **Minimal transformation** at Bronze stage
- **Traceability** using metadata columns
- **Delta format** for reliability and ACID properties
- **External table registration** using explicit ADLS locations
- **Idempotent reruns** for fact ingestion (incremental-safe behavior)

---

## 3. Environment Configuration

Storage account used:

- `storagedatalake9105`

Container-level separation:

- Source container: `raw`
- Target container: `bronze`

Base paths:

- `RAW_BASE = abfss://raw@storagedatalake9105.dfs.core.windows.net/adventureworks`
- `BRONZE_BASE = abfss://bronze@storagedatalake9105.dfs.core.windows.net/adventureworks`

Bronze metadata location:

- `abfss://bronze@storagedatalake9105.dfs.core.windows.net/adventureworks/_metastore/bronze.db`

---

## 4. Input Data

### Fact input

- `raw/fact/sales_*.csv`

### Dimension inputs

- `raw/dim/calendar.csv`
- `raw/dim/customers.csv`
- `raw/dim/product_categories.csv`
- `raw/dim/product_subcategories.csv`
- `raw/dim/products.csv`
- `raw/dim/territories.csv`

---

## 5. Output Data

### Fact output path

- `bronze/fact/sales` (Delta)

### Dimension output paths

- `bronze/dim/dim_calendar`
- `bronze/dim/dim_customers`
- `bronze/dim/dim_product_categories`
- `bronze/dim/dim_product_subcategories`
- `bronze/dim/dim_products`
- `bronze/dim/dim_territories`

---

## 6. Table Registration

Bronze tables registered by script:

- `bronze.sales`
- `bronze.dim_calendar`
- `bronze.dim_customers`
- `bronze.dim_product_categories`
- `bronze.dim_product_subcategories`
- `bronze.dim_products`
- `bronze.dim_territories`

---

## 7. Metadata Columns Added

All ingested datasets receive:

- `_ingested_at` (timestamp): load time
- `_source_file` (string): input file path
- `_batch_id` (string): run identifier (timestamp format)

These support observability, lineage, and troubleshooting.

---

## 8. Fact Ingestion Logic

Fact ingestion reads `sales_*.csv` and writes to Delta `bronze/fact/sales`.

### Incremental-safe behavior

The script checks whether the target path is an existing Delta table by testing for `_delta_log`.

- If `_delta_log` does not exist:
  - Treat as first run and load full dataset
- If `_delta_log` exists:
  - Attempt watermark filtering using:
    - `ModifiedDate` (preferred), else
    - `OrderDate`
  - If watermark is unavailable/inapplicable:
    - fallback dedupe via anti-join on business keys:
      - `OrderNumber`
      - `OrderLineItem`

### Practical behavior for fixed historical files

For static source data ( current setup):

- First run: full load (e.g., 56046 rows)
- Subsequent runs: 0 rows loaded (expected)
- Benefit: prevents duplicate reload on reruns

---

## 9. Dimension Ingestion Logic

Dimension ingestion loops through each dimension CSV and writes to dedicated Bronze Delta paths.

Current write mode:

- `overwrite`

This treats dimensions as latest snapshots at Bronze level.

---

## 10. Why `CREATE DATABASE ... LOCATION` Is Required

In my workspace, default schema creation without `LOCATION` may attempt to write metadata under DBFS root (`/user/hive/warehouse`), which is blocked by policy.

This script uses explicit ADLS location:

```sql
CREATE DATABASE IF NOT EXISTS bronze
LOCATION 'abfss://bronze@storagedatalake9105.dfs.core.windows.net/adventureworks/_metastore/bronze.db';
---

##   11. How to Run
Run 01_ingest.py from Databricks with a cluster attached.
Recommended pre-checks:
dbutils.fs.ls("abfss://raw@storagedatalake9105.dfs.core.windows.net/")
dbutils.fs.ls("abfss://bronze@storagedatalake9105.dfs.core.windows.net/")
Then execute script/notebook.
