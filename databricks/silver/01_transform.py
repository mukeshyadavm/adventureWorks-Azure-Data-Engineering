# Databricks - Silver transform (bronze -> silver) with profiling before transforms
# File: databricks/silver/01_transform.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ACCOUNT = "storagedatalake9105"

BRONZE_BASE = f"abfss://bronze@{ACCOUNT}.dfs.core.windows.net/adventureworks"
SILVER_BASE = f"abfss://silver@{ACCOUNT}.dfs.core.windows.net/adventureworks"
SILVER_DB_PATH = f"{SILVER_BASE}/_metastore/silver.db"

BRONZE_TABLES = {
    "calendar": "bronze.dim_calendar",
    "customers": "bronze.dim_customers",
    "product_categories": "bronze.dim_product_categories",
    "product_subcategories": "bronze.dim_product_subcategories",
    "products": "bronze.dim_products",
    "territories": "bronze.dim_territories",
    "sales": "bronze.sales",
}

KEYS = {
    "calendar": ["Date"],
    "customers": ["CustomerKey"],
    "product_categories": ["ProductCategoryKey"],
    "product_subcategories": ["ProductSubcategoryKey"],
    "products": ["ProductKey"],
    "territories": ["SalesTerritoryKey"],
    "sales": ["OrderNumber", "OrderLineItem"],
}


def trim_all_strings(df: DataFrame) -> DataFrame:
    out = df
    for c, t in out.dtypes:
        if t == "string":
            out = out.withColumn(c, F.trim(F.col(c)))
    return out


def profile_table(df: DataFrame, table_name: str, key_cols: list, show_rows: int = 5) -> None:
    print(f"\n=== PROFILE: {table_name} ===")
    print("Schema:")
    df.printSchema()

    total = df.count()
    print(f"Total rows: {total}")

    # Null counts on key columns
    for k in key_cols:
        if k in df.columns:
            null_cnt = df.filter(F.col(k).isNull()).count()
            print(f"Nulls in {k}: {null_cnt}")

    # Duplicate count by key
    if all(k in df.columns for k in key_cols):
        dup_cnt = (
            df.groupBy(*key_cols)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        print(f"Duplicate key groups ({key_cols}): {dup_cnt}")

    # Business-specific quick checks for fact
    if table_name == "sales":
        if "OrderQuantity" in df.columns:
            invalid_qty = df.filter(F.col("OrderQuantity") <= 0).count()
            print(f"Rows with OrderQuantity <= 0: {invalid_qty}")
        if "OrderDate" in df.columns:
            minmax = df.select(
                F.min("OrderDate").alias("min_order_date"),
                F.max("OrderDate").alias("max_order_date"),
            ).collect()[0]
            print(f"OrderDate min/max: {minmax['min_order_date']} -> {minmax['max_order_date']}")

    print("Sample rows:")
    df.show(show_rows, truncate=False)


def write_and_register(df: DataFrame, path: str, table_name: str, partition_cols=None) -> None:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)


def build_silver_dimensions() -> None:
    # calendar
    cal_raw = spark.table(BRONZE_TABLES["calendar"])
    profile_table(cal_raw, "calendar", KEYS["calendar"])
    cal = trim_all_strings(cal_raw).dropna(subset=["Date"]).dropDuplicates(["Date"])
    write_and_register(cal, f"{SILVER_BASE}/dim/calendar", "silver.calendar")
    print(f"silver.calendar rows: {cal.count()}")

    # customers
    cust_raw = spark.table(BRONZE_TABLES["customers"])
    profile_table(cust_raw, "customers", KEYS["customers"])
    cust = (
        trim_all_strings(cust_raw)
        .withColumn("AnnualIncome", F.regexp_replace(F.col("AnnualIncome"), r"[\$,]", "").cast("double"))
        .dropna(subset=["CustomerKey"])
        .dropDuplicates(["CustomerKey"])
    )
    write_and_register(cust, f"{SILVER_BASE}/dim/customers", "silver.customers")
    print(f"silver.customers rows: {cust.count()}")

    # product_categories
    pc_raw = spark.table(BRONZE_TABLES["product_categories"])
    profile_table(pc_raw, "product_categories", KEYS["product_categories"])
    pc = trim_all_strings(pc_raw).dropna(subset=["ProductCategoryKey"]).dropDuplicates(["ProductCategoryKey"])
    write_and_register(pc, f"{SILVER_BASE}/dim/product_categories", "silver.product_categories")
    print(f"silver.product_categories rows: {pc.count()}")

    # product_subcategories
    psc_raw = spark.table(BRONZE_TABLES["product_subcategories"])
    profile_table(psc_raw, "product_subcategories", KEYS["product_subcategories"])
    psc = trim_all_strings(psc_raw).dropna(subset=["ProductSubcategoryKey"]).dropDuplicates(["ProductSubcategoryKey"])
    write_and_register(psc, f"{SILVER_BASE}/dim/product_subcategories", "silver.product_subcategories")
    print(f"silver.product_subcategories rows: {psc.count()}")

    # products
    prod_raw = spark.table(BRONZE_TABLES["products"])
    profile_table(prod_raw, "products", KEYS["products"])
    prod = trim_all_strings(prod_raw).dropna(subset=["ProductKey"]).dropDuplicates(["ProductKey"])
    write_and_register(prod, f"{SILVER_BASE}/dim/products", "silver.products")
    print(f"silver.products rows: {prod.count()}")

    # territories
    terr_raw = spark.table(BRONZE_TABLES["territories"])
    profile_table(terr_raw, "territories", KEYS["territories"])
    terr = trim_all_strings(terr_raw).dropna(subset=["SalesTerritoryKey"]).dropDuplicates(["SalesTerritoryKey"])
    write_and_register(terr, f"{SILVER_BASE}/dim/territories", "silver.territories")
    print(f"silver.territories rows: {terr.count()}")


def build_silver_fact() -> None:
    sales_raw = spark.table(BRONZE_TABLES["sales"])
    profile_table(sales_raw, "sales", KEYS["sales"])

    sales = trim_all_strings(sales_raw)

    # Drop ingestion metadata from business silver fact
    drop_cols = [c for c in ["_ingested_at", "_source_file", "_batch_id"] if c in sales.columns]
    if drop_cols:
        sales = sales.drop(*drop_cols)

    if "OrderDate" in sales.columns:
        sales = sales.withColumn("OrderDate", F.to_date(F.col("OrderDate")))

    sales = (
        sales.filter(F.col("OrderQuantity") > 0)
        .dropna(subset=["OrderDate", "OrderNumber", "ProductKey", "CustomerKey", "TerritoryKey"])
        .dropDuplicates(["OrderNumber", "OrderLineItem"])
        .withColumn("sales_year", F.year(F.col("OrderDate")))
    )

    write_and_register(
        sales,
        f"{SILVER_BASE}/fact/sales",
        "silver.fact_sales",
        partition_cols=["sales_year"],
    )
    print(f"silver.fact_sales rows: {sales.count()}")


def main() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{SILVER_DB_PATH}'")
    build_silver_dimensions()
    build_silver_fact()
    print("Silver transform complete.")


if __name__ == "__main__":
    main()
