# Databricks - Gold star schema build (silver -> gold)
# File: databricks/gold/01_build_star_schema.py

from pyspark.sql import functions as F

ACCOUNT = "storagedatalake9105"

SILVER_BASE = f"abfss://silver@{ACCOUNT}.dfs.core.windows.net/adventureworks"
GOLD_BASE = f"abfss://gold@{ACCOUNT}.dfs.core.windows.net/adventureworks"
GOLD_DB_PATH = f"{GOLD_BASE}/_metastore/gold.db"


def write_delta(df, path: str, partition_col: str = None) -> None:
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)


def build_dim_date() -> None:
    dim_date = (
        spark.table("silver.calendar")
        .withColumn("date_key", F.date_format(F.col("Date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("Date"))
        .withColumn("month", F.month("Date"))
        .withColumn("month_name", F.date_format(F.col("Date"), "MMM"))
        .withColumn("day", F.dayofmonth("Date"))
        .withColumn("weekofyear", F.weekofyear("Date"))
        .select("date_key", "Date", "year", "month", "month_name", "day", "weekofyear")
    )
    write_delta(dim_date, f"{GOLD_BASE}/dim/dim_date")


def build_dim_customer() -> None:
    dim_customer = (
        spark.table("silver.customers")
        .select(
            "CustomerKey", "Prefix", "FirstName", "LastName", "BirthDate",
            "MaritalStatus", "Gender", "EmailAddress", "AnnualIncome",
            "TotalChildren", "EducationLevel", "Occupation", "HomeOwner"
        )
    )
    write_delta(dim_customer, f"{GOLD_BASE}/dim/dim_customer")


def build_dim_product() -> None:
    products = spark.table("silver.products")
    subcats = spark.table("silver.product_subcategories")
    cats = spark.table("silver.product_categories")

    dim_product = (
        products.alias("p")
        .join(subcats.alias("s"), "ProductSubcategoryKey", "left")
        .join(cats.alias("c"), F.col("s.ProductCategoryKey") == F.col("c.ProductCategoryKey"), "left")
        .select(
            F.col("p.ProductKey"),
            F.col("p.ProductSKU"),
            F.col("p.ProductName"),
            F.col("p.ModelName"),
            F.col("p.ProductDescription"),
            F.col("p.ProductColor"),
            F.col("p.ProductSize"),
            F.col("p.ProductStyle"),
            F.col("p.ProductCost"),
            F.col("p.ProductPrice"),
            F.col("s.ProductSubcategoryKey"),
            F.col("s.SubcategoryName"),
            F.col("c.ProductCategoryKey"),
            F.col("c.CategoryName")
        )
    )
    write_delta(dim_product, f"{GOLD_BASE}/dim/dim_product")


def build_dim_territory() -> None:
    dim_territory = (
        spark.table("silver.territories")
        .withColumnRenamed("SalesTerritoryKey", "TerritoryKey")
        .select("TerritoryKey", "Region", "Country", "Continent")
    )
    write_delta(dim_territory, f"{GOLD_BASE}/dim/dim_territory")


def build_fact_sales() -> None:
    fact_sales = spark.table("silver.fact_sales")
    dim_product = spark.read.format("delta").load(f"{GOLD_BASE}/dim/dim_product")

    fact_gold = (
        fact_sales.alias("f")
        .join(
            dim_product.select("ProductKey", "ProductCost", "ProductPrice").alias("p"),
            "ProductKey",
            "left"
        )
        .withColumn("date_key", F.date_format(F.col("OrderDate"), "yyyyMMdd").cast("int"))
        .withColumn("revenue", F.col("OrderQuantity") * F.col("ProductPrice"))
        .withColumn("cost", F.col("OrderQuantity") * F.col("ProductCost"))
        .withColumn("profit", F.col("revenue") - F.col("cost"))
        .select(
            "date_key",
            "CustomerKey",
            "ProductKey",
            "TerritoryKey",
            "OrderNumber",
            "OrderLineItem",
            "OrderQuantity",
            "revenue",
            "cost",
            "profit"
        )
    )

    write_delta(fact_gold, f"{GOLD_BASE}/fact/fact_sales", partition_col="date_key")


def register_gold_tables() -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION '{GOLD_DB_PATH}'")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_date
        USING DELTA
        LOCATION '{GOLD_BASE}/dim/dim_date'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_customer
        USING DELTA
        LOCATION '{GOLD_BASE}/dim/dim_customer'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_product
        USING DELTA
        LOCATION '{GOLD_BASE}/dim/dim_product'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_territory
        USING DELTA
        LOCATION '{GOLD_BASE}/dim/dim_territory'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.fact_sales
        USING DELTA
        LOCATION '{GOLD_BASE}/fact/fact_sales'
    """)

    spark.sql("""
        CREATE OR REPLACE VIEW gold.vw_sales_enriched AS
        SELECT
          f.OrderNumber,
          f.OrderLineItem,
          d.Date AS OrderDate,
          d.year,
          d.month,
          t.Region,
          t.Country,
          t.Continent,
          c.CustomerKey,
          c.FirstName,
          c.LastName,
          c.Gender,
          c.AnnualIncome,
          p.ProductKey,
          p.ProductName,
          p.CategoryName,
          p.SubcategoryName,
          f.OrderQuantity,
          f.revenue,
          f.cost,
          f.profit
        FROM gold.fact_sales f
        JOIN gold.dim_date d      ON f.date_key = d.date_key
        JOIN gold.dim_customer c  ON f.CustomerKey = c.CustomerKey
        JOIN gold.dim_product p   ON f.ProductKey = p.ProductKey
        JOIN gold.dim_territory t ON f.TerritoryKey = t.TerritoryKey
    """)


def main() -> None:
    build_dim_date()
    build_dim_customer()
    build_dim_product()
    build_dim_territory()
    build_fact_sales()
    register_gold_tables()

    print("Gold build complete.")
    print("Counts:")
    print("dim_date:", spark.table("gold.dim_date").count())
    print("dim_customer:", spark.table("gold.dim_customer").count())
    print("dim_product:", spark.table("gold.dim_product").count())
    print("dim_territory:", spark.table("gold.dim_territory").count())
    print("fact_sales:", spark.table("gold.fact_sales").count())


if __name__ == "__main__":
    main()
