# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer - Cleansed & Transformed Data
# MAGIC Reads from Bronze, applies transformations:
# MAGIC - Remove duplicates
# MAGIC - Null handling
# MAGIC - Data type casting
# MAGIC - Derived columns (year, month, quarter from order_date)
# MAGIC Merges (UPSERT) into silver Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")

catalog       = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, to_date, year, month, quarter,
    when, lit, current_timestamp, coalesce
)
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 - Read Bronze

# COMMAND ----------

df_raw = spark.table(f"{catalog}.{bronze_schema}.bronze_orders")
print(f"[Silver] Raw records from bronze: {df_raw.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 - Remove Duplicates

# COMMAND ----------

df_deduped = df_raw.dropDuplicates(["order_id"])
print(f"[Silver] After dedup: {df_deduped.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 - Null Handling

# COMMAND ----------

df_clean = df_deduped \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .withColumn("amount", when(col("amount").isNull(), lit(0.0)).otherwise(col("amount").cast("double"))) \
    .withColumn("customer_name", when(col("customer_name").isNull(), lit("Unknown")).otherwise(col("customer_name"))) \
    .withColumn("status", when(col("status").isNull(), lit("unknown")).otherwise(col("status")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 - Data Type Casting & Derived Columns

# COMMAND ----------

df_transformed = df_clean \
    .withColumn("order_id",    col("order_id").cast("integer")) \
    .withColumn("quantity",    col("quantity").cast("integer")) \
    .withColumn("unit_price",  col("unit_price").cast("double")) \
    .withColumn("amount",      col("amount").cast("double")) \
    .withColumn("customer_name",  trim(col("customer_name"))) \
    .withColumn("customer_email", trim(col("customer_email"))) \
    .withColumn("product_name",   trim(col("product_name"))) \
    .withColumn("category",       upper(trim(col("category")))) \
    .withColumn("status",         trim(col("status"))) \
    .withColumn("order_date",     to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("order_year",     year(col("order_date"))) \
    .withColumn("order_month",    month(col("order_date"))) \
    .withColumn("order_quarter",  quarter(col("order_date"))) \
    .withColumn("updated_at",     current_timestamp())

print(f"[Silver] Transformed records: {df_transformed.count()}")
df_transformed.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 - MERGE (Upsert) into Silver Delta Table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

silver_table = f"{catalog}.{silver_schema}.silver_orders"

if spark.catalog.tableExists(silver_table):
    delta_table = DeltaTable.forName(spark, silver_table)
    delta_table.alias("target").merge(
        df_transformed.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Silver] Merge completed into {silver_table}")
else:
    df_transformed.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
    print(f"[Silver] Initial load completed into {silver_table}")
