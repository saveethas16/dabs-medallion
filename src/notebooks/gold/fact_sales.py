# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - fact_sales
# MAGIC Aggregated sales fact table from silver_orders.
# MAGIC Merges (UPSERT) into gold Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")

catalog       = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, avg, max as _max, min as _min, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.silver_orders") \
    .filter("status = 'completed'")

# COMMAND ----------

fact_df = df_silver.groupBy(
    "order_id", "customer_id", "product_id",
    "order_date", "order_year", "order_month", "order_quarter",
    "quantity", "unit_price", "amount", "status"
).agg(
    current_timestamp().alias("updated_at")
)

print(f"[Gold] fact_sales records: {fact_df.count()}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

gold_table = f"{catalog}.{gold_schema}.fact_sales"

if spark.catalog.tableExists(gold_table):
    delta_table = DeltaTable.forName(spark, gold_table)
    delta_table.alias("target").merge(
        fact_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Gold] Merge completed into {gold_table}")
else:
    fact_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
    print(f"[Gold] Initial load completed into {gold_table}")
