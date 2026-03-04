# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - dim_product
# MAGIC Product dimension table derived from silver_orders.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")

catalog       = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

# COMMAND ----------

from pyspark.sql.functions import avg, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.silver_orders")

dim_product = df_silver.groupBy("product_id", "product_name", "category") \
    .agg(avg("unit_price").alias("avg_unit_price")) \
    .withColumn("updated_at", current_timestamp())

print(f"[Gold] dim_product records: {dim_product.count()}")
dim_product.show(5)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

gold_table = f"{catalog}.{gold_schema}.dim_product"

if spark.catalog.tableExists(gold_table):
    delta_table = DeltaTable.forName(spark, gold_table)
    delta_table.alias("target").merge(
        dim_product.alias("source"),
        "target.product_id = source.product_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Gold] Merge completed into {gold_table}")
else:
    dim_product.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
    print(f"[Gold] Initial load completed into {gold_table}")
