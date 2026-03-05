# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT dim_product
# MAGIC Product dimension table derived from silver_orders.

# COMMAND ----------

import dlt
from pyspark.sql.functions import avg, current_timestamp

dlt_gold_schema = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.table(
    name="dim_product",
    schema=dlt_gold_schema,
    comment="Product dimension table derived from silver_orders - DLT pipeline",
    table_properties={"quality": "gold"}
)
def dim_product():
    return (
        dlt.read("silver_orders")
        .groupBy("product_id", "product_name", "category")
        .agg(avg("unit_price").alias("avg_unit_price"))
        .withColumn("updated_at", current_timestamp())
    )