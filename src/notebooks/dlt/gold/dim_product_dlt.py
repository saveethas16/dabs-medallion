# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT dim_product

# COMMAND ----------

import dlt
from pyspark.sql.functions import avg, current_timestamp

catalog           = spark.conf.get("catalog")
dlt_silver_schema = spark.conf.get("dlt_silver_schema")
dlt_gold_schema   = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dlt.table(
    name=f"{catalog}.{dlt_gold_schema}.dim_product",
    comment="Product dimension table derived from silver_orders - DLT pipeline",
    table_properties={"quality": "gold"}
)
def dim_product():
    return (
        dlt.read(f"{catalog}.{dlt_silver_schema}.silver_orders")
        .groupBy("product_id", "product_name", "category")
        .agg(avg("unit_price").alias("avg_unit_price"))
        .withColumn("updated_at", current_timestamp())
    )