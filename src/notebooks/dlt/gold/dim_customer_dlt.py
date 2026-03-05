# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT dim_customer

# COMMAND ----------

import dlt
from pyspark.sql.functions import max as _max, current_timestamp

dlt_gold_schema = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.table(
    name="dim_customer",
    schema=dlt_gold_schema,
    comment="Customer dimension table - SCD Type 1, DLT pipeline",
    table_properties={"quality": "gold"}
)
def dim_customer():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id", "customer_name", "customer_email")
        .agg(_max("order_date").alias("last_order_date"))
        .withColumn("updated_at", current_timestamp())
    )