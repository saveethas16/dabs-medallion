# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT fact_sales

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp

catalog           = spark.conf.get("catalog")
dlt_silver_schema = spark.conf.get("dlt_silver_schema")
dlt_gold_schema   = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id",  "product_id IS NOT NULL")
@dlt.expect_or_drop("completed_status",  "status = 'completed'")
@dlt.table(
    name=f"{catalog}.{dlt_gold_schema}.fact_sales",
    comment="Sales fact table from completed orders - DLT pipeline",
    table_properties={"quality": "gold"}
)
def fact_sales():
    return (
        dlt.read(f"{catalog}.{dlt_silver_schema}.silver_orders")
        .groupBy(
            "order_id", "customer_id", "product_id",
            "order_date", "order_year", "order_month", "order_quarter",
            "quantity", "unit_price", "amount", "status"
        )
        .agg(current_timestamp().alias("updated_at"))
    )