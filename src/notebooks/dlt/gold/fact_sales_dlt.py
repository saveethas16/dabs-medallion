# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT fact_sales
# MAGIC Aggregated sales fact table from silver_orders.

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp

dlt_gold_schema = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id",  "product_id IS NOT NULL")
@dlt.expect_or_drop("completed_status",  "status = 'completed'")
@dlt.table(
    name="fact_sales",
    schema=dlt_gold_schema,
    comment="Sales fact table from completed orders - DLT pipeline",
    table_properties={"quality": "gold"}
)
def fact_sales():
    return (
        dlt.read("silver_orders")
        .groupBy(
            "order_id", "customer_id", "product_id",
            "order_date", "order_year", "order_month", "order_quarter",
            "quantity", "unit_price", "amount", "status"
        )
        .agg(current_timestamp().alias("updated_at"))
    )