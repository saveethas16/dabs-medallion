# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer - DLT Cleansed & Transformed Data

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, trim, upper, to_date, year, month, quarter,
    when, lit, current_timestamp
)

catalog           = spark.conf.get("catalog")
dlt_bronze_schema = spark.conf.get("dlt_bronze_schema")
dlt_silver_schema = spark.conf.get("dlt_silver_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_order_id",    "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_product_id",  "product_id IS NOT NULL")
@dlt.table(
    name=f"{catalog}.{dlt_silver_schema}.silver_orders",
    comment="Cleansed and validated orders - DLT pipeline",
    table_properties={"quality": "silver"}
)
def silver_orders():
    return (
        dlt.read(f"{catalog}.{dlt_bronze_schema}.bronze_orders")
        .dropDuplicates(["order_id"])
        .withColumn("amount",         when(col("amount").isNull(),        lit(0.0)).otherwise(col("amount").cast("double")))
        .withColumn("customer_name",  when(col("customer_name").isNull(), lit("Unknown")).otherwise(col("customer_name")))
        .withColumn("status",         when(col("status").isNull(),        lit("unknown")).otherwise(col("status")))
        .withColumn("order_id",       col("order_id").cast("integer"))
        .withColumn("quantity",       col("quantity").cast("integer"))
        .withColumn("unit_price",     col("unit_price").cast("double"))
        .withColumn("amount",         col("amount").cast("double"))
        .withColumn("customer_name",  trim(col("customer_name")))
        .withColumn("customer_email", trim(col("customer_email")))
        .withColumn("product_name",   trim(col("product_name")))
        .withColumn("category",       upper(trim(col("category"))))
        .withColumn("status",         trim(col("status")))
        .withColumn("order_date",     to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("order_year",     year(col("order_date")))
        .withColumn("order_month",    month(col("order_date")))
        .withColumn("order_quarter",  quarter(col("order_date")))
        .withColumn("updated_at",     current_timestamp())
    )