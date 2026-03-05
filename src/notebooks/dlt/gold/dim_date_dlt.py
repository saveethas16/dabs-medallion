# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - DLT dim_date

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, year, month, quarter, dayofmonth,
    dayofweek, weekofyear, date_format, current_timestamp
)

catalog           = spark.conf.get("catalog")
dlt_silver_schema = spark.conf.get("dlt_silver_schema")
dlt_gold_schema   = spark.conf.get("dlt_gold_schema")

# COMMAND ----------

@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dlt.table(
    name=f"{catalog}.{dlt_gold_schema}.dim_date",
    comment="Date dimension table generated from order dates - DLT pipeline",
    table_properties={"quality": "gold"}
)
def dim_date():
    return (
        dlt.read(f"{catalog}.{dlt_silver_schema}.silver_orders")
        .select("order_date").distinct()
        .withColumn("year",         year(col("order_date")))
        .withColumn("month",        month(col("order_date")))
        .withColumn("quarter",      quarter(col("order_date")))
        .withColumn("day",          dayofmonth(col("order_date")))
        .withColumn("day_of_week",  dayofweek(col("order_date")))
        .withColumn("week_of_year", weekofyear(col("order_date")))
        .withColumn("month_name",   date_format(col("order_date"), "MMMM"))
        .withColumn("day_name",     date_format(col("order_date"), "EEEE"))
        .withColumn("is_weekend",   dayofweek(col("order_date")).isin(1, 7))
        .withColumn("updated_at",   current_timestamp())
    )