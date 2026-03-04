# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - dim_date
# MAGIC Date dimension table generated from order dates in silver_orders.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")

catalog       = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, month, quarter, dayofmonth,
    dayofweek, weekofyear, date_format, current_timestamp
)
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.silver_orders")

dim_date = df_silver.select("order_date").distinct() \
    .withColumn("year",         year(col("order_date"))) \
    .withColumn("month",        month(col("order_date"))) \
    .withColumn("quarter",      quarter(col("order_date"))) \
    .withColumn("day",          dayofmonth(col("order_date"))) \
    .withColumn("day_of_week",  dayofweek(col("order_date"))) \
    .withColumn("week_of_year", weekofyear(col("order_date"))) \
    .withColumn("month_name",   date_format(col("order_date"), "MMMM")) \
    .withColumn("day_name",     date_format(col("order_date"), "EEEE")) \
    .withColumn("is_weekend",   (dayofweek(col("order_date")).isin(1, 7))) \
    .withColumn("updated_at",   current_timestamp())

print(f"[Gold] dim_date records: {dim_date.count()}")
dim_date.show(5)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

gold_table = f"{catalog}.{gold_schema}.dim_date"

if spark.catalog.tableExists(gold_table):
    delta_table = DeltaTable.forName(spark, gold_table)
    delta_table.alias("target").merge(
        dim_date.alias("source"),
        "target.order_date = source.order_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Gold] Merge completed into {gold_table}")
else:
    dim_date.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
    print(f"[Gold] Initial load completed into {gold_table}")
