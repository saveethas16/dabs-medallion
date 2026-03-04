# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Layer - dim_customer
# MAGIC Customer dimension table with SCD Type 1 (overwrite latest).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")

catalog       = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema   = dbutils.widgets.get("gold_schema")

# COMMAND ----------

from pyspark.sql.functions import max as _max, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

df_silver = spark.table(f"{catalog}.{silver_schema}.silver_orders")

dim_customer = df_silver.groupBy("customer_id", "customer_name", "customer_email") \
    .agg(_max("order_date").alias("last_order_date")) \
    .withColumn("updated_at", current_timestamp())

print(f"[Gold] dim_customer records: {dim_customer.count()}")
dim_customer.show(5)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

gold_table = f"{catalog}.{gold_schema}.dim_customer"

if spark.catalog.tableExists(gold_table):
    delta_table = DeltaTable.forName(spark, gold_table)
    delta_table.alias("target").merge(
        dim_customer.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    print(f"[Gold] Merge completed into {gold_table}")
else:
    dim_customer.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
    print(f"[Gold] Initial load completed into {gold_table}")
