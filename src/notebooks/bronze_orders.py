# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC Reads raw CSV from Volume and writes to Delta table with no transformations.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("volume_schema", "")
dbutils.widgets.text("volume_name", "")
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("bronze_schema", "")

catalog       = dbutils.widgets.get("catalog")
volume_schema = dbutils.widgets.get("volume_schema")
volume_name   = dbutils.widgets.get("volume_name")
file_name     = dbutils.widgets.get("file_name")
bronze_schema = dbutils.widgets.get("bronze_schema")

# COMMAND ----------

raw_path = f"/Volumes/{catalog}/{volume_schema}/{volume_name}/{file_name}"
print(f"[Bronze] Reading from: {raw_path}")

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_path)

print(f"[Bronze] Records read: {df.count()}")
df.printSchema()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze_schema}")

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{bronze_schema}.bronze_orders")

print(f"[Bronze] Successfully written to {catalog}.{bronze_schema}.bronze_orders")
