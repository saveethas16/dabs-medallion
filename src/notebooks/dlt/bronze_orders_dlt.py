# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer - DLT Raw Data Ingestion

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp

catalog           = spark.conf.get("catalog")
volume_schema     = spark.conf.get("volume_schema")
volume_name       = spark.conf.get("volume_name")
file_name         = spark.conf.get("file_name")
dlt_bronze_schema = spark.conf.get("dlt_bronze_schema")

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{dlt_bronze_schema}.bronze_orders",
    comment="Raw ingested orders - DLT pipeline",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    raw_path = f"/Volumes/{catalog}/{volume_schema}/{volume_name}/{file_name}"
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(raw_path)
        .withColumn("ingested_at", current_timestamp())
    )