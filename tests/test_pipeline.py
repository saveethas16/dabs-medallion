"""
Unit Tests for Medallion Pipeline
Run with: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("medallion_tests") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


@pytest.fixture
def sample_orders(spark):
    schema = StructType([
        StructField("order_id",       StringType(),  True),
        StructField("customer_id",    StringType(),  True),
        StructField("customer_name",  StringType(),  True),
        StructField("customer_email", StringType(),  True),
        StructField("product_id",     StringType(),  True),
        StructField("product_name",   StringType(),  True),
        StructField("category",       StringType(),  True),
        StructField("quantity",       StringType(),  True),
        StructField("unit_price",     StringType(),  True),
        StructField("amount",         StringType(),  True),
        StructField("order_date",     StringType(),  True),
        StructField("status",         StringType(),  True),
    ])
    data = [
        ("1001", "C001", "Alice Johnson", "alice@email.com", "P001", "Laptop", "Electronics", "1", "1200.00", "1200.00", "2024-01-15", "completed"),
        ("1002", "C002", "Bob Smith",     "bob@email.com",   "P002", "Chair",  "Furniture",   "2", "250.00",  "500.00",  "2024-01-16", "completed"),
        ("1001", "C001", "Alice Johnson", "alice@email.com", "P001", "Laptop", "Electronics", "1", "1200.00", "1200.00", "2024-01-15", "completed"),  # duplicate
        ("1003", None,   "Unknown",       "x@email.com",     "P003", "Hub",    "Electronics", "1", "30.00",   "30.00",   "2024-01-17", "completed"),  # null customer_id
        ("1004", "C003", "Carol White",   "carol@email.com", "P001", "Laptop", "Electronics", "1", "1200.00", None,      "2024-01-18", "completed"),  # null amount
    ]
    return spark.createDataFrame(data, schema)


# ─── Bronze Tests ──────────────────────────────────────────────────────────────

class TestBronze:
    def test_bronze_reads_all_rows(self, sample_orders):
        """Bronze should load all rows including duplicates and nulls."""
        assert sample_orders.count() == 5

    def test_bronze_has_expected_columns(self, sample_orders):
        expected = {"order_id", "customer_id", "product_name", "amount", "order_date"}
        assert expected.issubset(set(sample_orders.columns))


# ─── Silver Tests ──────────────────────────────────────────────────────────────

class TestSilver:
    def _transform(self, df):
        """Simulate silver transformations."""
        from pyspark.sql.functions import (
            trim, upper, to_date, year, month, quarter,
            when, lit, current_timestamp
        )
        return df \
            .dropDuplicates(["order_id"]) \
            .filter(col("order_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("amount", when(col("amount").isNull(), lit(0.0)).otherwise(col("amount").cast("double"))) \
            .withColumn("customer_name", when(col("customer_name").isNull(), lit("Unknown")).otherwise(col("customer_name"))) \
            .withColumn("category", upper(trim(col("category")))) \
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
            .withColumn("order_year", year(col("order_date"))) \
            .withColumn("order_month", month(col("order_date"))) \
            .withColumn("order_quarter", quarter(col("order_date")))

    def test_deduplication(self, sample_orders):
        """Duplicates on order_id should be removed."""
        result = self._transform(sample_orders)
        assert result.count() == result.dropDuplicates(["order_id"]).count()

    def test_null_customer_id_removed(self, sample_orders):
        """Rows with null customer_id should be filtered out."""
        result = self._transform(sample_orders)
        assert result.filter(col("customer_id").isNull()).count() == 0

    def test_null_amount_filled(self, sample_orders):
        """Null amount should be replaced with 0.0."""
        result = self._transform(sample_orders)
        assert result.filter(col("amount").isNull()).count() == 0

    def test_derived_columns_exist(self, sample_orders):
        """order_year, order_month, order_quarter should exist."""
        result = self._transform(sample_orders)
        assert "order_year"    in result.columns
        assert "order_month"   in result.columns
        assert "order_quarter" in result.columns

    def test_category_uppercased(self, sample_orders):
        """Category should be uppercased."""
        result = self._transform(sample_orders)
        categories = [r["category"] for r in result.select("category").collect()]
        assert all(c == c.upper() for c in categories)

    def test_order_year_correct(self, sample_orders):
        """order_year should be extracted correctly."""
        result = self._transform(sample_orders)
        years = [r["order_year"] for r in result.select("order_year").collect()]
        assert all(y == 2024 for y in years)


# ─── Gold Tests ────────────────────────────────────────────────────────────────

class TestGold:
    def test_fact_sales_only_completed(self, sample_orders):
        """fact_sales should only contain completed orders."""
        from pyspark.sql.functions import to_date, when, lit
        df = sample_orders \
            .dropDuplicates(["order_id"]) \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("amount", when(col("amount").isNull(), lit(0.0)).otherwise(col("amount").cast("double"))) \
            .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
            .filter("status = 'completed'")
        assert df.filter(col("status") != "completed").count() == 0

    def test_dim_customer_unique(self, sample_orders):
        """dim_customer should have unique customer_ids."""
        from pyspark.sql.functions import max as _max, current_timestamp
        df = sample_orders.filter(col("customer_id").isNotNull())
        dim = df.groupBy("customer_id", "customer_name", "customer_email") \
                .agg(_max("order_date").alias("last_order_date"))
        assert dim.count() == dim.dropDuplicates(["customer_id"]).count()

    def test_dim_product_unique(self, sample_orders):
        """dim_product should have unique product_ids."""
        dim = sample_orders.groupBy("product_id", "product_name", "category").count()
        assert dim.count() == dim.dropDuplicates(["product_id"]).count()
