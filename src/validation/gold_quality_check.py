import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder
    .appName("GoldQualityCheck")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

#gold table validation

gold_tables = {
    "dim_customer": "data/gold/dim_customer",
    "dim_vehicle": "data/gold/dim_vehicle",
    "dim_location": "data/gold/dim_location",
    "dim_date": "data/gold/dim_date",
    "dim_payment_method": "data/gold/dim_payment_method",
    "dim_booking_status": "data/gold/dim_booking_status",
    "fact_rides": "data/gold/fact_rides"
}

print("\n========== GOLD LAYER VALIDATION ==========")

for table_name, table_path in gold_tables.items():

    df = spark.read.format("delta").load(table_path)

    print(f"\nTable: {table_name}")
    print(f"Records: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    print("Status: Successfully verified")

print("\n========== VALIDATION COMPLETED ==========")

spark.stop()