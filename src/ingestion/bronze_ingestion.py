import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


#find the project root directory
project_root = Path(__file__).resolve().parents[2]

#define the source CSV file path
source_file = project_root / "data" / "raw" / "ncr_ride_bookings.csv"

#define bronze output directory
bronze_path = project_root / "data" / "bronze" / "ride_bookings"

#create a Spark session
builder = (
    SparkSession.builder 
    .appName("BronzeIngestion") 
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    #read the source csv
    df = (
        spark.read
        .option("header", "true") 
        .option("inferSchema", "true") 
        .csv(str(source_file))
    )


    import re

    #normalize column names for delta lake
    for column in df.columns:
        new_column = re.sub(r"[^a-zA-Z0-9_]", "_", column)
        df = df.withColumnRenamed(column, new_column)

        

    print("Data read successfully from:", source_file)
    print("number of rows:", df.count() )
    print("number of columns:", len(df.columns) )

    #write the DataFrame to the bronze directory in delta format
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(str(bronze_path))
    )

    print("Bronze ingestion completed successfully")
    print("Bronze output saved to:", bronze_path)

finally:
    #stop the Spark session
    spark.stop()
          
            
