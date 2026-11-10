import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

project_root = Path(__file__).resolve().parents[2]
bronze_path = project_root / "data" / "bronze" / "ride_bookings"
silver_path = project_root / "data" / "silver" / "ride_bookings"


builder = (
    SparkSession.builder
    .appName("SilverTransformation")
    .master("local[*]") 
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    # Read the bronze data
    df = spark.read.format("delta").load(str(bronze_path))

    # PROFILE DATA BEFORE SILVER TRANSFORMATION
    
    print("\n--- ACTUAL BOOKING STATUS VALUES ---")

    df.groupBy("Booking_Status") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(50, truncate=False)

    #profile numeric columns

    numeric_columns = [
        "Avg_VTAT",
        "Avg_CTAT",
        "Cancelled_Rides_by_Customer",
        "Cancelled_Rides_by_Driver",
        "Incomplete_Rides",
        "Booking_Value",
        "Ride_Distance",
        "Driver_Ratings",
        "Customer_Rating",
    ]

    print("\n--- NUMERIC COLUMN VALUE PROFILES ---")

    for column_name in numeric_columns:
        print(f"\nColumn: {column_name}")

        df.select(column_name) \
            .distinct() \
            .show(15, truncate=False)

    print("\n--- END OF DATA PROFILE ---")

    #bronze data summary

    print("\n--- BRONZE DATA SUMMARY ---")

    bronze_row_count = df.count()

    print("Number of rows:", bronze_row_count)
    print("Number of columns:", len(df.columns))
    print("Column names:", df.columns)

    print("\nBronze schema:")
    df.printSchema()

    #validate the required columns
    required_columns = [
        "Booking_ID",
        "Booking_Status",
        "Customer_ID",
        "Vehicle_Type",
        "Booking_Value",
        "Ride_Distance",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]
                       
     
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # identify duplicate Booking_ID

    duplicate_booking_ids = (
        df.groupBy("Booking_ID")
        .count()
        .filter(F.col("count") > 1)
        .select("Booking_ID")
    )

    duplicate_count = duplicate_booking_ids.count()

    print("\n--- DATA QUALITY CHECKS ---")
    print("Duplicate Booking IDs:", duplicate_count)

    duplicate_records = (
        df.join(
            duplicate_booking_ids,
            on="Booking_ID", 
            how="inner")
        .orderBy("Booking_ID")

    )

    print("\nSample duplicate records:")

    duplicate_records.show(10, truncate=False)

    duplicate_record_count = duplicate_records.count()

    print(
        "Rows associated with duplicate Booking IDs:",
        duplicate_record_count
    )


    # identify missing or empty Booking ids
    
    missing_booking_ids = df.filter(
        F.col("Booking_ID").isNull()
        | (F.trim(F.col("Booking_ID")) == "")
    )

    missing_id_count = missing_booking_ids.count()

    print("Missing or blank booking IDs:", missing_id_count)

    #  validate Booking Statuses

    valid_statuses = [
    "Completed",
    "Cancelled By Customer",
    "Cancelled By Driver",
    "No Driver Found",
    "Incomplete",
]
    
    normalized_status = F.initcap(F.trim(F.col("Booking_Status")))

    invalid_statuses = df.filter(
        normalized_status.isNull()
        | (~normalized_status.isin(valid_statuses))
)

    invalid_status_count = invalid_statuses.count()

    print("Rows with invalid Booking Statuses:", invalid_status_count)


    #Define numric target data types

    numeric_types = {
        "Avg_VTAT": "DOUBLE",
        "Avg_CTAT": "DOUBLE",
        "Cancelled_Rides_by_Customer": "INT",
        "Cancelled_Rides_by_Driver": "INT",
        "Incomplete_Rides": "INT",
        "Booking_Value": "DECIMAL(10, 2)",
        "Ride_Distance": "DOUBLE",
        "Driver_Ratings": "DOUBLE",
        "Customer_Rating": "DOUBLE",

    }

    # Normalize text representations of missing numeric values
    for column_name in numeric_types:
        df = df.withColumn(
            column_name,
            F.when(
                F.lower(F.trim(F.col(column_name))).isin(
                    "null", "none", "nan", ""
                ),
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col(column_name)))
        )

    # Normalize text representations of missing payment methods

    df = df.withColumn(
        "Payment_Method",
        F.when(
            F.lower(F.trim(F.col("Payment_Method"))).isin(
                "null", "none", "nan", ""
            ),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col("Payment_Method")))
    )   

    
    print("\n--- NUMERIC CONVERSION VALIDATION ---")

    conversion_errors = {}

    for column_name, target_type in numeric_types.items():

        original_value = F.trim(F.col(column_name))

        converted_value = F.expr(
            f"try_cast(`{column_name}` AS {target_type})"
        )

        # Count nonblank, non-null values that cannot
        # be converted to the target data type.
        invalid_count = df.filter(
            F.col(column_name).isNotNull()
            & (original_value != "")
            & converted_value.isNull()
        ).count()

        # Count blank strings separately.
        blank_count = df.filter(
            F.col(column_name).isNotNull()
            & (original_value == "")
        ).count()

        print(
            f"{column_name}: "
            f"blank values={blank_count}, "
            f"target={target_type}, "
            f"invalid values={invalid_count}"
        )

        if invalid_count > 0:
            conversion_errors[column_name] = invalid_count


    # Stop before writing Silver if invalid conversions exist.
    if conversion_errors:
        raise ValueError(
            "Numeric conversion validation failed. "
            f"Silver data was not written: {conversion_errors}"
        )
    
    print("\nNumeric conversion validation passed.")

        
    # BUILD THE SILVER DATAFRAME
    print("\n--- BUILDING SILVER DATAFRAME ---")

    silver_df = df

    # Standardize booking status capitalization and whitespace
    silver_df = df.withColumn(
        "Booking_Status",
        F.initcap(F.trim(F.col("Booking_Status")))
    )

    # Keep the five recognized booking statuses
    silver_df = silver_df.filter(
        F.col("Booking_Status").isin(valid_statuses)
    )

    # Remove records with missing or blank Booking_ID
    silver_df = silver_df.filter(
        F.col("Booking_ID").isNotNull()
        & (F.trim(F.col("Booking_ID")) != "")
    )

    # Convert numeric columns to their target data types
    for column_name, target_type in numeric_types.items():
        silver_df = silver_df.withColumn(
            column_name,
            F.expr(f"try_cast(`{column_name}` AS {target_type})")
        )

    #validate silver schema

    print("\n--- SILVER SCHEMA ---")

    silver_df.printSchema()

    if len(silver_df.columns) != len(df.columns):
        raise ValueError(
            "Column count changed unexpectedly during transformation."
        )


    print("\n--- PAYMENT METHOD VALIDATION ---")

    silver_df.groupBy("Payment_Method").count().show()

    print(
        "Actual NULL payment methods:",
        silver_df.filter(F.col("Payment_Method").isNull()).count()
    )


    # write to silver delta table

    print("\n--- WRITING SILVER DATA ---")

    ( 
         silver_df.write
        .format("delta")        
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(silver_path))


    )

    silver_row_count = silver_df.count()

    print("\n--- SILVER TRANSFORMATION RESULT ---")

    print("Silver transformation completed successfully!")
    print("Bronze rows:", bronze_row_count)
    print("Silver rows:", silver_row_count)
    print("Silver columns:", len(silver_df.columns))
    print("Silver output:", silver_path)

    print("\n--- SILVER DATA TYPES ---")

    silver_df.printSchema()

finally:
    
    spark.stop()