from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


project_root = Path(__file__).resolve().parents[2]
silver_path = project_root / "data" / "silver" / "ride_bookings"

spark =( SparkSession.builder.appName("SilverQualityCheck")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0"
        )
        .getOrCreate()
)

try:
    # Read the silver data
    df = spark.read.format("delta").load(str(silver_path))

    print("\n--- SILVER DATA ---")
    print("Number of rows:", df.count())
    print("Number of columns:", len(df.columns))


    print("\ncolumn data types:")
    df.printSchema()

    #null values in every column
    null_counts = df.select(
        *[
            F.count(
                F.when(F.col(column).isNull(), 1)
            ).alias(column)
            for column in df.columns
        ]
    )

    print("\n--- NULL VALUES BY COLUMN ---")
    null_counts.show(truncate=False,vertical=True)

    #count blank strings in string columns
    string_columns = [
        field.name
        for field in df.schema.fields 
        if field.dataType.simpleString() == "string"
    ]

    blank_string_counts = df.select(
        *[
            F.count(
                F.when(
                    F.trim(F.col(column)) == "", 1)
            ).alias(column)
            for column in string_columns
        ]
    )

    print("\n--- BLANK STRINGS BY COLUMN ---")
    blank_string_counts.show(truncate=False,vertical=True)

finally:
    spark.stop()