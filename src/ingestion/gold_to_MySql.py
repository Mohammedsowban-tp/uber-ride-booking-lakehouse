import os
import sys

from dotenv import load_dotenv

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

#project configuration

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")

)

# load environment variables
load_dotenv(os.path.join(PROJECT_ROOT,".env"))


JDBC_JAR = os.path.join(
    PROJECT_ROOT,
    "jars",
    "mysql-connector-j-26.10.0.jar")


# build spark session

builder = (
    SparkSession.builder
    .appName("GoldToMySQL")
    .master("local[*]")
    .config("spark.jars", JDBC_JAR)
    .config(
         "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Spark session created successfully!")


#mysql connection configuration

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT","3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

#test the connection

connection_test = (
    spark.read
    .format("jdbc")
    .option("url",JDBC_URL)
    .option("query","SELECT VERSION() AS mysql_version")
    .option("user",MYSQL_USER)
    .option("password",MYSQL_PASSWORD)
    .option("driver","com.mysql.cj.jdbc.Driver")
    .load()
)

connection_test.show()

print("mysql connection successfull")

#transfer tables to mysql
gold_tables = {
    "dim_customer": "data/gold/dim_customer",
    "dim_vehicle": "data/gold/dim_vehicle",
    "dim_location": "data/gold/dim_location",
    "dim_date": "data/gold/dim_date",
    "dim_payment_method": "data/gold/dim_payment_method",
    "dim_booking_status": "data/gold/dim_booking_status",
    "fact_rides": "data/gold/fact_rides"
}

#use function for reuse the code

def write_to_mysql(df, table_name):

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

    print(f"{table_name} transferred successfully!")

#transfering

for table_name, table_path in gold_tables.items():

    df = spark.read.format("delta").load(
        os.path.join(PROJECT_ROOT, table_path)
    )

    write_to_mysql(df, table_name)


spark.stop()

print("Gold to MySQL transfer completed!")




