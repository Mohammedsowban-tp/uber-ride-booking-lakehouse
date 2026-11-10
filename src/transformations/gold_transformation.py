import os
import sys
from numpy import select
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

builder = (
    SparkSession.builder.appName("GoldTransformation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver_path = "data/silver/ride_bookings"
gold_path = "data/gold/booking_performance"


silver_df = spark.read.format("delta").load(silver_path)

silver_df.show(5)

# Register Silver DataFrame as SQL Temporary View
silver_df.createOrReplaceTempView("ride_bookings")

result = spark.sql("""
SELECT COUNT(*) AS total_bookings
FROM ride_bookings
""")

result.show()

# dimension data analysis

dimension_analysis = spark.sql("""
    SELECT
        COUNT(DISTINCT Customer_ID) AS unique_customers,
        COUNT(DISTINCT Vehicle_Type) AS unique_vehicles,
        COUNT(DISTINCT Pickup_Location) AS unique_pickup_locations,
        COUNT(DISTINCT Drop_Location) AS unique_drop_locations,
        COUNT(DISTINCT Date) AS unique_dates
    FROM ride_bookings
""")

dimension_analysis.show()

#create customer dimension table
dim_customer = spark.sql("""
    SELECT DISTINCT
    trim(both '"' FROM Customer_ID) as customer_id
    FROM ride_bookings
    where Customer_ID is not null
""")

dim_customer.show(10)
print("Total unique customers:", dim_customer.count())

#create vehicle dimension table
dim_vehicle = spark.sql("""
    SELECT DISTINCT
    Vehicle_Type as vehicle_type
    FROM ride_bookings
    where Vehicle_Type is not null
""")

dim_vehicle.show()

print("Total unique vehicle types:", dim_vehicle.count())

#create location dimension table
dim_location = spark.sql("""
    SELECT DISTINCT 
        location_name
    FROM (
        SELECT Pickup_Location AS location_name FROM ride_bookings
        UNION
        SELECT Drop_Location AS location_name FROM ride_bookings
    )
    WHERE location_name IS NOT NULL
""")

dim_location.show()

print("Total unique locations:", dim_location.count())

#create date dimension table
dim_date = spark.sql("""
    SELECT DISTINCT
        cast(date_format(Date, 'yyyyMMdd') as int) as date_id,
        Date as full_date,
        DAY(Date) as day,
        MONTH(Date) as month,
        QUARTER(Date) as quarter,
        YEAR(Date) as year

    FROM ride_bookings
    WHERE Date IS NOT NULL
""")

dim_date.show(10)

print("Total unique dates:", dim_date.count())

# create payment method dimension table

dim_payment_method = spark.sql("""
    SELECT DISTINCT
        Payment_Method as payment_method
    FROM ride_bookings
    WHERE Payment_Method IS NOT NULL
""")

dim_payment_method.show()

print("Total unique payment methods:", dim_payment_method.count())

#create booking status dimension table
dim_booking_status = spark.sql("""
    SELECT DISTINCT
        Booking_Status as booking_status
    FROM ride_bookings
    WHERE Booking_Status IS NOT NULL
""")

dim_booking_status.show()

print("Total unique booking statuses:", dim_booking_status.count())

# Add surrogate key to vehicle dimension table

from pyspark.sql.window import Window

vehicle_window = Window.orderBy("vehicle_type")

dim_vehicle = dim_vehicle.withColumn(
    "vehicle_id", 
    F.row_number().over(vehicle_window)) 

dim_vehicle.show()

# Add surrogate key to location dimension table

location_window = Window.orderBy("location_name")

dim_location = dim_location.withColumn(
    "location_id",
    F.row_number().over(location_window))

dim_location.show(10)

print("Total unique locations with surrogate keys:", dim_location.count())

# Add surrogate key to payment method dimension table

payment_window = Window.orderBy("payment_method")

dim_payment_method = dim_payment_method.withColumn(
    "payment_method_id",
    F.row_number().over(payment_window)
)

dim_payment_method.show()

# Add Unknown payment method

unknown_payment = spark.createDataFrame(
    [(0, "Unknown")],
    ["payment_method_id", "payment_method"]
)

dim_payment_method = dim_payment_method.unionByName(unknown_payment)

dim_payment_method.show()

#Add surrogate key to booking status dimension table

status_window = Window.orderBy("booking_status")

dim_booking_status = dim_booking_status.withColumn(
    "booking_status_id",
    F.row_number().over(status_window)
)

dim_booking_status.show()

# save dimension tables

print("\n--- SAVING DIMENSION TABLES ---")

dim_customer.write.format("delta").mode("overwrite").save("data/gold/dim_customer")
dim_vehicle.write.format("delta").mode("overwrite").save("data/gold/dim_vehicle")
dim_location.write.format("delta").mode("overwrite").save("data/gold/dim_location")
dim_date.write.format("delta").mode("overwrite").save("data/gold/dim_date")
dim_payment_method.write.format("delta").mode("overwrite").save("data/gold/dim_payment_method")
dim_booking_status.write.format("delta").mode("overwrite").save("data/gold/dim_booking_status")

print("Dimension tables saved successfully")

# build fact table

print("\n--- BUILDING FACT TABLE ---")

fact_source = silver_df.withColumn(
    "customer_id",
     F.trim(F.regexp_replace(F.col("Customer_ID"),'"',""))
).withColumn(
    "booking_id",
    F.trim(F.regexp_replace(F.col("Booking_ID"),'"',""))
).withColumn(
    "date_id",
    F.date_format(F.col("Date"), "yyyyMMdd").cast("int")
)

print("fact source records:", fact_source.count())

fact_source.show(5)

# join customer dimension

print("\n--- JOINING CUSTOMER DIMENSION ---")

#check unmatched customer 

unmatched_customers = fact_source.join(
    dim_customer,
    on="customer_id",
    how="left_anti"
).count()

print("Unmatched customer records:", unmatched_customers)

#perform left join

fact_with_customer = fact_source.join(
    dim_customer,
    on="customer_id",
    how="left"  
)

print("records after customer join:", fact_with_customer.count())

# join vehicle dimension

print("\n--- JOINING VEHICLE DIMENSION ---")

# check unmatched vehicle types

unmatched_vehicles = fact_with_customer.join(
    dim_vehicle,    
    on="Vehicle_Type",
    how="left_anti"
).count()

print("Unmatched vehicle records:", unmatched_vehicles)

# perform left join
fact_with_vehicle = fact_with_customer.join(
    dim_vehicle,
    on="Vehicle_Type",
    how="left"
)

print("records after vehicle join:", fact_with_vehicle.count())

# verify vehicle_ids
fact_with_vehicle.select("Vehicle_Type", "vehicle_id").show(10)

# join pickup location dimension

print("\n--- JOINING PICKUP LOCATION ---")

# check unmatched pickup locations
unmatched_pickup_locations = fact_with_vehicle.join(
    dim_location,
    fact_with_vehicle["Pickup_Location"] == dim_location["location_name"],
    how="left_anti"
).count()

print("Unmatched pickup location records:", unmatched_pickup_locations)

# perform pickup location join
fact_with_pickup = fact_with_vehicle.join(
    dim_location,
    fact_with_vehicle["Pickup_Location"] == dim_location["location_name"],
    "left"
).withColumn(
    "pickup_location_id", F.col("location_id")
).drop("location_id", "location_name")

print("records after pickup location join:", fact_with_pickup.count())

# join drop location dimension

print("\n--- JOINING DROP LOCATION ---")

# check unmatched drop locations

unmatched_drop_locations = fact_with_pickup.join(
    dim_location,
    fact_with_pickup["Drop_Location"] == dim_location["location_name"],
    "left_anti"
).count()

print("Unmatched drop location records:", unmatched_drop_locations)

# perform drop location join
fact_with_drop = fact_with_pickup.join(
    dim_location,
    fact_with_pickup["Drop_Location"] == dim_location["location_name"],
    "left"
).withColumn(
    "drop_location_id", F.col("location_id")
).drop("location_id", "location_name")

print("records after drop location join:", fact_with_drop.count())

# verify pickup and drop location ids
fact_with_drop.select(
    "Pickup_Location",
    "pickup_location_id", 
    "Drop_Location",
    "drop_location_id"
).show(10)

# join date dimension

print("\n--- JOINING DATE DIMENSION ---")

# check unmatched dates

unmatched_dates = fact_with_drop.join(
    dim_date,
    on="date_id",
    how=
    "left_anti"
).count()

print("Unmatched date records:", unmatched_dates)

# perform date join
fact_with_date = fact_with_drop.join(
    dim_date,
    on="date_id",
    how="left"
)

print("records after date join:", fact_with_date.count())

# verify date ids
fact_with_date.select(
    "Date",
    "date_id",
    "full_date",
    "day",
    "month",
    "quarter",
    "year"
).show(10)

# join payment method dimension

print("\n--- JOINING PAYMENT METHOD DIMENSION ---")

# check unmatched payment methods
unmatched_payment_methods = fact_with_date.join(
    dim_payment_method,
    fact_with_date["Payment_Method"] == dim_payment_method["payment_method"],
    "left_anti"
).filter(
    F.col("Payment_Method").isNotNull()
).count()

print("Unmatched payment method records:", unmatched_payment_methods)

# perform payment method join
fact_with_payment = fact_with_date.alias("f").join(
    dim_payment_method.alias("p"),
    F.col("f.Payment_Method") == F.col("p.payment_method"),
    "left"
).select(
    "f.*",
    F.coalesce(F.col("p.payment_method_id"),
               F.lit(0)
               ).alias("payment_method_id")
)

print("records after payment method join:", fact_with_payment.count())

# verify payment method ids
fact_with_payment.select(
    "Payment_Method",
    "payment_method_id"
).show(10)      


# join booking status dimension
print("\n--- JOINING BOOKING STATUS DIMENSION ---")

# check unmatched booking statuses
unmatched_booking_statuses = fact_with_payment.join(
    dim_booking_status,
    fact_with_payment["Booking_Status"] == dim_booking_status["booking_status"],
    "left_anti"
).count()


print("Unmatched booking status records:",unmatched_booking_statuses)


# Perform booking status join

fact_with_status = (
    fact_with_payment.alias("f")
    .join(
        dim_booking_status.alias("s"),
        F.col("f.Booking_Status") == F.col("s.booking_status"),
        "left"
    )
    .select(
        "f.*",
        F.col("s.booking_status_id").alias("booking_status_id")
    )
)

print("Records after booking status join:", fact_with_status.count())

# Verify booking status IDs
fact_with_status.select(
    "Booking_Status",
    "booking_status_id"
).show(10)

# generate ride id

print("\n--- GENERATING RIDE ID ---")

ride_window = Window.orderBy(F.monotonically_increasing_id())

fact_with_ride_id = fact_with_status.withColumn(
    "ride_id",
    F.row_number().over(ride_window)
)

print("Records after generating ride ID:", fact_with_ride_id.count())

fact_with_ride_id.select(
    "ride_id",
    "Booking_ID",
    "customer_id",
    "booking_status_id",
    "vehicle_id"
).show(10)

# validate ride IDs

print("\n--- VALIDATING RIDE ID ---")

# Total records in fact table
total_records = fact_with_ride_id.count()

#Unique ride IDs
unique_ride_ids = fact_with_ride_id.select(
    "ride_id"
    ).distinct().count()

# Check for null ride IDs
null_ride_ids = fact_with_ride_id.filter(
    F.col("ride_id").isNull()
).count()

print("Total records in fact table:", total_records)
print("Unique ride IDs:", unique_ride_ids)  
print("Null ride IDs:", null_ride_ids)

if total_records == unique_ride_ids and null_ride_ids == 0:
    print("Ride ID validation passed.")
else:
    print("Ride ID validation failed.")


# Review the final fact table columns 
print("\n--- FINAL FACT TABLE COLUMNS ---")

print("Total columns :", len(fact_with_ride_id.columns))

for column in fact_with_ride_id.columns:
    print(column)


# creating final fact table 

print("\n--- CREATING FINAL FACT TABLE ---")

fact_rides = fact_with_ride_id.select(
    "ride_id",
    "booking_id",
    "customer_id",
    "vehicle_id",
    "pickup_location_id",
    "drop_location_id",
    "date_id",
    "booking_status_id",
    "Time",
    "payment_method_id",
    "Avg_VTAT",
    "Avg_CTAT",
    "Cancelled_Rides_by_Customer",
    "Reason_for_cancelling_by_Customer",
    "Cancelled_Rides_by_Driver",
    "Driver_Cancellation_Reason",
    "Incomplete_Rides",
    "Incomplete_Rides_Reason",
    "Booking_Value",
    "Ride_Distance",
    "Driver_Ratings",
    "Customer_Rating"
)

print("Total records:", fact_rides.count())
print("Total columns:", len(fact_rides.columns))

fact_rides.show(10)


#save fact table

print("\n--- SAVING FACT RIDES ---")

fact_rides.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/gold/fact_rides")

print("Fact rides saved successfully")

#verify saved fact table

print("\n--- VERIFYING SAVED FACT RIDES ---")

saved_fact_rides = spark.read.format("delta").load("data/gold/fact_rides")
print("Verified saved fact rides:", saved_fact_rides.count())

print("Total columns in saved fact rides:", len(saved_fact_rides.columns))

saved_fact_rides.show(10)

