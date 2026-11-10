show TABLES


SELECT 'dim_customer' AS table_name, COUNT(*) AS total_rows
FROM dim_customer

UNION ALL

SELECT 'dim_vehicle', COUNT(*) FROM dim_vehicle

UNION ALL

SELECT 'dim_location', COUNT(*) FROM dim_location

UNION ALL

SELECT 'dim_date', COUNT(*) FROM dim_date

UNION ALL

SELECT 'dim_payment_method', COUNT(*) FROM dim_payment_method

UNION ALL

SELECT 'dim_booking_status', COUNT(*) FROM dim_booking_status

UNION ALL

SELECT 'fact_rides', COUNT(*) FROM fact_rides;


USE uber_analytics;

-- FACT TABLE REFERENTIAL INTEGRITY CHECK
--unmatched

SELECT
    'Customer' AS dimension_name,
    COUNT(*) AS unmatched_records
FROM fact_rides f
LEFT JOIN dim_customer d
    ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL

UNION ALL

SELECT
    'Vehicle',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_vehicle d
    ON f.vehicle_id = d.vehicle_id
WHERE d.vehicle_id IS NULL

UNION ALL

SELECT
    'Pickup Location',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_location d
    ON f.pickup_location_id = d.location_id
WHERE d.location_id IS NULL

UNION ALL

SELECT
    'Drop Location',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_location d
    ON f.drop_location_id = d.location_id
WHERE d.location_id IS NULL

UNION ALL

SELECT
    'Date',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_date d
    ON f.date_id = d.date_id
WHERE d.date_id IS NULL

UNION ALL

SELECT
    'Payment Method',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_payment_method d
    ON f.payment_method_id = d.payment_method_id
WHERE d.payment_method_id IS NULL

UNION ALL

SELECT
    'Booking Status',
    COUNT(*)
FROM fact_rides f
LEFT JOIN dim_booking_status d
    ON f.booking_status_id = d.booking_status_id
WHERE d.booking_status_id IS NULL;


#duplicate ride ids
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT ride_id) AS unique_ride_ids,
    COUNT(*) - COUNT(DISTINCT ride_id) AS duplicates
FROM fact_rides;

#missing values

SELECT
    SUM(ride_id IS NULL) AS missing_ride_id,
    SUM(customer_id IS NULL) AS missing_customer_id,
    SUM(vehicle_id IS NULL) AS missing_vehicle_id,
    SUM(pickup_location_id IS NULL) AS missing_pickup,
    SUM(drop_location_id IS NULL) AS missing_drop,
    SUM(date_id IS NULL) AS missing_date,
    SUM(booking_status_id IS NULL) AS missing_status
FROM fact_rides;

#Invalid booking values

SELECT
    SUM(Booking_Value < 0) AS negative_booking_values,
    SUM(Ride_Distance < 0) AS negative_ride_distances
FROM fact_rides;

#Booking status distribution

SELECT
    s.booking_status,
    COUNT(*) AS total_bookings
FROM fact_rides f
JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id
GROUP BY s.booking_status
ORDER BY total_bookings DESC;


#Payment method distribution

SELECT
    p.payment_method,
    COUNT(*) AS total_bookings
FROM fact_rides f
JOIN dim_payment_method p
    ON f.payment_method_id = p.payment_method_id
GROUP BY p.payment_method
ORDER BY total_bookings DESC;

