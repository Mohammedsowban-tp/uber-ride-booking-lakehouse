      #1.REVENUE ANALYSIS

# TOATAL REVENUE FROM COMPLETED RIDES
SELECT
    SUM(f.booking_value) as total_revenue
FROM fact_rides f 
JOIN dim_booking_status s 
    ON f.booking_status_id = s.booking_status_id
WHERE s.booking_status = 'Completed';

#Revenue by vehicle type.
SELECT
    v.vehicle_type,
    SUM(f.booking_value) AS total_revenue
FROM fact_rides f
JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id
JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id
WHERE s.booking_status = 'Completed'
GROUP BY
    v.vehicle_type
ORDER BY
    total_revenue DESC;
  
#Which vehicle type generates the highest average booking value?
SELECT
    v.vehicle_type,
    AVG(f.booking_value) as avg_revenue
from fact_rides f 

join dim_booking_status s 
    on f.booking_status_id = s.booking_status_id

join dim_vehicle v 
    on f.vehicle_id = v.vehicle_id

WHERE s.booking_status = "completed"
GROUP BY
     v.vehicle_type

ORDER BY
    avg_revenue DESC

#Overall Average Booking Value

SELECT
    AVG(f.booking_value) AS avg_booking_value

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status = 'Completed';

SELECT *
FROM dim_date
LIMIT 5;


#Monthly Revenue Trends
SELECT
    d.year,
    d.month,
    SUM(f.booking_value) AS total_revenue

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

JOIN dim_date d
    ON f.date_id = d.date_id

WHERE s.booking_status = 'Completed'

GROUP BY
    d.year,
    d.month

ORDER BY
    d.year,
    d.month;


         #2.BOOKING ANALYSIS

#Total Bookings

SELECT
    COUNT(ride_id) AS total_bookings
FROM fact_rides;

#Completed Bookings

SELECT
    COUNT(f.ride_id) AS completed_bookings

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status = 'Completed';

#Booking Cancellation Rate

SELECT
    COUNT(f.ride_id) AS cancelled_bookings,

    ROUND(
        COUNT(f.ride_id) * 100.0 /
        (SELECT COUNT(*) FROM fact_rides),
        2
    ) AS cancellation_rate

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status in(
    'Cancelled By Driver',
    'Cancelled By Customer'
)

#Customer Cancellation Analysis

SELECT
    COUNT(f.ride_id) AS customer_cancelled_bookings,

    ROUND(
        COUNT(f.ride_id) * 100.0 /
        (SELECT COUNT(*) FROM fact_rides),
        2
    ) AS customer_cancellation_rate

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status = 'Cancelled By Customer';

#Driver Cancellation Analysis

SELECT
    COUNT(f.ride_id) AS driver_cancelled_bookings,

    ROUND(
        COUNT(f.ride_id) * 100.0 /
        (SELECT COUNT(*) FROM fact_rides),
        2
    ) AS driver_cancellation_rate

FROM fact_rides f

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status ="Cancelled By Driver"


             #3.CUSTOMER ANALYSIS

#Unique Customers

SELECT
    COUNT(DISTINCT customer_id) AS total_unique_customers

FROM fact_rides;

#Top 10 Customers by Booking Count

SELECT
    customer_id,
    COUNT(ride_id) AS total_bookings

FROM fact_rides

GROUP BY
    customer_id

ORDER BY
    total_bookings DESC

LIMIT 10;

#Average Customer Rating

SELECT
    ROUND(AVG(customer_rating), 2) AS avg_customer_rating

FROM fact_rides

WHERE customer_rating IS NOT NULL;

#Highest Customer Booking Value

SELECT
    customer_id,
    SUM(booking_value) AS total_customer_value

FROM fact_rides

WHERE booking_status_id IN (
    SELECT booking_status_id
    FROM dim_booking_status
    WHERE booking_status = 'Completed'
)

GROUP BY customer_id

ORDER BY total_customer_value DESC

LIMIT 10;


#Customer Rating Distribution

SELECT
    customer_rating,
    COUNT(*) AS total_ratings

FROM fact_rides

WHERE customer_rating IS NOT NULL

GROUP BY customer_rating

ORDER BY customer_rating DESC;


            #4 – Vehicle & Location Analysis

#Most Booked Vehicle Types
SELECT
    v.vehicle_type,
    COUNT(f.ride_id) AS total_bookings

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

GROUP BY
    v.vehicle_type

ORDER BY
    total_bookings DESC;

#Top 10 Pickup Locations

SELECT
    p.location_name AS pickup_location,
    COUNT(f.ride_id) AS total_bookings

FROM fact_rides f

JOIN dim_location p
    ON f.pickup_location_id = p.location_id

GROUP BY
    p.location_name

ORDER BY
    total_bookings DESC

LIMIT 10;

#Top 10 Drop Locations

SELECT
    d.location_name AS drop_location,
    COUNT(f.ride_id) AS total_dropoffs

FROM fact_rides f

JOIN dim_location d
    ON f.drop_location_id = d.location_id

GROUP BY
    d.location_name

ORDER BY
    total_dropoffs DESC

LIMIT 10;


#Revenue by Pickup Location

SELECT
    p.location_name AS pickup_location,
    SUM(f.booking_value) AS total_revenue

FROM fact_rides f

JOIN dim_location p
    ON f.pickup_location_id = p.location_id

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status = 'Completed'

GROUP BY
    p.location_name

ORDER BY
    total_revenue DESC

LIMIT 10;

#Average Ride Distance by Vehicle Type

SELECT
    v.vehicle_type,
    ROUND(AVG(f.ride_distance), 2) AS avg_ride_distance

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

WHERE s.booking_status = 'Completed'

GROUP BY
    v.vehicle_type

ORDER BY
    avg_ride_distance DESC;


                  #5.OPERATIONAL ANALYSIS

#Average VTAT by Vehicle Type

SELECT
    v.vehicle_type,
    ROUND(AVG(f.Avg_VTAT), 2) AS avg_vtat

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

WHERE f.Avg_VTAT IS NOT NULL

GROUP BY
    v.vehicle_type

ORDER BY
    avg_vtat ASC;

#Average CTAT by Vehicle Type

SELECT
    v.vehicle_type,
    ROUND(AVG(f.avg_ctat), 2) AS avg_ctat

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

WHERE f.avg_ctat IS NOT NULL

GROUP BY
    v.vehicle_type

ORDER BY
    avg_ctat ASC;

#Peak Booking Hours

SELECT
    HOUR(f.time) AS booking_hour,
    COUNT(f.ride_id) AS total_bookings

FROM fact_rides f

GROUP BY
    HOUR(f.time)

ORDER BY
    total_bookings DESC;

#Vehicle Cancellation Rate

SELECT
    v.vehicle_type,

    COUNT(f.ride_id) AS total_bookings,

    SUM(
        CASE
            WHEN s.booking_status IN (
                'Cancelled By Driver',
                'Cancelled By Customer'
            )
            THEN 1
            ELSE 0
        END
    ) AS cancelled_bookings,

    ROUND(
        SUM(
            CASE
                WHEN s.booking_status IN (
                    'Cancelled By Driver',
                    'Cancelled By Customer'
                )
                THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(f.ride_id),
        2
    ) AS cancellation_rate

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

JOIN dim_booking_status s
    ON f.booking_status_id = s.booking_status_id

GROUP BY
    v.vehicle_type

ORDER BY
    cancellation_rate DESC;

#Average Driver Rating by Vehicle Type

SELECT
    v.vehicle_type,
    ROUND(AVG(f.driver_ratings), 2) AS avg_driver_rating

FROM fact_rides f

JOIN dim_vehicle v
    ON f.vehicle_id = v.vehicle_id

WHERE f.driver_ratings IS NOT NULL

GROUP BY
    v.vehicle_type

ORDER BY
    avg_driver_rating DESC;




























