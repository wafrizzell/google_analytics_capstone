/*
     Cyclistic Bike-Share Analysis for Google Data Analytics Certificate Capstone
     by William Frizzell-Carlton
     20 OCT 2022
*/
 
 -- Create Schema for Cyclistic Bike-Share
 CREATE SCHEMA cyclistic CHARSET utf8mb4;
 
 -- Set cyclistic to default database schema 
 USE cyclistic;
 
 -- Create table to import CSV data into
 -- 12 CSV files of bike-share ride data at ride level per month
 DROP TABLE IF EXISTS rides;
 CREATE TABLE rides (
	ride_id VARCHAR(16),
    rideable_type VARCHAR(13),
    started_at DATETIME,
    ended_at DATETIME,
    start_station_name VARCHAR(65) DEFAULT NULL,
    start_station_id VARCHAR(45) DEFAULT NULL,
    end_station_name VARCHAR(65) DEFAULT NULL,
    end_station_id VARCHAR(45) DEFAULT NULL,
    start_lat DECIMAL(10,8),
    start_lng DECIMAL(10,8),
    end_lat DECIMAL(10,8),
    end_lng DECIMAL(10,8),
    member_casual VARCHAR(6),
    PRIMARY KEY (ride_id)
);

-- Count number of records after importing CSV files
SELECT COUNT(*) FROM rides;
-- 5,883,043 bike-share records matches CSV count - import sussessful

-- Check for distinct values
SELECT DISTINCT rideable_type
FROM rides;
-- classic_bike, electric_bike, docked_bike

SELECT DISTINCT member_casual
FROM rides;
-- member, casual

-- Basic statistical analysis of ride times, mean, min, and max
SELECT
	YEAR(started_at) AS yr,
    MONTH(started_at) AS mon,
	AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg,
    MIN(TIMEDIFF(ended_at,started_at)) AS min,
    MAX(TIMEDIFF(ended_at,started_at)) AS max
FROM rides
GROUP BY
	yr,
    mon
ORDER BY
	yr,
    mon
;

-- Setting the minimum and maximum ride time between 1 minute and 24 hours
-- How many records would this affect?
-- Count the number of records under 1 minute
SELECT
	COUNT(CASE WHEN TIMESTAMPDIFF(MINUTE,started_at,ended_at) < 1 THEN ride_id ELSE NULL END) AS count_under_min
FROM rides;
-- 110,712 rides under 1 minute

-- Count number of records greater than or equal to 24 hours
SELECT
	COUNT(CASE WHEN TIMESTAMPDIFF(HOUR,started_at,ended_at) >= 24 THEN ride_id ELSE NULL END) AS count_over_max
FROM rides;
-- 5185 rides greater than 24 hours
-- Total 115,897 records or 1.97% of total records. Remove these records analysis

-- TURN OFF AUTOCOMMIT, VERIFY DELETE RESULTS THEN COMMIT
SET AUTOCOMMIT = 0;
SELECT @@autocommit;

-- Delete rows that do not fit in our ride definition
DELETE FROM rides
WHERE TIMESTAMPDIFF(MINUTE,started_at,ended_at) < 1;

DELETE FROM rides
WHERE TIMESTAMPDIFF(HOUR,started_at,ended_at) >= 24;

-- Count total records after deletion
SELECT COUNT(*) FROM rides;
-- 5,767,146 records which is 5,883,043 - 115,897

-- Recalulate the mean, min, and max
SELECT
	YEAR(started_at) AS yr,
    MONTH(started_at) AS mon,
	AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg,
    MIN(TIMEDIFF(ended_at,started_at)) AS min,
    MAX(TIMEDIFF(ended_at,started_at)) AS max
FROM rides
GROUP BY
	yr,
    mon
ORDER BY
	yr,
    mon
;
-- Min and Max values in proper ranges. Verified delete, now commit
COMMIT;
SET AUTOCOMMIT = 1;
SELECT @@autocommit;


-- Query to return bike-share rides as daily statistics
SELECT
	DATE(started_at),
    CASE
		WHEN WEEKDAY(started_at) = 0 THEN 'Mon'
        WHEN WEEKDAY(started_at) = 1 THEN 'Tue'
        WHEN WEEKDAY(started_at) = 2 THEN 'Wed'
        WHEN WEEKDAY(started_at) = 3 THEN 'Thu'
        WHEN WEEKDAY(started_at) = 4 THEN 'Fri'
        WHEN WEEKDAY(started_at) = 5 THEN 'Sat'
        WHEN WEEKDAY(started_at) = 6 THEN 'Sun'
        ELSE 'Oops, double check logic'
	END AS day_of_week,
    CASE WHEN WEEKDAY(started_at) IN (5,6) THEN '1' ELSE '0' END AS is_weekend,
    AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg_ride_length,
	AVG(CASE WHEN member_casual = 'member' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_member,
    AVG(CASE WHEN member_casual = 'casual' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_casual,
    COUNT(CASE WHEN member_casual = 'member' THEN ride_id ELSE NULL END) AS member_rides,
    COUNT(CASE WHEN member_casual = 'casual' THEN ride_id ELSE NULL END) AS casual_rides,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_member,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_casual
FROM rides
GROUP BY
	DATE(started_at),
    day_of_week,
    is_weekend
ORDER BY
	DATE(started_at)
;

-- Query to return bike-share rides as weekly statistics
SELECT
	MIN(DATE(started_at)) AS week_of,
    AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg_ride_length,
	AVG(CASE WHEN member_casual = 'member' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_member,
    AVG(CASE WHEN member_casual = 'casual' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_casual,
    COUNT(CASE WHEN member_casual = 'member' THEN ride_id ELSE NULL END) AS member_rides,
    COUNT(CASE WHEN member_casual = 'casual' THEN ride_id ELSE NULL END) AS casual_rides,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_member,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_casual
FROM rides
GROUP BY
	YEARWEEK(started_at)
ORDER BY
	YEARWEEK(started_at)
; 
 
 
-- Query to return bike-share rides as monthly statistics
SELECT
	YEAR(started_at) AS year,
    MONTH(started_at) AS month,
    AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg_ride_length,
	AVG(CASE WHEN member_casual = 'member' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_member,
    AVG(CASE WHEN member_casual = 'casual' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_casual,
    COUNT(CASE WHEN member_casual = 'member' THEN ride_id ELSE NULL END) AS member_rides,
    COUNT(CASE WHEN member_casual = 'casual' THEN ride_id ELSE NULL END) AS casual_rides,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_member,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_casual
FROM rides
GROUP BY
	year,
    month
ORDER BY
	year,
    month
; 

-- Query to return bike-share rides as hourly statistics
SELECT
	HOUR(started_at) AS hour,
    AVG(TIMESTAMPDIFF(SECOND,started_at,ended_at))/60 AS avg_ride_length,
	AVG(CASE WHEN member_casual = 'member' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_member,
    AVG(CASE WHEN member_casual = 'casual' THEN TIMESTAMPDIFF(SECOND,started_at,ended_at) ELSE NULL END)/60 AS avg_ride_length_casual,
    COUNT(CASE WHEN member_casual = 'member' THEN ride_id ELSE NULL END) AS member_rides,
    COUNT(CASE WHEN member_casual = 'casual' THEN ride_id ELSE NULL END) AS casual_rides,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_member,
    COUNT(CASE WHEN member_casual = 'member' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_member,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'electric_bike' THEN ride_id ELSE NULL END) AS electric_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'classic_bike' THEN ride_id ELSE NULL END) AS classic_casual,
    COUNT(CASE WHEN member_casual = 'casual' AND rideable_type = 'docked_bike' THEN ride_id ELSE NULL END) AS docked_casual
FROM rides
GROUP BY
	hour
ORDER BY
	hour
; 

-- Find TOP start and end stations for member vs casual riders
-- Top 5 member start stations
SELECT
	start_station_name AS top_5_member_stations,
    start_lat,
    start_lng,
    COUNT(start_station_name) AS station_count
FROM rides
WHERE
	member_casual = 'member'
	AND NOT start_station_name = ''
GROUP BY 
	start_station_name,
    start_lat,
    start_lng
ORDER BY station_count DESC
LIMIT 5;

-- Top 5 member end stations
SELECT
	end_station_name AS top_5_member_stations,
    end_lat,
    end_lng,
    COUNT(start_station_name) AS station_count
FROM rides
WHERE
	member_casual = 'member'
	AND NOT end_station_name = ''
GROUP BY 
	end_station_name,
    end_lat,
    end_lng
ORDER BY station_count DESC
LIMIT 5;

-- Top 5 casual start stations
SELECT
	start_station_name AS top_5_casual_stations,
    start_lat,
    start_lng,
    COUNT(start_station_name) AS station_count
FROM rides
WHERE
	member_casual = 'casual'
	AND NOT start_station_name = ''
GROUP BY 
	start_station_name,
    start_lat,
    start_lng
ORDER BY station_count DESC
LIMIT 5;

-- Top 5 casual end stations
SELECT
	end_station_name AS top_5_casual_stations,
    end_lat,
    end_lng,
    COUNT(start_station_name) AS station_count
FROM rides
WHERE
	member_casual = 'casual'
	AND NOT end_station_name = ''
GROUP BY 
	end_station_name,
    end_lat,
    end_lng
ORDER BY station_count DESC
LIMIT 5;