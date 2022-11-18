-- Created the tables and copied the dataset from my downloaded CSV file
-- the query was repeated for the rest of the 11 months analysed.

CREATE TABLE tripdata_2021 (
ride_id varchar(20),
	rideable_type varchar(20),
	started_at timestamp,
	ended_at timestamp,
	start_station_name varchar(100),
	start_station_id varchar(50),
	end_station_name varchar(100),
	end_station_id varchar(50),
	start_lat double precision,
	start_lng double precision,
	end_lat double precision,
	end_lng double precision,
	member_casual varchar (6)
);

COPY tripdata_202111
FROM 'C:\Users\shimo\Desktop\New folder\GDA\Google Data Analytics\8 Capstone\Bicycle Sharing\Files Unzipped CSV\202111-divvy-tripdata.csv'
WITH (FORMAT CSV, HEADER);

SELECT *
FROM tripdata_202111;

--Created a new table using likeness of one of the existing tables 
--and created its dataset by the union of all the entire 12 months

CREATE TABLE trip_data (LIKE tripdata_202109);
INSERT INTO trip_data
SELECT *
FROM tripdata_202109
UNION ALL
SELECT *
FROM tripdata_202110
UNION ALL
SELECT *
FROM tripdata_202111
UNION ALL
SELECT *
FROM tripdata_202112
UNION ALL
SELECT *
FROM tripdata_202201
UNION ALL
SELECT *
FROM tripdata_202202
UNION ALL
SELECT *
FROM tripdata_202203
UNION ALL
SELECT *
FROM tripdata_202204
UNION ALL
SELECT *
FROM tripdata_202205
UNION ALL
SELECT *
FROM tripdata_202206
UNION ALL
SELECT *
FROM tripdata_202207
UNION ALL
SELECT *
FROM tripdata_202208;

SELECT *
FROM trip_data;

--created a copy of my joined table
CREATE TABLE trip_data_copy (LIKE trip_data );
INSERT INTO trip_data_copy
SELECT *
FROM trip_data;

--removed unnecessary columns
ALTER TABLE trip_data
DROP COLUMN start_station_name,
DROP COLUMN  start_station_id,
DROP COLUMN  end_station_name, 
DROP COLUMN  end_station_id, 
DROP COLUMN start_lat,
DROP COLUMN end_lat,
DROP COLUMN end_lng;
ALTER TABLE trip_data
DROP COLUMN start_lng;

-- Added new column for ride_length and updated the new column as the difference
--between start and end times
ALTER TABLE trip_data
ADD COLUMN ride_length interval;

UPDATE trip_data
SET ride_length = ended_at - started_at;

--created index for easier querying

CREATE INDEX ended_at_idx ON trip_data (ended_at);
CREATE INDEX started_at_idx ON trip_data (started_at);
CREATE INDEX ride_length_idx ON trip_data (ride_length);

--Found the difference between the start and end times of the ride
--discovered negative values in ride length which are bad data

SELECT ride_id, 
rideable_type,
started_at,
ended_at,
CAST (ended_at - started_at AS interval) AS ride_length
FROM all_combined_ridedata

ORDER BY ride_length;

--Deleted where trip_data = 0
DELETE FROM trip_data
WHERE ended_at = started_at;


--Deleted where trip_data is negative
DELETE FROM trip_data
WHERE ended_at < started_at
RETURNING *;

--Create new column week_day to calculate the week day each ride started
--From week_day column, 0 = sunday, 1 = monday, etc.
ALTER TABLE trip_data
ADD COLUMN week_day integer;

UPDATE trip_data
SET week_day = EXTRACT (DOW FROM started_at
);

-- Found the overall mean, max, mode and count of the ride length
SELECT
AVG (ride_length) AS total_mean_ridelength,
MAX (ride_length) AS max_ridelength,
MODE () WITHIN GROUP (ORDER BY ride_length) AS mode_of_ridelength,
COUNT (ride_length) AS total_ridelength_count
FROM trip_data;

--Calculating the mean, max, and count for the ridelength and mode by week day
--week day goes from 0 as Sunday to 6 as Saturday
SELECT
member_casual,
AVG (ride_length) AS total_mean_ridelength,
MAX (ride_length) AS max_ridelength,
MODE () WITHIN GROUP (ORDER BY week_day) AS mode_of_week_day,
COUNT (ride_length) AS total_ridelength_count
FROM trip_data
GROUP BY member_casual;

--Calculating the mean, max, and count for the ridelength and week day
--week day goes from 0 as Sunday to 6 as Saturday
SELECT
member_casual,
week_day,
AVG (ride_length) AS mean_ridelength_day,
COUNT (ride_length) AS ridelength_count_by_day
FROM trip_data
GROUP BY member_casual,
week_day;

--Calculating the mean, max, and count for the ridelength and ride type
--Confirming the ride type most used by the casual members
--to advise on the ride type to be adverised most
SELECT
member_casual,
rideable_type,
AVG (ride_length) AS mean_ridelength_day,
COUNT (ride_length) AS ridelength_count_by_day
FROM trip_data
GROUP BY member_casual,
rideable_type;
