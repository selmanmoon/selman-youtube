-- Athena Table

CREATE EXTERNAL TABLE openaq_data (
    country STRING,
    coordinates STRUCT<latitude:DOUBLE, longitude:DOUBLE>,
    date STRUCT<local:STRING, utc:STRING>,
    city STRING,
    parameter STRING,
    unit STRING,
    value DOUBLE,
    quality_label STRING
)
STORED AS PARQUET
LOCATION 's3://myprocessedbucket-785255668313/openaq/processed_data/';

# --Top 10 Cities with Highest Average PM2.5, Grouped by Country:

WITH CityAverages AS (
   SELECT 
       country, 
       city,
       AVG(value) as average_pm25
   FROM openaq_data
   WHERE parameter = 'pm25'
   GROUP BY country, city
),
RankedCities AS (
   SELECT 
       country, 
       city, 
       average_pm25,
       RANK() OVER(PARTITION BY country ORDER BY average_pm25 DESC) as rank
   FROM CityAverages
)

SELECT country, city, average_pm25
FROM RankedCities
WHERE rank <= 10 
ORDER BY country, rank, city;


--Top 5 avg pm25

SELECT 
    country,
    AVG(value) as avg_pm25
FROM openaq_data
WHERE parameter = 'pm25'
GROUP BY country
ORDER BY avg_pm25 DESC
LIMIT 5;



