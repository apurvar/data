DROP TABLE IF EXISTS WeatherRaw;

CREATE  TABLE  WeatherRaw(station_id STRING, observation_date STRING, observation_type STRING, observation_value STRING, observation_measure STRING, observation_quality STRING, observation_source STRING, observation_time STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'usr/local/Cellar/hive/3.1.1/data/weather' ;

LOAD DATA LOCAL INPATH '/Users/arathi/Documents/scripts/2017.csv' INTO TABLE WeatherRaw;

