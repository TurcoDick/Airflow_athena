-- create tables

CREATE EXTERNAL TABLE IF NOT EXISTS global_temperatures(
  dt DATE,
  land_average_temperature FLOAT,
  land_average_temperature_uncertainty FLOAT,
  land_max_temperature FLOAT,
  land_max_temperature_uncertainty FLOAT,
  land_min_temperature FLOAT,
  land_min_temperature_uncertainty FLOAT,
  land_and_ocean_average_temperature FLOAT,
  land_and_ocean_average_temperature_uncertainty FLOAT
  )
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
  LOCATION 's3://final-project-udacity/original/temperatures_data/global_temperatures/'
  TBLPROPERTIES("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS global_land_temperatures_by_state(
    dt DATE,
    average_temperature FLOAT,
    average_temperature_uncertainty FLOAT,
    state VARCHAR(100),
    country VARCHAR(100)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/temperatures_datas/global_land_temperatures_by_state/'
TBLPROPERTIES("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS global_land_temperatures_by_major_city(
    dt DATE,
    average_temperature FLOAT,
    average_temperature_uncertainty FLOAT,
    city VARCHAR(100),
    country VARCHAR(100),
    latitude VARCHAR(10),
    longintude VARCHAR(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/temperatures_data/global_land_temperatures_by_major_city/'
TBLPROPERTIES("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS global_land_temperatures_by_country(
    dt DATE,
    average_temperature FLOAT,
    average_temperature_uncertainty FLOAT,
    country VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/temperatures_data/global_land_temperatures_by_country/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS global_land_temperatures_by_city(
    dt DATE,
    average_temperature FLOAT,
    average_temperature_uncertainty FLOAT,
    city VARCHAR(200),
    country VARCHAR(100),
    latitude VARCHAR(10),
    longitude VARCHAR(10)
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/temperatures_data/global_land_temperatures_by_city/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS immigration(
    passender_id VARCHAR(50),
    cicid FLOAT,
    i94yr INT,
    i94mon INT,
    i94cit INT,
    i94res INT,
    i94port INT,
    arrdate INT,
    i94mode INT,
    i94addr INT,
    depdate INT,
    i94bir INT,
    i94visa INT,
    `count` INT,
    dtadfile INT,
    visapost VARCHAR(10),
    occup VARCHAR(10),
    entdepa VARCHAR(1),
    entdepd VARCHAR(1),
    entdepu VARCHAR(1),
    matflag VARCHAR(1),
    biryear INT,
    dtaddto VARCHAR(15),
    gender VARCHAR(1),
    `insnum` INT,
    airline VARCHAR(10),
    admnum BIGINT,
    fltno VARCHAR(10),
    visatype VARCHAR(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/immigration_data/'
TBLPROPERTIES("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS us_cities_demographics(
    city VARCHAR(30),
    state VARCHAR(30),
    median_age FLOAT,
    male_population BIGINT,
    female_population BIGINT, 
    total_polulation BIGINT,
    number_veterans INT,
    foreign_born INT,
    average_household_size FLOAT,
    state_code VARCHAR(5),
    race VARCHAR(20),
    quant INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/us_cities_demographics/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS airport_codes(
    ident VARCHAR(15),
    `type` VARCHAR(20),
    name VARCHAR(30),
    elevation_ft INT,
    continent VARCHAR(20),
    iso_country VARCHAR(10),
    iso_region VARCHAR(15),
    municipality VARCHAR(40),
    gps_code VARCHAR(100),
    iata_code VARCHAR(15),
    local_code VARCHAR(200),
    coordinates VARCHAR(200)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://final-project-udacity/original/airport_codes/'
TBLPROPERTIES("skip.header.line.count"="1");

-- buscar a quantidade de anos distintos para particionar por ano 

-- criando as tabelas intermediarias

CREATE TABLE global_temperatures_intermediary
WITH (
      format = 'Parquet',
      parquet_compression = 'SNAPPY',
      external_location = 's3://final-project-udacity/intermediary/global_temperatures_intermediary/'
  )
  AS SELECT 
  dt,
  land_average_temperature,
  land_average_temperature_uncertainty,
  land_max_temperature,
  land_max_temperature_uncertainty,
  land_min_temperature,
  land_min_temperature_uncertainty,
  land_and_ocean_average_temperature,
  land_and_ocean_average_temperature_uncertainty,
  EXTRACT(YEAR FROM dt) as year,
  EXTRACT(MONTH FROM dt) as month
  FROM global_temperatures;    


CREATE TABLE global_land_temperatures_by_state_intermediary
WITH(
      format = 'Parquet',
      parquet_compression = 'SNAPPY',
      external_location = 's3://final-project-udacity/intermediary/global_land_temperatures_by_state_intermediary/'
    )
AS SELECT
dt,
average_temperature,
average_temperature_uncertainty,
state,
country,
EXTRACT(YEAR FROM dt) as year,
EXTRACT(MONTH FROM dt) as month
FROM global_land_temperatures_by_state;


CREATE TABLE global_land_temperatures_by_major_city_intermediary
WITH(
        format = 'Parquet',
        parquet_compression = 'SNAPPY',
        external_location = 's3://final-project-udacity/intermediary/global_land_temperatures_by_major_city_intermediary/'
    )
AS SELECT
    dt, 
    average_temperature,
    average_temperature_uncertainty,
    city,
    country,
    latitude,
    longintude,
    EXTRACT(YEAR FROM dt) as year,
    EXTRACT(MONTH FROM dt) as month
    from global_land_temperatures_by_major_city;

global_land_temperatures_by_country


global_land_temperatures_by_city

-- particionando
CREATE TABLE global_temperatures_partitioned 
WITH (
      format = 'Parquet',
      parquet_compression = 'SNAPPY',
      external_location = 's3://final-project-udacity/partitioned-data/global_temperatures_partitioned/',
      bucketed_by = ARRAY['year'],
      bucket_count = 266
      )
  AS SELECT dt,
  land_average_temperature,
  land_average_temperature_uncertainty,
  land_max_temperature,
  land_max_temperature_uncertainty,
  land_min_temperature,
  land_min_temperature_uncertainty,
  land_and_ocean_average_temperature,
  land_and_ocean_average_temperature_uncertainty,
  year,
  month
  FROM global_temperatures_intermediary; 


CREATE TABLE global_land_temperatures_by_state_partitioned
WITH (
    format = 'Parquet',
    parquet_compression = 'SNAPPY',
    external_location = 's3://final-project-udacity/partitioned-data/global_land_temperatures_by_state_partitioned/'

)