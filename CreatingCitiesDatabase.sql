/***************************
Setting up the environment
***************************/

-- Drop the database if it already exists
DROP DATABASE IF EXISTS sql_gans;

-- Create the database
CREATE DATABASE sql_gans;

-- Use the database
USE sql_gans;

-- Create the 'country' table
CREATE TABLE country (
    country_id INT AUTO_INCREMENT, -- Automatically generated ID for each country
    country_name VARCHAR(255) NOT NULL, -- Name of the country
    PRIMARY KEY (country_id) -- Primary key to uniquely identify each city
);

-- Create the 'city' table
CREATE TABLE city (
    city_id INT AUTO_INCREMENT, -- Automatically generated ID for each city
    city_name VARCHAR(255) NOT NULL, -- Name of the city
    latitude FLOAT NOT NULL, -- Latitude
    longitude FLOAT NOT NULL, -- Longitude
    country_id INT NOT NULL, -- Name of the country
    PRIMARY KEY (city_id), -- Primary key to uniquely identify each city
    FOREIGN KEY (country_id) REFERENCES country(country_id) -- Foreign key to connect each population to its city
);

-- Create the 'population' table
CREATE TABLE city_population (
    population INT NOT NULL, -- Population
    timestamp_population DATE NOT NULL, -- Timestamps
    city_id INT NOT NULL, -- ID of the city
    PRIMARY KEY (city_id), -- Primary key to uniquely identify each city
    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
);

DROP TABLE IF EXISTS weather;

-- Create the 'weather' table
CREATE TABLE weather (
	visibility INT NOT NULL,
	precipitation_probability FLOAT NOT NULL,
	date_time DATETIME NOT NULL,
	temperature FLOAT NOT NULL,
	feels_like FLOAT NOT NULL,
	pressure INT NOT NULL,
	humidity INT NOT NULL,
	weather_main VARCHAR(255) NOT NULL,
	description VARCHAR(255) NOT NULL,
	clouds INT NOT NULL,
	wind_speed FLOAT NOT NULL,
	part_of_day CHAR(1) NOT NULL,
	rain_3h FLOAT NOT NULL,
	snow_3h FLOAT NOT NULL,
	city_id INT NOT NULL,
	weather_timestamp DATETIME NOT NULL, -- Timestamps
    PRIMARY KEY (city_id, date_time), -- Primary key to uniquely identify each city
    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
);

DROP TABLE IF EXISTS airport;

-- Create the 'airport' table
CREATE TABLE airport (
	icao VARCHAR(4) NOT NULL,
	airport_name VARCHAR(50) NOT NULL,
	city_id INT NOT NULL,
    PRIMARY KEY (icao), -- Primary key to uniquely identify each airport
    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
);

DROP TABLE IF EXISTS arrival;

-- Create the 'arrival' table
CREATE TABLE arrival (
	arrival_id INT AUTO_INCREMENT, -- Automatically generated ID for each 
	number VARCHAR(10) NOT NULL,
	icao  VARCHAR(4) NOT NULL,
	scheduledTime DATETIME NOT NULL, -- Scheduled Time
	revisedTime DATETIME NOT NULL, -- Revised Time
    PRIMARY KEY (arrival_id), -- Primary key to uniquely identify each city
	UNIQUE (number, scheduledTime), -- Recommended
    FOREIGN KEY (icao) REFERENCES airport(icao) -- Foreign key to connect each population to its city
);

DROP TABLE IF EXISTS departure;

-- Create the 'departure' table
CREATE TABLE departure (
	departure_id INT AUTO_INCREMENT, -- Automatically generated ID for each 
	number VARCHAR(10) NOT NULL,
	icao  VARCHAR(4) NOT NULL,
	scheduledTime DATETIME NOT NULL, -- Scheduled Time
	revisedTime DATETIME NOT NULL, -- Revised Time
    PRIMARY KEY (departure_id), -- Primary key to uniquely identify each city
	UNIQUE (number, scheduledTime), -- Recommended
    FOREIGN KEY (icao) REFERENCES airport(icao) -- Foreign key to connect each population to its city
);

SELECT * FROM country;
SELECT * FROM city;
SELECT * FROM city_population;
SELECT * FROM weather;
SELECT * FROM airport;
SELECT * FROM arrival;
SELECT * FROM departure;


SELECT SUM(snow_3h) FROM weather;
SELECT SUM(rain_3h) FROM weather;

SHOW CREATE TABLE weather;
