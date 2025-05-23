# To scratch the web
import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests

# To make Timestamps
!pip install lat-lon-parser
from lat_lon_parser import parse
import datetime

# To connect to MySQL
from sqlalchemy import create_engine, text
import os

# To read the .env file
from dotenv import load_dotenv
load_dotenv()

# For the timezone cleaning
import re

# Obtain the details of the parsed city
def getCityInfo(city, verbose=False):
    city_data = []
    
    # Create the URL to parse to BeautifulSoup
    url_ = 'https://en.wikipedia.org/wiki/'
    url_city = url_ + city
    response_city = requests.get(url_city)
    
    # Create soup
    complete_soup = BeautifulSoup(response_city.content, 'html.parser')
    soup_city = complete_soup.find(class_='infobox ib-settlement vcard')
    
    # Get city and country name
    city_name = complete_soup.find('h1', class_='firstHeading').get_text()
    city_country = soup_city.select('td.infobox-data')[0].get_text().replace('\xa0', '').strip()
    
    # Get the code of the city
    city_code = soup_city.select('td.infobox-data nickname')#find(class_='').get_text()
    
    # Get latitude and longitude
    city_latitude = soup_city.find(class_='latitude').get_text()
    city_latitude = parse(city_latitude)
    city_longitude = soup_city.find(class_='longitude').get_text()
    city_longitude = parse(city_longitude)
    
    # Get population
    city_population = soup_city.find(string='Population').find_next(class_='infobox-data').get_text()
    
    # Get Timestamp
    today = datetime.datetime.today().strftime('%d.%m.%Y %H:%M:%S')
    
    # Append City Data
    city_data.append(city_name)
    city_data.append(city_country)
    city_data.append(city_latitude)
    city_data.append(city_longitude)
    city_data.append(city_population)
    city_data.append(today)

    print(f'🌐 {city} data scratched')
    
    return city_data

# Define SQL Engine
def sql_engine(schema="sql_gans", host="127.0.0.1", user="root", port=3306, reset_database=False, verbose=False):
    
    password = os.getenv("MYSQL_PASSWORD")  # Make sure this is set in your environment!
    
    # Create connection string
    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    if reset_database == True:
        connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/'
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    return engine

# Cities Into to SQL
def cities_to_database(cities, schema="sql_gans", host="127.0.0.1", user="root", port=3306, reset_database=False, verbose=False):
 
    # Reset Database
    if reset_database == True:
        # Set MySQL
        # Create connection string (without specifying Database, so that it can drop database (if existing)
        engine = sql_engine(schema=schema, host=host, user=user, port=port, reset_database=reset_database)
    
        # Reset Database
        with engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {schema};"))
            connection.execute(text(f"CREATE DATABASE {schema};"))
            print(f"🧮 Database {schema} dropped and recreated.")

    # Create SQLAlchemy engine
    engine = sql_engine(schema=schema, host=host, user=user, port=port)

    # Test the connection
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("✔️ Connection successful:", result.scalar())
    except Exception as e:
        print('❌ Connection failed:', e)

    try:
        # Create MySQL Tables
        with engine.connect() as conn:
            # Create the 'country' table
            conn.execute(text("""
                CREATE TABLE country (
                    country_id INT AUTO_INCREMENT, -- Automatically generated ID for each country
                    country_name VARCHAR(255) NOT NULL, -- Name of the country
                    PRIMARY KEY (country_id) -- Primary key to uniquely identify each city
                );
            """))
            print('🌎️ Table "country" created')
    except Exception as e:
        print('🌎️ Previous "country" table found. No tables created')
    
    try:
        # Create MySQL Tables
        with engine.connect() as conn:
            # Create the 'city' table
            conn.execute(text("""
                CREATE TABLE city (
                    city_id INT AUTO_INCREMENT, -- Automatically generated ID for each city
                    city_name VARCHAR(255) NOT NULL, -- Name of the city
                    latitude FLOAT NOT NULL, -- Latitude
                    longitude FLOAT NOT NULL, -- Longitude
                    country_id INT NOT NULL, -- Name of the country
                    PRIMARY KEY (city_id), -- Primary key to uniquely identify each city
                    FOREIGN KEY (country_id) REFERENCES country(country_id) -- Foreign key to connect each population to its city
                );
            """))
            print('🗾 Table "city" created')
    except Exception as e:
        print('🗾 Previous "city" table found. No tables created')
        
    try:
        # Create MySQL Tables
        with engine.connect() as conn:
            # Create the 'population' table
            conn.execute(text("""
                CREATE TABLE city_population (
                    population INT NOT NULL, -- Population
                    timestamp_population DATETIME NOT NULL, -- Timestamps
                    city_id INT NOT NULL, -- ID of the city
                    PRIMARY KEY (city_id, timestamp_population), -- Primary key to uniquely identify each city
                    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
                );
            """))
            print('👨‍👩‍👧‍👦 Table "city_population" created')
    except Exception as e:
        print('👨‍👩‍👧‍👦 Previous "city_population" table found. No tables created')
        
    # Iterate to get the info of all the listed cities
    cities_info = []
    for city in cities:
        cities_info.append(getCityInfo(city))

    # Transform to a Pandas DataFrame
    cities_info_df = pd.DataFrame(cities_info)
    cities_info_df.columns = ['city_name', 'country_name', 'latitude', 'longitude', 'population', 'timestamp_population']
    
    # Transform the countries to its own Dataframe
    country_unique = cities_info_df["country_name"].unique()
    country_df = pd.DataFrame({"country_name": country_unique})

    # Check existing data on SQL tables for Countries
    # Read existing details
    previous_country_sql = pd.read_sql("SELECT * FROM country", con=engine)
    # Filter country details to only those not yet in the table
    country_df = country_df.loc[~(
        country_df['country_name'].isin(
            previous_country_sql['country_name']
        )
    ), :]
    
    # Send 'country_df' to SQL
    country_df.to_sql('country',
                    if_exists='append',
                    con=engine,
                    index=False)
    print('🌎️ Table "country" populated')

    # Fetch countries with IDs from the DB to merge with cities
    country_from_sql = pd.read_sql("SELECT * FROM country", con=engine)#, con=connection_string)

    # Merge to assign country_id to cities
    city_unique_df = cities_info_df.merge(country_from_sql,
                                       on = "country_name",
                                       how="left")

    # Drop columns not needed in city table
    city_df = city_unique_df.drop(['country_name', 'population', 'timestamp_population'], axis=1)

    # Check existing data on SQL Cities tables
    # Read existing details
    previous_city_sql = pd.read_sql("SELECT * FROM city", con=engine)
    # Filter city details to only those not yet in the table
    city_df = city_df.loc[~(
        city_df['city_name'].isin(
            previous_city_sql['city_name']
        )
    ), :]

    # Send city_df to SQL
    city_df.to_sql('city',
                   if_exists='append',
                   con=engine,
                   index=False)
    print('🗾 Table "city" populated')

    # Fetch cities with IDs from DB to merge with population info
    city_from_sql = pd.read_sql("SELECT * FROM city", con=engine)#, con=connection_string)

    # Prepare population info by merging city IDs
    info_unique_df = cities_info_df.merge(city_from_sql[['city_id', 'city_name']], on="city_name", how="left")

    # Select only columns needed for city_population table
    city_population_df = info_unique_df[['population', 'timestamp_population', 'city_id']].copy()

    # Convert timestamp_population to datetime.date (MySQL DATE format)
    city_population_df['timestamp_population'] = pd.to_datetime(
        city_population_df['timestamp_population'],
        format='%d.%m.%Y %H:%M:%S'
    )#.dt.date

    # Clean population column (remove commas and convert to int)
    city_population_df['population'] = city_population_df['population'].astype(str).str.replace(',', '').astype(int)
    
    # Read existing data in city_population
    previous_city_population_sql = pd.read_sql("SELECT population, city_id FROM city_population", con=engine)
    
    merged = city_population_df.merge(
        previous_city_population_sql,
        on=['population', 'city_id'],
        how='left',
        indicator=True
    )
    city_population_df = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    if not city_population_df.empty:
        city_population_df.to_sql('city_population', con=engine, if_exists='append', index=False)
        print('👨‍👩‍👧‍👦 Table "city_population" populated')
    else:
        print('👨‍👩‍👧‍👦 No new rows to insert in "city_population"')

    city = pd.read_sql("SELECT * FROM city", con=engine)
    print('🗾 Table "city" read')
    country = pd.read_sql("SELECT * FROM country", con=engine)
    print('🌎️ Table "country" read')
    city_population = pd.read_sql("SELECT * FROM city_population", con=engine)
    print('👨‍👩‍👧‍👦 Table "city_population" read')

    return city, country, city_population

# Get weather Information
def get_city_weather(city, verbose=False):
    # Open Weather Map API Key
    key = os.getenv("OPEN_WEATHER_API")
    
    weather = []
    
    # Extract location data from the cities
    for i in range(len(city)):
        cit = city.loc[i, 'city_name']
        lat = city.loc[i, 'latitude']
        lon = city.loc[i, 'longitude']

        # Generate and fetch URLs
        URL = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={key}&units=metric"
        response = requests.get(URL)
        print(f'⛅️ Weather info from {cit} fetched')

        # Get the JSON responses
        response_json = response.json()

        # Extract the weather list as a dictionary
        for item in response_json['list']:
            item['weather'] = item['weather'][0]

        # Transform JSON into a DataFrame
        response_norm = pd.json_normalize(response_json['list'])

        # Fill NaN from the Rain column
        if 'rain.3h' in response_norm.columns:
            response_norm['rain.3h'] = response_norm['rain.3h'].fillna(0)
        else:
            response_norm['rain.3h'] = 0.0

        # Fill NaN from the Snow column
        if 'snow.3h' in response_norm.columns:
            response_norm['snow.3h'] = response_norm['snow.3h'].fillna(0)
        else:
            response_norm['snow.3h'] = 0.0

        # Connect to the Cities IDs
        response_norm['city_id'] = city.loc[i, 'city_id']

        # Drop unnecessary columns
        response_norm_dropped = response_norm.drop([
            'dt',
            'main.temp_min',
            'main.temp_max',
            'main.pressure',
            'main.sea_level',
            'main.temp_kf',
            'weather.icon',
            'weather.id',
            'wind.deg',
            'wind.gust'
        ], axis=1)

        # Rename Columns
        response_norm_dropped = response_norm_dropped.rename(columns={
            'pop':'precipitation_probability',
            'dt_txt':'date_time',
            'main.temp':'temperature',
            'main.feels_like':'feels_like',
            'main.grnd_level':'pressure',
            'main.humidity':'humidity',
            'weather.main':'weather_main',
            'weather.description':'description',
            'clouds.all':'clouds',
            'wind.speed':'wind_speed',
            'sys.pod':'part_of_day',
            'rain.3h':'rain_3h',
            'snow.3h':'snow_3h'
        })

        # Transform date to datetime frame
        response_norm_dropped['date_time'] = pd.to_datetime(
            response_norm_dropped['date_time'],
            format='%Y-%m-%d %H:%M:%S'
        )

        # Get Weather Timestamp
        today = datetime.datetime.today().strftime('%d.%m.%Y %H:%M:%S')
        today = pd.to_datetime(today, format='%d.%m.%Y %H:%M:%S')        
        response_norm_dropped['weather_timestamp'] = today

        # Insert into the weather list
        weather.append(response_norm_dropped)

    weather_df = pd.concat(weather, ignore_index=True)

    return weather_df

# Weather to SQL
def weather_to_sql(weather, schema="sql_gans", host="127.0.0.1", user="root", port=3306, reset_database=False, verbose=False):
    # Create SQLAlchemy engine
    engine = sql_engine(schema=schema, host=host, user=user, port=port)
    
    # Create MySQL Weather Table
    with engine.connect() as conn:
        
        # Drop Weather Table if reset is set
        if reset_database == True:
            # Delete the 'weather' table if it exists
            conn.execute(text("""
               DROP TABLE IF EXISTS weather;
            """))
            print(f'🌤️ Weather database dropped and recreated.')

        # Create the 'weather' table
        try:
            conn.execute(text("""
                CREATE TABLE weather (
                    visibility INT NOT NULL,
                    precipitation_probability FLOAT NOT NULL,
                    date_time DATETIME NOT NULL,
                    temperature FLOAT NOT NULL, -- C
                    feels_like FLOAT NOT NULL, -- C
                    pressure INT NOT NULL, -- pressure at groud level hPa
                    humidity INT NOT NULL, -- %
                    weather_main VARCHAR(255) NOT NULL, -- Group of weather parameters
                    description VARCHAR(255) NOT NULL, -- Weather condition within the group
                    clouds INT NOT NULL, -- Cloudness %
                    wind_speed FLOAT NOT NULL, -- m/seg
                    part_of_day CHAR(1) NOT NULL, -- n: night , d: day)
                    rain_3h FLOAT NOT NULL, -- rain volume for last 3 hours mm
                    snow_3h FLOAT NOT NULL, -- snow volume for last 3 hours mm
                    city_id INT NOT NULL,
                    weather_timestamp DATETIME NOT NULL, -- Timestamps
                    PRIMARY KEY (city_id, date_time), -- Primary key to uniquely identify each city
                    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
                );
            """))
            print('⛅️ Table "weather" created')
        except Exception as e:
            print('⛅️ Previous "weather" table found. No tables created')

    # Fetch existing primary keys from the weather table
    existing = pd.read_sql("SELECT city_id, date_time FROM weather", con=engine)
    existing['date_time'] = pd.to_datetime(existing['date_time'])
    
    # Merge to find new rows
    weather_merged = weather.merge(existing, on=['city_id', 'date_time'], how='left', indicator=True)
    weather_new = weather_merged[weather_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    if not weather_new.empty:
        weather_new.to_sql('weather', if_exists='append', con=engine, index=False)
        print('⛅️ Table "weather" populated with new rows')
    else:
        print('⛅️ No new weather rows to insert')

    # Fetch from SQL
    weather_df = pd.read_sql("SELECT * FROM weather", con=engine)
    print('⛅️ Table "weather" read')

    return weather_df

# Create Airports Tables
def get_airports(city_tables, verbose=False):
    # Extract cities details
    city_ids = city_tables['city_id']
    latitudes = city_tables['latitude']
    longitudes = city_tables['longitude']
    city_names = city_tables['city_name']

    # API headers
    headers = {
        "X-RapidAPI-Key": RapidAPI_KEY,
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }

    querystring = {"withFlightInfoOnly": "true"}

    # DataFrame to store results
    all_airports = []

    for idn, lat, lon, city_name in zip(city_ids, latitudes, longitudes, city_names):
        # Construct the URL with the latitude and longitude
        url = f"https://aerodatabox.p.rapidapi.com/airports/search/location/{lat}/{lon}/km/50/16"

        # Make the API request
        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code == 200:
            data = response.json()
            airports = pd.json_normalize(data.get('items', []))
            # Add city_id
            airports['city_id'] = idn
            all_airports.append(airports)

            if verbose == True:
                print(f'Airports for {city_name} (city_id {idn}):\n', airports)

        else:
            print('Error: ', response.status_code)

        if verbose:
            print(f"API response for {cit}: {response.json()}")

    airports_df = pd.concat(all_airports, ignore_index=True)

    airports_df['airport_name'] = airports_df['name']

    # Drop unnecessary columns
    column_drop = [
        # 'icao',
        'iata',
        'name',
        'shortName',
        'municipalityName',
        'countryCode',
        'timeZone',
        'location.lat',
        'location.lon'
        ]

    # Drop columns
    airports_df.drop(
        columns=column_drop,
        inplace=True
        )

    if verbose==True:
        print('Airports gotten:\n',airports_df)

    return airports_df

# Insert Airports to Database
def airports_to_sql(airport, schema="sql_gans", host="127.0.0.1", user="root", port=3306, reset_database=False, verbose=False):

    # Connection setup
    engine = sql_engine(schema=schema, host=host, user=user, port=port, )
    
    # Create MySQL Weather Table
    with engine.connect() as conn:
        
        # Drop Airports Table if reset is set
        if reset_database == True:
            # Delete the 'airports' table if it exists
            conn.execute(text("""
               DROP TABLE IF EXISTS airport;
            """))
            print(f"✈️ Airport database dropped and recreated.")

        try:
            # Create the 'airport' table
            conn.execute(text("""
                CREATE TABLE airport (
                	icao VARCHAR(4) NOT NULL,
                    airport_name VARCHAR(50) NOT NULL,
                	city_id INT NOT NULL,
                    PRIMARY KEY (icao), -- Primary key to uniquely identify each city
                    FOREIGN KEY (city_id) REFERENCES city(city_id) -- Foreign key to connect each population to its city
                );
            """))
            print('✈️ Table "airport" created')
        except Exception as e:
            print('✈️ Previous "airport" table found. No tables created')

    if verbose:
        print('Airports to insert:')
        print(airport)
    
    # Check existing data on SQL tables for Airports
    # Read existing details
    previous_airport_sql = pd.read_sql("SELECT * FROM airport", con=engine)
    # Filter airport details to only those not yet in the table
    airport = airport.loc[~(
        airport['icao'].isin(
            previous_airport_sql['icao']
        )
    ), :]
    
    if verbose:
        if not previous_airport_sql.empty:
            print('Airport from SQL:\n', previous_airport_sql.sample())
        else:
            print('Airport from SQL is empty.')
        print(previous_airport_sql.info())
        
        if not airport.empty:
            print('Airport from API cleaned:\n', airport.sample())
        else:
            print('Airport from API cleaned is empty.')
        print(airport.info())

    
    # Insert to SQL
    airport.to_sql('airport',
                    if_exists='append',
                    con=engine,
                    index=False)
    print('✈️ Table "airport" populated')

    # Fetch countries with IDs from the DB to merge with cities
    airport_from_sql = pd.read_sql("SELECT * FROM airport", con=engine)#, con=connection_string)
    print('✈️ Table "airport" read')

    if verbose == True:
        print('Airport to SQL:\n', airport_from_sql.sample())
        print(airport_from_sql.info())

    return airport_from_sql

# Get Arrival and Departure Times
def get_times(airports, days=1, verbose=False):
    all_arrival_json = []
    all_departure_json = []

    # Get times
    half_day_periods = 2 * days
    times_to_get = half_day_periods + 1
    times = []
    periods = []

    # Populate times list
    for t in range(times_to_get):
        time_t = pd.Timestamp.now(tz='Europe/Berlin')
        time_t = time_t + (t * datetime.timedelta(hours=12))
        times.append(time_t)
    # Get time periods
    for p in range(half_day_periods):
        time_p0 = times[p].strftime('%Y-%m-%dT%H:%M')
        time_p1 =  times[p+1] - datetime.timedelta(minutes=1)
        time_p1 = time_p1.strftime('%Y-%m-%dT%H:%M')
        period_p = time_p0 + '/' + time_p1
        periods.append(period_p)

    for icao in airports['icao']:
        for period in periods:
    
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{period}"
    
            querystring = {"withLeg":"false","direction":"Both","withCancelled":"false","withCodeshared":"true","withCargo":"false","withPrivate":"true","withLocation":"false"}
    
            headers = {
                "X-RapidAPI-Key": RapidAPI_KEY,
                "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
                }
    
            # Get query
            response_raw = requests.get(url, headers=headers, params=querystring)
    
            if response_raw.status_code == 200:
                # Separate the arrivals from the departures
                response_json_arrival = response_raw.json()['arrivals']
                response_json_departure = response_raw.json()['departures']
        
                # Specify airport
                [item.update({'icao': icao}) for item in response_json_arrival]
                [item.update({'icao': icao}) for item in response_json_departure]
    
                # Extend the dictionary with all the values
                all_arrival_json.extend(response_json_arrival)
                all_departure_json.extend(response_json_departure)


    # Normalize dictionaries
    response_norm_arrival = pd.json_normalize(all_arrival_json)
    response_norm_departure = pd.json_normalize(all_departure_json)
    
    # Columns to rename
    rename_columns = {
        # 'movement.airport.icao': 'icao',
        'movement.scheduledTime.local': 'scheduledTime',
        'movement.revisedTime.local': 'revisedTime'
    }

    # Rename columns
    response_norm_arrival.rename(columns=rename_columns, inplace=True)
    response_norm_departure.rename(columns=rename_columns, inplace=True)

    # Get only the columns of interest
    response_arrival = pd.DataFrame({
        'number': response_norm_arrival['number'], 
        'icao': response_norm_arrival['icao'], 
        'scheduledTime': response_norm_arrival['scheduledTime'], 
        'revisedTime': response_norm_arrival['revisedTime']
        })
    response_departure = pd.DataFrame({
        'number': response_norm_departure['number'], 
        'icao': response_norm_departure['icao'], 
        'scheduledTime': response_norm_departure['scheduledTime'], 
        'revisedTime':response_norm_departure['revisedTime']
        })

    def remove_tz_offset(s):
        # Removes trailing timezone offset like +02:00 or -04:00
        return re.sub(r'([+-]\d{2}:\d{2})$', '', str(s)).strip()
    
    for df in [response_arrival, response_departure]:
        for col in ['scheduledTime', 'revisedTime']:
            df[col] = df[col].apply(remove_tz_offset)
    
    # Convert times to datetime.date (MySQL DATE format) [ex. 2025-05-23 16:30]
    response_arrival['scheduledTime'] = pd.to_datetime(
        response_arrival['scheduledTime'],
        format='%Y-%m-%d %H:%M'
    )
    response_arrival['revisedTime'] = pd.to_datetime(
        response_arrival['revisedTime'],
        format='%Y-%m-%d %H:%M'
    )
    response_departure['scheduledTime'] = pd.to_datetime(
        response_departure['scheduledTime'],
        format='%Y-%m-%d %H:%M'
    )
    response_departure['revisedTime'] = pd.to_datetime(
        response_departure['revisedTime'],
        format='%Y-%m-%d %H:%M'
    )

    return response_arrival, response_departure

# Arrivals and Departures into SQL
def flights_to_sql(arrivals, departures, schema="sql_gans", host="127.0.0.1", user="root", port=3306, reset_database=False, verbose=False):
    
    # Connection setup
    engine = sql_engine(schema=schema, host=host, user=user, port=port, )
    
    # Create MySQL Flights Tables
    with engine.connect() as conn:
        
        # Drop Flights Tables if reset is set
        if reset_database == True:
            # Delete the 'arrival' table if it exists
            conn.execute(text("""
               DROP TABLE IF EXISTS arrival;
            """))
            print(f"🛬 Arrival database dropped and recreated.")
            # Delete the 'departure' table if it exists
            conn.execute(text("""
               DROP TABLE IF EXISTS departure;
            """))
            print(f"🛫 Departure database dropped and recreated.")

        try:
            # Create the 'arrival' table
            conn.execute(text("""
                CREATE TABLE arrival (
                	arrival_id INT AUTO_INCREMENT, -- Automatically generated ID for each 
                	number VARCHAR(10) NOT NULL,
                	icao VARCHAR(4) NOT NULL,
                	scheduledTime DATETIME NOT NULL, -- Scheduled Time
                	revisedTime DATETIME NOT NULL, -- Revised Time
                    PRIMARY KEY (arrival_id), -- Primary key to uniquely identify each city
                    UNIQUE (number, scheduledTime), -- Recommended
                    FOREIGN KEY (icao) REFERENCES airport(icao) -- Foreign key to connect each population to its city
                );
            """))
            print('🛬 Table "arrival" created')
        except Exception as e:
            print('🛬 Previous "arrival" table found. No tables created')
            
        try:
            # Create the 'departure' table
            conn.execute(text("""
                CREATE TABLE departure (
                	departure_id INT AUTO_INCREMENT, -- Automatically generated ID for each 
                	number VARCHAR(10) NOT NULL,
                	icao VARCHAR(4) NOT NULL,
                	scheduledTime DATETIME NOT NULL, -- Scheduled Time
                	revisedTime DATETIME NOT NULL, -- Revised Time
                    PRIMARY KEY (departure_id), -- Primary key to uniquely identify each city
                    UNIQUE (number, scheduledTime), -- Recommended
                    FOREIGN KEY (icao) REFERENCES airport(icao) -- Foreign key to connect each population to its city
                );
            """))
            print('🛫 Table "departure" created')
        except Exception as e:
            print('🛫 Previous "departure" table found. No tables created')
    
    # Check existing data on SQL tables for Arrivals
    # Read existing details
    previous_arrival_sql = pd.read_sql("SELECT * FROM arrival", con=engine)
    previous_departure_sql = pd.read_sql("SELECT * FROM departure", con=engine)

    # Antes de insertar en SQL, rellena nulos en revisedTime
    arrivals['revisedTime'] = arrivals['revisedTime'].fillna(arrivals['scheduledTime'])
    departures['revisedTime'] = departures['revisedTime'].fillna(departures['scheduledTime'])

    if verbose == True:
        if not previous_arrival_sql.empty:
            print('Arrival from SQL:\n', previous_arrival_sql.sample())
            print(previous_arrival_sql.info())
        else:
            print('Arrival from SQL is empty.')
        
        if not previous_departure_sql.empty:
            print('Departure from SQL:\n', previous_departure_sql.sample())
            print(previous_departure_sql.info())
        else:
            print('Departure from SQL is empty.')
        if not arrivals.empty:
            print('Arrival from API:\n', arrivals.sample())
            print(arrivals.info())
        else:
            print('Arrival from API is empty.')
        
        if not departures.empty:
            print('Departure from API:\n', departures.sample())
            print(departures.info())
        else:
            print('Departure from API is empty.')

    # Justo antes del merge en flights_to_sql:
    arrivals['scheduledTime'] = pd.to_datetime(arrivals['scheduledTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    previous_arrival_sql['scheduledTime'] = pd.to_datetime(previous_arrival_sql['scheduledTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    arrivals = arrivals.drop_duplicates(subset=['number', 'scheduledTime'])

    # Justo antes del merge en flights_to_sql:
    departures['scheduledTime'] = pd.to_datetime(departures['scheduledTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    previous_departure_sql['scheduledTime'] = pd.to_datetime(previous_departure_sql['scheduledTime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    departures = departures.drop_duplicates(subset=['number', 'scheduledTime'])

    merged = arrivals.merge(
        previous_arrival_sql[['number', 'scheduledTime']],
        on=['number', 'scheduledTime'],
        how='left',
        indicator=True
    )
    arrivals = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    merged = departures.merge(
        previous_departure_sql[['number', 'scheduledTime']],
        on=['number', 'scheduledTime'],
        how='left',
        indicator=True
    )
    departures = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    # Insert to SQL
    arrivals.to_sql('arrival',
                    if_exists='append',
                    con=engine,
                    index=False)
    print('🛬 Table "arrival" populated')
    departures.to_sql('departure',
                    if_exists='append',
                    con=engine,
                    index=False)
    print('🛫 Table "departure" populated')

    # Fetch arrivals
    arrival_from_sql = pd.read_sql("SELECT * FROM arrival", con=engine)#, con=connection_string)
    print('🛬 Table "arrival" read')
    # Fetch departures
    departure_from_sql = pd.read_sql("SELECT * FROM departure", con=engine)#, con=connection_string)
    print('🛫 Table "departure" read')

    return arrival_from_sql, departure_from_sql

# Fetch all the details at once
def get_city_info(cities, schema="sql_gans", host="127.0.0.1", user="root", port=3306, days=1, reset_database=False, verbose=False):

    # Transform cities to list if it is only one
    if type(cities) != list:
        cities = [cities]
        
    # Get basic cities info
    city_df, country_df, city_population_df = cities_to_database(
        cities, 
        schema=schema, 
        host=host,
        user=user,
        port=port,
        reset_database=reset_database, 
        verbose=verbose
    )

    # Fetch cities weather
    weather = get_city_weather(city_df, verbose=verbose)

    # Turn weather pd dataframe to sql
    weather_df = weather_to_sql(
        weather,
        reset_database=reset_database, 
        verbose=verbose
    )

    # Get airports
    airports = get_airports(city_df)
    airport_df = airports_to_sql(
        airports, 
        schema=schema, 
        host=host,
        user=user,
        port=port,
        reset_database=reset_database,
        verbose=verbose
    )

    # Get flights
    arrivals, departures = get_times(airport_df, days=days, verbose=verbose)

    arrival_df, departure_df = flights_to_sql(
        arrivals,
        departures,
        schema="sql_gans",
        host="127.0.0.1", 
        user="root", 
        port=3306, 
        reset_database=False,
        verbose=verbose
    )

    print('✅ FINISHED ✅')

    return city_df, country_df, city_population_df, weather_df, airport_df, arrival_df, departure_df

# Set Parametersreset_database = True
verbose = False
days = 1

cities = [
    'Berlin',
    'Hamburg',
    'Frankfurt',
    'Cologne',
    'Caracas',
    'Lima',
    'Freiburg',
    'Auckland',
    'Cusco',
    'Santiago',
    'Arequipa',
    'Paris',
    'Lyon',
    'Buenos_Aires',
    'Mumbay',
    'London',
    'Heidelberg',
    'Munich'
    ]

schema = "sql_gans"
host = "127.0.0.1"
user = "root"
port = 3306
RapidAPI_KEY = os.getenv("RapidAPI_KEY")  # Make sure this is set in your environment!

# RUN!
city, country, city_population, weather, airport, arrival, departure = get_city_info(
    cities, 
    schema=schema,
    host=host,
    user=user, 
    port=port, 
    days=days,
    reset_database=reset_database,
    verbose=verbose
)
