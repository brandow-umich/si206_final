import csv
import sqlite3
import json
import os
import time
import requests
import pandas as pd
import altair as alt
import folium
from urllib.request import urlopen

# DOGS:
# id INTEGER PRIMARY KEY,
# location_id INTEGER,
# breed_id INTEGER,
# FOREIGN KEY (location_id) REFERENCES locations(id),
# FOREIGN KEY (breed_id) REFERENCES breeds(id)

# LOCATIONS:
# id INTEGER PRIMARY KEY,
# address TEXT,
# latitude FLOAT,
# longitude FLOAT

# BREEDS:
# id INTEGER PRIMARY KEY AUTOINCREMENT,
# primary_breed TEXT


# Brandon's API/Secret key
API_KEY = '8rNWQrEyOuNVDs1EtEmRKpkqGPhXMfErpAMxI1uSqfWwfM1CWs'
SECRET_KEY = 'O39nMA96aRPvNyERjDnWrcVufBnPU1axEfZQZ8j7'

# Maps API Key
MAPS_API_KEY = 'AIzaSyDnyhvFfr0OyyFSj7cbhj58Xgi7ogPcTA8'


"""
Input: Database name
Creates a connection to the database and returns the cursor and connection
Returns: cur, conn
"""
def db_setup(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db_name)
    cur = conn.cursor()
    return cur, conn

"""
Input: cur, conn
Creates the dogs table
Returns: None
"""
def create_dogs_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS dogs ('
                'id INTEGER PRIMARY KEY,'
                'location_id INTEGER, '
                'breed_id INTEGER, '
                'FOREIGN KEY (location_id) REFERENCES locations(id), '
                'FOREIGN KEY (breed_id) REFERENCES breeds(id))')

    conn.commit()

"""
Input: cur, conn
Creates the locations table
Returns: None
"""
def create_locations_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS locations ('
                'id INTEGER PRIMARY KEY, '
                'address TEXT, '
                'latitude FLOAT, '
                'longitude FLOAT)')

    conn.commit()

"""
Input: cur, conn
Creates the breeds table
Returns: None
"""
def create_breeds_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS breeds ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'primary_breed TEXT)')

    conn.commit()

"""
Input: cur, conn
Function runs all create table functions
Returns: None
"""
def create_tables(cur, conn):
    create_dogs_table(cur, conn)
    create_locations_table(cur, conn)
    create_breeds_table(cur, conn)

"""
Input: Api key, secret key for Petfinder API
Returns the oAuth token for the API
"""
def get_oAuth_token(API_KEY, SECRET_KEY):
    # Set the endpoint URL
    ENDPOINT = 'https://api.petfinder.com/v2/oauth2/token'

    # Request data
    data = {
        'grant_type': 'client_credentials',
        'client_id': API_KEY,
        'client_secret': SECRET_KEY
    }

    # Make the request
    r = requests.post(ENDPOINT, data=data)

    # Check the status code of the response
    if r.status_code != 200:
        print(f'Request failed with status code {r.status_code}')
        return None
    else:
        # Get the token from the response
        token = r.json()['access_token']
        return token


"""
Input: address and API key for Google Maps API
Returns the latitude and longitude of the address
"""
def get_coordinates(address, MAPS_API_KEY):
    # Set the endpoint URL
    ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'

    # Request data
    params = {
        'address': address,
        'key': MAPS_API_KEY
    }

    # Make the request
    r = requests.get(ENDPOINT, params=params)

    # Parse JSON response
    data = r.json()

    # Get the coordinates
    lat = data['results'][0]['geometry']['location']['lat']
    lng = data['results'][0]['geometry']['location']['lng']

    return lat, lng


"""
Input: latitude and longitude and API key for Google Maps API
Returns the county of the latitude and longitude
"""
def get_county_info(lat, lng, MAPS_API_KEY):
    # Set the endpoint URL
    ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'

    # Request data
    params = {
        'latlng': str(lat) + ',' + str(lng),
        'result_type': 'administrative_area_level_2',
        'key': MAPS_API_KEY
    }

    # Make the request
    r = requests.get(ENDPOINT, params=params)

    # Parse JSON response
    data = r.json()

    # Get the county
    # county_info = data['results'][1]['address_components'][3]['long_name']
    county_info = data['results'][0]['address_components'][0]['long_name']

    return county_info

"""
Input: oAuth token, city state formatted 'city, state', cursor and connection
Function queries the Petfinder API and adds the data to the database
"""
def query_petfinder(token, cityState, cur, conn):
    
    start_time = time.perf_counter()

    # Set the endpoint URL
    ENDPOINT = 'https://api.petfinder.com/v2/animals'

    # Keep track of page we're on
    page = 1

    # Flag indicating if we've reached the last page
    has_more_pages = True

    # Loop a set of 25 results and add them to the database
    # Do this until we've reached the last page
    while has_more_pages:
        # Request data
        params = {
            'type': 'dog',
            'page': page,
            'location': cityState,  # Calculated Cadillac, MI to be possible center of State when factoring in the UP  
            'distance': 150,        # Went with Saginaw because Cadillac queried a lot of other states, increasing build time. 
            'limit': 25             # LIMITED RESULTS TO 25
        }

        # Add the token to the request headers
        headers = {
            'Authorization': f'Bearer {token}'
        }

        # Make the request
        r = requests.get(ENDPOINT, params=params, headers=headers)

        # Check the status code of the response
        if r.status_code != 200:
            print(f'Request failed with status code {r.status_code}')
            break

        # Get the response data
        data = r.json()

        # Loop through the results
        for dog in data['animals']:
            id = dog['id']
            address1 = dog['contact']['address']['address1']
            city = dog['contact']['address']['city']
            state = dog['contact']['address']['state']
            postcode = dog['contact']['address']['postcode']
            address2 = dog['contact']['address']['address2']

            # Check if address lines are null
            if address1 is None:
                address1 = ''
            if address2 is None:
                address2 = ''

            # Combine address1 and address2
            address = address1 + ' ' + address2

            # Concatenate address, city, state, and postcode
            # If address = ' ', then it will be removed
            if address == ' ': 
                full_address = city + ', ' + state + ' ' + postcode
            else:
                full_address = address + ', ' + city + ', ' + state + ' ' + postcode

            # print(full_address)

            primary_breed = dog['breeds']['primary']

            # Check if dog is from MI
            if state == 'MI':

                # Get latitude and longitude from Maps API
                latitude, longitude = get_coordinates(full_address, MAPS_API_KEY)

                # print(latitude, longitude)  # TODO

                # Get location_id from locations table
                cur.execute('SELECT id FROM locations WHERE address = ?', (full_address,))
                location_id = cur.fetchone()

                # Check if location_id is null. If it is, then insert a row into the locations table and get the location_id. Otherwise, get the location_id
                if location_id is None:
                    cur.execute('INSERT OR IGNORE INTO locations VALUES (NULL, ?, ?, ?)', (full_address, latitude, longitude))
                    cur.execute('SELECT id FROM locations WHERE address = ? AND latitude = ? AND longitude = ?',
                     (full_address, latitude, longitude))
                    location_id = cur.fetchone()[0]
                else:
                    location_id = location_id[0]

                # Get breed_id
                cur.execute('SELECT id FROM breeds WHERE primary_breed = ?', (primary_breed,))
                breed_id = cur.fetchone()

                # Check if breed_id is null. If it is, then insert a row into the breeds table and get the breed_id. Otherwise, get the breed_id
                if breed_id is None:
                    cur.execute('INSERT OR IGNORE INTO breeds VALUES (NULL, ?)', (primary_breed, ))
                    cur.execute('SELECT id FROM breeds WHERE primary_breed = ?', (primary_breed,))
                    breed_id = cur.fetchone()[0]
                else:
                    breed_id = breed_id[0]

                # Insert a row into the dogs table
                cur.execute('INSERT OR IGNORE INTO dogs VALUES (?, ?,?)', 
                            (id, location_id, breed_id))

        # Check if we've reached the last page
        if data['pagination']['current_page'] == data['pagination']['total_pages']:

            # Set the flag to false
            has_more_pages = False

        # Increment the page number
        page += 1

        # Commit the changes to the database
        conn.commit()

    # Close the cursor
    cur.close()

    end_time = time.perf_counter()

    total_time = end_time - start_time

    print('Total time: ' + str(total_time))


"""
Function takes in cursor and connection to the database 
Calls a query to get the breed with the most dogs and the count of 
that breed at each location and the location's latitude and longitude.
Creates a map and adds a marker for each location with a popup 
containing the breed with the most dogs and the count of that breed.

"""
def map_count_by_breed_per_location(cur, conn):
    
    # Get the breed with the most dogs and the count of that breed at each location and the 
    # location's latitude and longitude
    query = '''
        SELECT
        locations.address,
        locations.latitude,
        locations.longitude,
        breeds.primary_breed,
        COUNT(dogs.id) AS dog_count
        FROM dogs
        INNER JOIN breeds ON breeds.id = dogs.breed_id
        INNER JOIN locations ON locations.id = dogs.location_id
        GROUP BY locations.address, breeds.primary_breed
        ORDER BY dog_count ASC
    '''
    
    # Execute the query
    cur.execute(query)

    # Get the data
    data = cur.fetchall()

    # Create a map
    m = folium.Map(location=[43.0, -84.0], zoom_start=7)

    # Loop through the data and add markers to the map
    for row in data:
        
        # Get the data from the row
        address = row[0]
        latitude = row[1]
        longitude = row[2]
        primary_breed = row[3]
        dog_count = row[4]

        # Create a popup for the marker
        popup = f'{primary_breed} - {dog_count}'

        # Add a marker to the map
        folium.Marker([latitude, longitude], popup=popup).add_to(m)

    # Save the map
    m.save('map1.html')
    

"""
Input: cursor and connection to the database
Output: Prints the number of dogs at each location
Not used for visualization
"""
def get_dog_count_by_location(cur, conn):
    query = '''
        SELECT COUNT(dogs.id) AS dog_count, locations.address
        FROM dogs
        JOIN locations ON dogs.location_id = locations.id
        GROUP BY locations.address
    '''

    # Execute the query
    cur.execute(query)

    # Get the data
    data = cur.fetchall()

    # Print the data in a formatted way
    # "Location: location, Dog Count: dog_count"
    for row in data:
        print('Location: ' + row[1] + ', Dog Count: ' + str(row[0]))

    # Get the total number of dogs
    cur.execute('SELECT COUNT(id) FROM dogs')
    total_dogs = cur.fetchone()[0]

    # Print the total number of dogs
    print('Total number of dogs: ' + str(total_dogs))
    

"""
Function takes in cursor and connection to the database

Calls a query to get the count of dogs by breed at each location.
Outputs the data to a csv file called dog_count_by_breed_per_location.csv
It then takes the csv and creates a large bar chart with the count of dogs by 
breed at each location.

"""
def get_dog_count_by_breed_per_location(cur, conn):

    query = '''
        SELECT COUNT(dogs.id) AS dog_count, breeds.primary_breed, locations.address
        FROM dogs
        JOIN breeds ON dogs.breed_id = breeds.id
        JOIN locations ON dogs.location_id = locations.id
        GROUP BY breeds.primary_breed, locations.address
        ORDER BY dog_count DESC
    '''

    # Execute the query
    cur.execute(query)

    # Get the data
    data = cur.fetchall()

    # Print to csv file
    with open('dog_count_by_breed_per_location.csv', 'w', newline='') as f:
        writer = csv.writer(f)

        # Header row
        writer.writerow(['dog_count', 'primary_breed', 'address'])

        # Loop through data response and write
        for row in data:
            writer.writerow(row)      

    # Create graph with altair
    df = pd.read_csv('dog_count_by_breed_per_location.csv')

    # Sort the data by dog count
    df = df.sort_values(by='dog_count', ascending=True)

    # Create the chart
    chart = alt.Chart(df).mark_bar().encode(
        x='address',
        y='dog_count',
        color='primary_breed',
        tooltip=['primary_breed', 'dog_count']
    ).interactive()

    # Save the chart
    chart.save('dog_count_by_breed_per_location.html')


"""
Function takes in cursor and connection to the database

Calls a query to get the most popular breed at each location.
Outputs the data to a csv file called most_popular_breed_per_location.csv

"""
def get_most_popular_breed_per_location(cur, conn):
    
    query = '''
        SELECT
        locations.address,
        breeds.primary_breed,
        COUNT(dogs.id) AS dog_count
        FROM dogs
        INNER JOIN breeds ON breeds.id = dogs.breed_id
        INNER JOIN locations ON locations.id = dogs.location_id
        GROUP BY locations.address, breeds.primary_breed
        ORDER BY dog_count DESC
    '''

    # Execute the query
    cur.execute(query)

    # Get the data
    data = cur.fetchall()

    # Print to csv file
    with open('most_popular_breed_per_location.csv', 'w', newline='') as f:
        writer = csv.writer(f)

        # Header row
        writer.writerow(['Address', 'Most Popular Breed', 'Number of Dogs'])

        # Loop through data response and write
        for row in data:
            writer.writerow(row)
    
"""
Function takes in cursor and connection to the database

Calls a query to get the most popular breed at each location.
Outputs to a choropleth map with shading for each county based on the number of dogs

"""
def choropleth_map_visualization(cur, conn):

    # Get the data from the database
    query = '''
        SELECT COUNT(D.id) as num_dogs, C.county
        FROM DOGS D
        INNER JOIN LOCATIONS L ON D.location_id = L.id
        INNER JOIN COUNTIES C ON L.county_id = C.id
        GROUP BY C.county

    '''

    # Execute the query
    cur.execute(query)

    # Get the data
    data = cur.fetchall()
    
    # Create the map
    m = folium.Map(location=[43.0, -84.0], zoom_start=7)

    # Get the geojson data
    response = urlopen("https://geojson-cloud.up.railway.app/states?fips=26")

    # Load the data
    michigan = json.loads(response.read())

    # Create the choropleth map
    folium.Choropleth(
        geo_data='michigan-with-county-boundaries_1105.geojson',
        name='choropleth',
        data=data,
        columns=['county', 'dog_count'],
        key_on='feature.properties.name',
        fill_color='YlOrRd',
        fill_opacity=0.7,
        line_opacity=0.2,
        highlight=True,
        legend_name='Dog Count'
    ).add_to(m)

    # Save the map to a file
    m.save('choropleth_map.html')

    
if __name__ == '__main__':
    # Get the petfinder token
    token = get_oAuth_token(API_KEY, SECRET_KEY)

    # Set up the database
    cur, conn = db_setup('dogsfinal1.db')

    # Create the tables
    create_tables(cur, conn)

    # Run the query
    # query_petfinder(token, 'Saginaw, MI', cur, conn)

    # Get the number of dogs at each location
    # get_dog_count_by_location(cur, conn)

    get_dog_count_by_breed_per_location(cur, conn)

    get_most_popular_breed_per_location(cur, conn)

    map_count_by_breed_per_location(cur, conn)

    get_dog_count_by_breed_per_location(cur, conn)

    print('Done!')