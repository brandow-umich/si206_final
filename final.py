import sqlite3
import json
import os
import time
import requests

# DOGS:
# id PRIMARY KEY (integer)
# name_id (integer)
# age_id (text) 
# gender_id (integer)
# location_id (integer)
# breed_id (integer)
# FOREIGN KEY (name_id) REFERENCES names(id)
# FOREIGN KEY (age_id) REFERENCES ages(id)
# FOREIGN KEY (gender_id) REFERENCES genders(id)
# FOREIGN KEY (location_id) REFERENCES locations(id)
# FOREIGN KEY (breed_id) REFERENCES breeds(id)

# NAMES:
# id PRIMARY KEY AUTOINCREMENT (integer)
# name (text)

# AGES:
# id PRIMARY KEY AUTOINCREMENT (integer)
# age (text) (ex. 'Adult', 'Young', 'Senior', 'Baby')

# GENDERS:
# id PRIMARY KEY AUTOINCREMENT (integer)
# gender (text)

# LOCATIONS:
# id PRIMARY KEY (integer)
# number_of_dogs (integer)
# address (text)
# county_id (integer)
# coordinates_id (integer)
# FOREIGN KEY (county_id) REFERENCES counties(id)
# FOREIGN KEY (coordinates_id) REFERENCES coordinates(id)

# BREEDS:
# id PRIMARY KEY AUTOINCREMENT (integer)
# primary_breed (text)
# num_dogs_of_breed (integer)

# COUNTIES:
# id PRIMARY KEY AUTOINCREMENT (integer)
# county (text)


# Brandon's API/Secret key
API_KEY = '8rNWQrEyOuNVDs1EtEmRKpkqGPhXMfErpAMxI1uSqfWwfM1CWs'
SECRET_KEY = 'O39nMA96aRPvNyERjDnWrcVufBnPU1axEfZQZ8j7'

# Maps API Key
MAPS_API_KEY = 'AIzaSyDnyhvFfr0OyyFSj7cbhj58Xgi7ogPcTA8'

def db_setup(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + db_name)
    cur = conn.cursor()
    return cur, conn


def create_dogs_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS dogs ('
                'id INTEGER PRIMARY KEY,'
                'name_id INTEGER, '
                'age_id INTEGER, '
                'gender_id INTEGER, '
                'location_id INTEGER, '
                'breed_id INTEGER, '
                'FOREIGN KEY (location_id) REFERENCES locations(id), '
                'FOREIGN KEY (breed_id) REFERENCES breeds(id))')

    conn.commit()


def create_ages_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS ages ('
                'id INTEGER PRIMARY KEY, '
                'age TEXT)')

    conn.commit()

def create_names_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS names ('
                'id INTEGER PRIMARY KEY, '
                'name TEXT)')

    conn.commit()

def create_genders_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS genders ('
                'id INTEGER PRIMARY KEY, '
                'gender TEXT)')

    conn.commit()
    

def create_locations_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS locations ('
                'id INTEGER PRIMARY KEY, '
                'address TEXT, '
                'county_id INTEGER, '
                'coordinates_id INTEGER, '
                'FOREIGN KEY (county_id) REFERENCES counties(id), '
                'FOREIGN KEY (coordinates_id) REFERENCES coordinates(id))')

    conn.commit()



def create_breeds_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS breeds ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'primary_breed TEXT)')

    conn.commit()

def create_counties_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS counties ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'county TEXT)')

    conn.commit()


def create_coordinates_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS coordinates ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'latitude FLOAT, '
                'longitude FLOAT)')

    conn.commit()

def create_tables(cur, conn):
    create_dogs_table(cur, conn)
    create_names_table(cur, conn)
    create_coordinates_table(cur, conn)
    create_counties_table(cur, conn)
    create_locations_table(cur, conn)
    create_breeds_table(cur, conn)
    create_ages_table(cur, conn)
    create_genders_table(cur, conn)


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

def get_county_info(lat, lng, MAPS_API_KEY):
    # Set the endpoint URL
    ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'

    # Request data
    params = {
        'latlng': str(lat) + ',' + str(lng),
        'key': MAPS_API_KEY
    }

    # Make the request
    r = requests.get(ENDPOINT, params=params)

    # Parse JSON response
    data = r.json()

    # Get the county
    county_info = data['results'][1]['address_components'][2]['long_name']

    return county_info


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
            pet_id = dog['id']
            name = dog['name']
            age = dog['age']
            gender = dog['gender']
            address1 = dog['contact']['address']['address1']
            address2 = dog['contact']['address']['address2']

            # Check if address1 is null
            if address1 is None:
                address1 = ''
            # Check if address2 is null
            if address2 is None:
                address2 = ''

            # Combine address1 and address2
            address = address1 + ' ' + address2

            city = dog['contact']['address']['city']
            state = dog['contact']['address']['state']
            postcode = dog['contact']['address']['postcode']

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
                    
                # Get the name_id
                cur.execute('SELECT id FROM names WHERE name = ?', (name,))
                name_id = cur.fetchone()

                # Check if name_id is null
                if name_id is None:
                    cur.execute('INSERT OR IGNORE INTO names (name) VALUES (?)', (name,))
                    cur.execute('SELECT id FROM names WHERE name = ?', (name,))
                    name_id = cur.fetchone()[0]
                else:
                    name_id = name_id[0]

                # Get age_id
                cur.execute('SELECT id FROM ages WHERE age = ?', (age,))
                age_id = cur.fetchone()

                # Check if age_id is null
                if age_id is None:
                    cur.execute('INSERT OR IGNORE INTO ages (age) VALUES (?)', (age,))
                    cur.execute('SELECT id FROM ages WHERE age = ?', (age,))
                    age_id = cur.fetchone()[0]
                else:
                    age_id = age_id[0]

                # Get latitude and longitude from Maps API
                latitude, longitude = get_coordinates(full_address, MAPS_API_KEY)

                # print(latitude, longitude)  # TODO

                # Get county from Maps API
                county = get_county_info(latitude, longitude, MAPS_API_KEY)

                # print(county)   # TODO

                # Get county_id
                cur.execute('SELECT id FROM counties WHERE county = ?', (county,))
                county_id = cur.fetchone()

                # If county_id is null, then insert a row into the counties table and get the county_id. Otherwise, get the county_id
                if county_id is None:
                    cur.execute('INSERT OR REPLACE INTO counties VALUES (NULL, ?)', (county,))
                    cur.execute('SELECT id FROM counties WHERE county = ?', (county,))
                    county_id = cur.fetchone()[0]
                else:
                    county_id = county_id[0]

                # Get coordinates_id
                cur.execute('SELECT id FROM coordinates WHERE latitude = ? AND longitude = ?', (latitude, longitude))
                coordinates_id = cur.fetchone()

                # Check if coordinates_id is null. If it is, then insert a row into the coordinates table and get the coordinates_id. Otherwise, get the coordinates_id
                if coordinates_id is None:
                    cur.execute('INSERT OR IGNORE INTO coordinates VALUES (NULL, ?, ?)', (latitude, longitude))
                    cur.execute('SELECT id FROM coordinates WHERE latitude = ? AND longitude = ?', (latitude, longitude))
                    coordinates_id = cur.fetchone()
                # else:
                coordinates_id = coordinates_id[0]

                # print('coordinates_id ' + str(coordinates_id))  # TODO

                # Get location_id from locations table
                cur.execute('SELECT id FROM locations WHERE address = ?', (full_address,))
                location_id = cur.fetchone()

                # Check if location_id is null. If it is, then insert a row into the locations table and get the location_id. Otherwise, get the location_id
                if location_id is None:
                    cur.execute('INSERT OR IGNORE INTO locations VALUES (NULL, :address, :county_id, :coordinates_id)',
                                {'address': full_address, 'county_id': county_id, 'coordinates_id': coordinates_id})
                    cur.execute('SELECT id FROM locations WHERE address = ? AND county_id = ? AND coordinates_id = ?', (full_address, county_id, coordinates_id))
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

                # Get gender_id
                cur.execute('SELECT id FROM genders WHERE gender = ?', (gender,))
                gender_id = cur.fetchone()

                # Check if gender_id is null. If it is, then insert a row.
                if gender_id is None:
                    cur.execute('INSERT OR IGNORE INTO genders VALUES (NULL, ?)', (gender,))
                    cur.execute('SELECT id FROM genders WHERE gender = ?', (gender,))
                    gender_id = cur.fetchone()[0]
                else:
                    gender_id = gender_id[0]

                # Insert a row into the dogs table
                cur.execute('INSERT OR IGNORE INTO dogs VALUES (?, ?, ?, ?, ?, ?)', 
                            (pet_id, name_id, age_id, gender_id, location_id, breed_id))

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

if __name__ == '__main__':
    # Get the petfinder token
    token = get_oAuth_token(API_KEY, SECRET_KEY)

    # Set up the database
    cur, conn = db_setup('dogs1.db')

    # Create the tables
    create_tables(cur, conn)

    # Run the query
    query_petfinder(token, 'Saginaw, MI', cur, conn)

    # Close the connection to the database
    conn.close()

    print('Done!')