import sqlite3
import json
import os
import requests

# DOGS:
# id PRIMARY KEY (integer)
# name (text)
# age_id (text) 
# gender_id (integer)
# location_id (integer)
# breed_id (integer)
# FOREIGN KEY (age_id) REFERENCES ages(id)
# FOREIGN KEY (gender_id) REFERENCES genders(id)
# FOREIGN KEY (location_id) REFERENCES locations(id)
# FOREIGN KEY (breed_id) REFERENCES breeds(id)

# AGES:
# id PRIMARY KEY AUTOINCREMENT (integer)
# age (text) (ex. 'Adult', 'Young', 'Senior', 'Baby')

# GENDERS:
# id PRIMARY KEY AUTOINCREMENT (integer)
# gender (text)

# LOCATIONS:
# id PRIMARY KEY (integer)
# shelter_name (text)
# address (text)
# city (text)
# state (text)
# postcode (text)
# county_id (integer)
# FOREIGN KEY (county_id) REFERENCES counties(id)
# latitude (float)
# longitude (float)

# BREEDS:
# id PRIMARY KEY AUTOINCREMENT (integer)
# primary_breed (text)
# secondary_breed (text)
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
                'name TEXT, '
                'age_id TEXT, '
                'gender_id TEXT, '
                'location_id INTEGER, '
                'breed_id INTEGER, '
                'FOREIGN KEY (location_id) REFERENCES locations(id), '
                'FOREIGN KEY (breed_id) REFERENCES breeds(id))')

    conn.commit()


def create_ages_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS ages ('
                'id INTEGER PRIMARY KEY, '
                'age TEXT)')

    cur.execute('INSERT INTO ages (id, age) VALUES (1, "Baby"), (2, "Young"), (3, "Adult"), (4, "Senior")')

    conn.commit()



def create_genders_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS genders ('
                'id INTEGER PRIMARY KEY, '
                'gender TEXT)')
    cur.execute('INSERT INTO genders (id, gender) VALUES (1, "Female"), (2, "Male")')

    conn.commit()
    

def create_locations_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS locations ('
                'id INTEGER PRIMARY KEY, '
                'number_of_dogs INTEGER, '
                'address TEXT, '
                'city TEXT, '
                'state TEXT, '
                'postcode TEXT, '
                'county_id INTEGER, '
                'latitude FLOAT, '
                'longitude FLOAT, '
                'FOREIGN KEY (county_id) REFERENCES counties(id))')

    conn.commit()



def create_breeds_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS breeds ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'primary_breed TEXT, '
                'secondary_breed TEXT, '
                'num_dogs_of_breed INTEGER)')

    conn.commit()

def create_counties_table(cur, conn):
    cur.execute('CREATE TABLE IF NOT EXISTS counties ('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, '
                'county TEXT)')

    conn.commit()


def create_tables(cur, conn):
    create_dogs_table(cur, conn)
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
    county_id = data['results'][1]['address_components'][2]['long_name']

    # # Get the county id
    # cur.execute('SELECT id FROM counties WHERE county = ?', (county,))
    # county_id = cur.fetchone()[0]

    return county_id


def query_petfinder(token, cityState, cur, conn):
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
            'location': cityState,
            'distance': 290,        # Calculated Cadillac, MI to be possible center of State when factoring in the UP  
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
            primary_breed = dog['breeds']['primary']
            secondary_breed = dog['breeds']['secondary']

            # Check if dog is from MI
            if state == 'MI':
                
                # Get age_id
                cur.execute('SELECT id FROM ages WHERE age = ?', (age,))
                age_id = cur.fetchone()[0]

                # Get gender_id
                cur.execute('SELECT id FROM genders WHERE gender = ?', (gender,))
                gender_id = cur.fetchone()[0]

                # Use full address to get coordinates from Google Maps API
                full_address = json.dumps(dog['contact']['address'])
                latitude, longitude = get_coordinates(full_address, MAPS_API_KEY)

                # Get county_id
                county_id = get_county_info(latitude, longitude, MAPS_API_KEY)

                # Get location_id by latitude and longitude because some locations don't have an address
                # Increment the number of dogs at this location if the location already exists and if not insert a new row
                cur.execute('SELECT id FROM locations WHERE latitude = ? AND longitude = ?', (latitude, longitude))
                location_id = cur.fetchone()

                # Check if location_id is null
                if location_id is None:
                    # Insert a row into the locations table if the location doesn't already exist
                    cur.execute('INSERT OR REPLACE INTO locations VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (1, address, city, state, postcode, county_id, latitude, longitude))
                else:
                    # Increment the number of dogs at this location
                    cur.execute('UPDATE locations SET number_of_dogs = number_of_dogs + 1 WHERE id = ?', (location_id))

                # Select breed_id from breeds table, if breed doesn't exist insert a new row
                cur.execute('SELECT id FROM breeds WHERE primary_breed = ? AND secondary_breed = ?', (primary_breed, secondary_breed))
                breed_id = cur.fetchone()

                if breed_id:
                    # Increment the number of dogs of this breed combination
                    cur.execute('UPDATE breeds SET num_dogs_of_breed = num_dogs_of_breed + 1 WHERE id = ?', (breed_id))
                else:
                    # Insert a row into the breeds table
                    cur.execute('INSERT OR REPLACE INTO breeds VALUES (NULL, ?, ?, ?)', (primary_breed, secondary_breed, 1))

                # Get gender_id
                cur.execute('SELECT id FROM genders WHERE gender = ?', (gender,))
                gender_id = cur.fetchone()[0]


                if location_id is None:
                    location_id = "NULL"
                else:
                    location_id = location_id[0]

                if breed_id is None:
                    breed_id = "NULL"
                else:
                    breed_id = breed_id[0]

                # Insert a row into the dogs table
                cur.execute('INSERT OR IGNORE INTO dogs VALUES (?, ?, ?, ?, ?, ?)', 
                            (pet_id, name, age_id, gender_id, location_id, breed_id))
                # # Insert a row into the dogs table
                # cur.execute('INSERT OR IGNORE INTO dogs VALUES (?, ?, ?, ?, ?, ?)', 
                #             (pet_id, name, age_id, gender_id, breed_id, location_id))

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

if __name__ == '__main__':
    # Get the petfinder token
    token = get_oAuth_token(API_KEY, SECRET_KEY)

    # Set up the database
    cur, conn = db_setup('adoptions.db')

    # Create the tables TODO: Does the dogs table need to be created here?
    create_tables(cur, conn)

    # Run the query
    query_petfinder(token, 'Cadillac, MI', cur, conn)

    # Close the connection to the database
    conn.close()

    print('Done!')
