import sqlite3
import json
import os
import requests

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
                # Unique Primary ID
                'id INTEGER PRIMARY KEY,'
                'name TEXT, '
                'age TEXT, '
                'gender TEXT, '
                'city TEXT,'
                'state TEXT,'
                'primary_breed TEXT, '
                'secondary_breed TEXT, '
                'mixed_breed TEXT,'
                'unknown_breed TEXT)')

    conn.commit()


def get_oauth_token(API_KEY, SECRET_KEY):
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


def query_petfinder(token, cityState, cur, conn):
    # Set the endpoint URL
    ENDPOINT = 'https://api.petfinder.com/v2/animals'

    # Keep track of page we're on
    page = 1

    # Flag indicating if we've reached the last page
    has_more_pages = True

    # Loop until we we reach last page
    while has_more_pages:
        # Request data
        params = {
            'type': 'dog',
            'page': page,
            'location': cityState,
            'distance': 150,
            'limit': 25  # LIMITED RESULTS TO 25
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
            name = dog['name']
            age = dog['age']
            gender = dog['gender']
            location = dog['contact']['address']
            city = location['city']
            state = location['state']
            primary_breed = dog['breeds']['primary']
            secondary_breed = dog['breeds']['secondary']
            mixed_breed = dog['breeds']['mixed']  # BOOLEAN
            unknown_breed = dog['breeds']['unknown']  # BOOLEAN

            # Check if dog is from MI
            if state == 'MI':
                # Insert a row into the dogs table
                cur.execute('INSERT OR IGNORE INTO dogs VALUES (?,?,?,?,?,?,?,?,?,?)',
                            (id, name, age, gender, city, state, primary_breed, secondary_breed, mixed_breed, unknown_breed))

        # Check if there are more pages
        has_more_pages = data['pagination']['total_pages'] > page

        # Increment the page number
        page += 1

    # Commit the changes to the database
    conn.commit()


if __name__ == '__main__':
    # Get the token
    token = get_oauth_token(API_KEY, SECRET_KEY)

    # Set up the database
    cur, conn = db_setup('dogs.db')

    # Create the dogs table
    create_dogs_table(cur, conn)

    # Query the API
    query_petfinder(token, 'Saginaw, MI', cur, conn)

    # Close the connection to the database
    conn.close()

    print('Done!')
