import os
import sqlite3

import requests

# Brandon's API/Secret key
API_KEY = '8rNWQrEyOuNVDs1EtEmRKpkqGPhXMfErpAMxI1uSqfWwfM1CWs'
SECRET_KEY = 'O39nMA96aRPvNyERjDnWrcVufBnPU1axEfZQZ8j7'


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


def get_adopted_dogs(token, location):
    # Set the endpoint URL
    ENDPOINT = 'https://api.petfinder.com/v2/animals'

    # Request data
    params = {
        'type': 'dog',
        'location': location,
        'limit': 25,  # LIMIT RESULTS TO 25
        'after': '2020-01-01T00:00:00Z'
    }

    # Set the token in the header
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # Empty list to store the dogs
    dogs = []

    # Keep making requests until we have all the dogs since 2020
    while True:
        # Make the request
        r = requests.get(ENDPOINT, params=params, headers=headers)

        # Check the status code of the response
        if r.status_code != 200:
            print(f'Request failed with status code {r.status_code}')
            break

        # Add the dogs from the response to the list
        data = r.json()
        dogs.extend(data['animals'])

        # If there are more dogs available, update the pagination parameters
        # and make another request. Otherwise, break.
        if data['pagination']['total_pages'] > data['pagination']['current_page']:
            params['page'] = data['pagination']['current_page'] + 1
        else:
            break

    # Return the list of dogs
    return dogs


def add_dogs_to_db(dogs):
    # Connect to the database
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + '/' + 'adopted_dogs.db')
    cur = conn.cursor()

    # Create the table if it does not already exist
    cur.execute('CREATE TABLE IF NOT EXISTS dogs (id INTEGER PRIMARY KEY, name TEXT, breed TEXT, age TEXT)')

    # Add each dog to the table
    for dog in dogs:
        cur.execute('INSERT INTO dogs (id, name, age) VALUES (?, ?, ?)', (dog['id'], dog['name'], dog['age']))

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()


if __name__ == '__main__':
    # Get the token
    token = get_oauth_token(API_KEY, SECRET_KEY)

    # Get the dogs
    dogs = get_adopted_dogs(token, 'Ann Arbor, MI')

    # Add the dogs to the database
    add_dogs_to_db(dogs)
