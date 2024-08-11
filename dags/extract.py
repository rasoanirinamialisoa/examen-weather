import os
import requests
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

demographic_data_path = '../data/demographic_data.csv'
geographic_data_path = '../data/geographic_data.csv'

cities = [
    {'name': 'Paris', 'lat': '48.866667', 'lon': '2.333333', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Los_Angeles', 'lat': '34.0536909', 'lon': '-118.242766', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Lima', 'lat': '-12.0432', 'lon': '-77.0282', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Nairobi', 'lat': '-1.30326415', 'lon': '36.8263840993416', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Tokyo', 'lat': '35.652832', 'lon': '139.839478', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Antananarivo', 'lat': '-18.77914875', 'lon': '46.7121716165673', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'}
]

def fetch_data(url, lat, lon, start, end, api_key):
    start_timestamp = int(start.timestamp())
    end_timestamp = int(end.timestamp())
    formatted_url = url.format(lat=lat, lon=lon, start=start_timestamp, end=end_timestamp, api_key=api_key)
    response = requests.get(formatted_url)
    response.raise_for_status()
    return response.json()

def extract():
    api_key = '0bc2a0cd6a0e1cde97a84544805bc849'
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 12, 31, 23, 59, 59)

    if not os.path.exists('../data'):
        os.makedirs('../data')

    for city in cities:
        print(f"Récupération des données pour {city['name']}...")
        data = fetch_data(city['api_url'], city['lat'], city['lon'], start, end, api_key)
        
        json_path = f'../data/{city["name"].lower()}_pollution.json'
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Données sauvegardées dans '{json_path}'")
        logging.info(f"Données sauvegardées dans '{json_path}'")

def get_cities():
    return cities