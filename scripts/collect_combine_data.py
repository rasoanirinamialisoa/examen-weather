import os
import requests
import pandas as pd
from datetime import datetime

demographic_data_path = '../data/demographic_data.csv'
geographic_data_path = '../data/geographic_data.csv'

demographic_df = pd.read_csv(demographic_data_path)
geographic_df = pd.read_csv(geographic_data_path)

demo_geo_df = pd.merge(demographic_df, geographic_df, on='Location', how='left')

cities = [
    {'name': 'Paris', 'lat': '48.866667', 'lon': '2.333333', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Los Angeles', 'lat': '34.0536909', 'lon': '-118.242766', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Lima', 'lat': '-12.0432', 'lon': '-77.0282', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Nairobi', 'lat': '-1.30326415', 'lon': '36.8263840993416', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Tokyo', 'lat': '35.652832', 'lon': '139.839478', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'},
    {'name': 'Antananarivo', 'lat': '-18.77914875', 'lon': '46.7121716165673', 'api_url': 'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'}
]

def fetch_data(url, lat, lon, start, end, api_key):
    start_timestamp = int(start.timestamp())
    end_timestamp = int(end.timestamp())
    formatted_url = url.format(lat=lat, lon=lon, start=start_timestamp, end=end_timestamp, api_key=api_key)
    print(f"Request URL: {formatted_url}")  
    response = requests.get(formatted_url)
    response.raise_for_status()
    return response.json()

def transform_data(data, city_name):
    records = []
    for item in data.get('list', []):
        timestamp = item.get('dt')
        date_time = datetime.utcfromtimestamp(timestamp)
        record = {
            'date_time': date_time.strftime('%Y-%m-%d %H:%M:%S'),
            'aqi': item.get('main', {}).get('aqi'),
            'co': item.get('components', {}).get('co'),
            'no': item.get('components', {}).get('no'),
            'no2': item.get('components', {}).get('no2'),
            'o3': item.get('components', {}).get('o3'),
            'so2': item.get('components', {}).get('so2'),
            'pm2_5': item.get('components', {}).get('pm2_5'),
            'pm10': item.get('components', {}).get('pm10'),
            'nh3': item.get('components', {}).get('nh3'),
            'Location': city_name
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date_time']).dt.date
    df['hour'] = pd.to_datetime(df['date_time']).dt.hour
    
    df = df[df['hour'] == 12].drop(['hour'], axis=1)
    
    return df

def combine_data(pollution_df, demo_geo_df):
    final_combined_data = pd.merge(pollution_df, demo_geo_df, on='Location', how='left')
    return final_combined_data

def main():
    api_key = '0bc2a0cd6a0e1cde97a84544805bc849'
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 12, 31, 23, 59, 59)

    if not os.path.exists('../data'):
        os.makedirs('../data')

    for city in cities:
        print(f"Récupération des données pour {city['name']}...")
        data = fetch_data(city['api_url'], city['lat'], city['lon'], start, end, api_key)
        print(f"Transformation des données pour {city['name']}...")
        df = transform_data(data, city['name'])

        print(f"Combinaison des données pour {city['name']}...")
        final_combined_data = combine_data(df, demo_geo_df)

        final_combined_data = final_combined_data.drop_duplicates()

        csv_path = f'../data/air_pollution_{city["name"].lower().replace(" ", "_")}.csv'
        print(f"Sauvegarde des données combinées dans le fichier CSV '{csv_path}'...")
        final_combined_data.to_csv(csv_path, index=False)
        print(f"Données combinées sauvegardées dans '{csv_path}'")

if __name__ == "__main__":
    main()
