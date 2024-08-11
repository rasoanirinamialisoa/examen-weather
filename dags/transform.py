import os
import json
import pandas as pd
from datetime import datetime
import logging
from extract import get_cities

logging.basicConfig(level=logging.INFO)

cities = get_cities()

demographic_data_path = '../data/demographic_data.csv'
geographic_data_path = '../data/geographic_data.csv'

demographic_df = pd.read_csv(demographic_data_path)
geographic_df = pd.read_csv(geographic_data_path)

print("Colonnes de demographic_df:", demographic_df.columns)
print("Colonnes de geographic_df:", geographic_df.columns)

print("Données démographiques:")
print(demographic_df.head())

print("Données géographiques:")
print(geographic_df.head())

demo_geo_df = pd.merge(demographic_df, geographic_df, on='Location', how='left')

def transform_data(data, city_name, lat, lon):
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
            'Location': city_name,
            'Latitude': lat,
            'Longitude': lon
        }
        records.append(record)
    
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date_time']).dt.date
    df['hour'] = pd.to_datetime(df['date_time']).dt.hour
    
    df = df[df['hour'] == 12].drop(['hour'], axis=1)
    
    return df

def transform():
    all_data = []
    for city in ['paris', 'los_angeles', 'lima', 'nairobi', 'tokyo', 'antananarivo']:
        json_path = f'../data/{city}_pollution.json'
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        print(f"Transformation des données pour {city}...")
        city_info = next((c for c in cities if c['name'].lower() == city), None)
        if city_info:
            df = transform_data(data, city_info['name'], city_info['lat'], city_info['lon'])
            final_combined_data = pd.merge(df, demo_geo_df, on='Location', how='left')
            
            print(f"Final combined data pour {city}:")
            print(final_combined_data.head())
            
            final_combined_data = final_combined_data.drop_duplicates()
            all_data.append(final_combined_data)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    combined_df = combined_df.replace([float('inf'), -float('inf')], pd.NA)
    combined_df = combined_df.fillna(0) 
    
    csv_path = '../data/air_pollution_combined.csv'
    print(f"Sauvegarde des données combinées dans le fichier CSV '{csv_path}'...")
    combined_df.to_csv(csv_path, index=False)
    print(f"Données combinées sauvegardées dans '{csv_path}'")
    logging.info(f"Données combinées sauvegardées dans '{csv_path}'")

if __name__ == "__main__":
    transform()
