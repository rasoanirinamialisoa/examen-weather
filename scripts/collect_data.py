import os
import requests
import pandas as pd

# Configuration
api_key = '0bc2a0cd6a0e1cde97a84544805bc849'
lat = '10.5880695'
lon = '14.2834604'
start = '1606223802'
end = '1606482999'
url = f'https://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start}&end={end}&appid={api_key}'

# Fonction pour récupérer les données
def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Lancer une exception en cas de réponse non valide
    return response.json()

# Fonction pour transformer les données JSON en DataFrame
def transform_data(data):
    records = []
    for item in data.get('list', []):
        record = {
            'dt': item.get('dt'),
            'aqi': item.get('main', {}).get('aqi'),
            'co': item.get('components', {}).get('co'),
            'no': item.get('components', {}).get('no'),
            'no2': item.get('components', {}).get('no2'),
            'o3': item.get('components', {}).get('o3'),
            'so2': item.get('components', {}).get('so2'),
            'pm2_5': item.get('components', {}).get('pm2_5'),
            'pm10': item.get('components', {}).get('pm10'),
            'nh3': item.get('components', {}).get('nh3')
        }
        records.append(record)
    return pd.DataFrame(records)

# Fonction principale
def main():
    print("Récupération des données...")
    data = fetch_data(url)
    print("Transformation des données...")
    df = transform_data(data)
    
   
    
    print("Sauvegarde des données dans un fichier CSV...")
    df.to_csv('../data/air_pollution_data.csv', index=False)
    print("Données sauvegardées dans '../data/air_pollution_data.csv'")

if __name__ == "__main__":
    main()
