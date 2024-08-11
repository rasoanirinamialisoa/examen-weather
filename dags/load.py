import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging

logging.basicConfig(level=logging.INFO)

def load():
    logging.info('Début du chargement des données dans Google Sheets...')
    print('Début du chargement des données dans Google Sheets...')

    csv_path = '../data/air_pollution_combined.csv'
    print(f"Chemin du fichier CSV: {csv_path}")

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_path = '/home/mialy/key/airpollution-432110-a0fe7c75d98c.json'
    print(f"Chemin des credentials: {creds_path}")
    
    creds = Credentials.from_service_account_file(creds_path, scopes=scope)
    client = gspread.authorize(creds)
    print("Authentification réussie avec Google Sheets")

    spreadsheet_id = '1SmxsZSJbl_JRo-92S4LsTIycaH5UGNO6UAgHMTVggGo'
    print(f"ID de la feuille de calcul: {spreadsheet_id}")
    
    spreadsheet = client.open_by_key(spreadsheet_id)
    sheet = spreadsheet.sheet1
    print("Feuille de calcul ouverte avec succès")

    df = pd.read_csv(csv_path)
    print(f"Colonnes du DataFrame: {df.columns.tolist()}")
    print(f"Nombre de lignes lues depuis le CSV: {len(df)}")

    data = df.values.tolist()
    print(f"Exemple de données à charger: {data[:5]}")

    sheet.clear()
    print("Feuille de calcul effacée")
    
    try:
        sheet.update([df.columns.tolist()] + data)
        print("Données chargées dans Google Sheets")
    except Exception as e:
        logging.error(f"Erreur lors de la mise à jour de Google Sheets: {e}")
        print(f"Erreur lors de la mise à jour de Google Sheets: {e}")

if __name__ == "__main__":
    load()
