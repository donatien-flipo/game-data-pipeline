import os
import requests
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

from minio_utils import upload_to_minio

load_dotenv()

RAWG_KEY = os.getenv("RAWG_API_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_rawg_daily(execution_date=None):
    rawg_key = os.getenv("RAWG_API_KEY") or RAWG_KEY
    if not rawg_key:
        raise ValueError("Missing RAWG_API_KEY in environment variables.")

    if execution_date:
        logical_dt = datetime.strptime(execution_date, "%Y%m%d")
    else:
        logical_dt = datetime.now()
        
    yesterday = logical_dt - timedelta(days=1)
    
    date_to = logical_dt.strftime("%Y-%m-%d")
    date_from = yesterday.strftime("%Y-%m-%d")
    
    date_folder = logical_dt.strftime("%Y%m%d")
    
    logging.info(f"🚀 RAWG : Récupération des mises à jour entre {date_from} et {date_to}")

    all_updated_games = []
    page = 1
    
    while True:
        url = f"https://api.rawg.io/api/games?key={rawg_key}&updated={date_from},{date_to}&ordering=-updated&page_size=40&page={page}"
        
        try:
            response = requests.get(url)
            
            if response.status_code == 429:
                logging.error("Rate limit RAWG atteint. ")
                time.sleep(60)
                continue
                
            if response.status_code != 200:
                logging.error(f"Erreur API RAWG : {response.status_code}")
                break

            data = response.json()
            games = data.get('results', [])

            if not games:
                break

            all_updated_games.extend(games)
            logging.info(f"Page {page} récupérée ({len(games)} jeux au total)")

            if len(games) < 40:
                break
            if page > 10:
                break
            page += 1
            time.sleep(1) 

        except Exception as e:
            logging.error(f"💥 Erreur : {e}")
            break

    if all_updated_games:
        s3_path = f"raw/rawg/daily_games/{date_folder}/games.json"
        upload_to_minio(all_updated_games, s3_path)
        logging.info(f"Terminé : {len(all_updated_games)} jeux RAWG sauvegardés.")
    else:
        logging.info("Aucun jeu mis à jour sur RAWG aujourd'hui.")

if __name__ == "__main__":
    ingest_rawg_daily()