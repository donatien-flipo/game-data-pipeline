import os
import time
import datetime
import logging
import requests
from dotenv import load_dotenv

from minio_utils import upload_to_minio, read_from_minio

load_dotenv()

RAWG_KEY = os.getenv("RAWG_API_KEY")
STATE_FILE = "raw/rawg/_metadata/initial_import_state.json"
PAGE_SIZE = 40  
MAX_PAGES = 300

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_all_rawg_games():
    rawg_key = os.getenv("RAWG_API_KEY") or RAWG_KEY
    if not rawg_key:
        raise ValueError("Missing RAWG_API_KEY in environment variables.")

    state = read_from_minio(STATE_FILE)
    if state:
        current_page = state.get("page", 1)
        total_fetched = state.get("total_fetched", 0)
        logging.info(f"Reprise de l'import RAWG : Page {current_page}")
    else:
        current_page = 1
        total_fetched = 0
        logging.info("Premier lancement de l'import initial RAWG.")

    while current_page <= MAX_PAGES:
        logging.info(f"Récupération RAWG : Page {current_page}...")
        
        url = f"https://api.rawg.io/api/games?key={rawg_key}&page={current_page}&page_size={PAGE_SIZE}&ordering=-added"
        
        try:
            response = requests.get(url)
            
            if response.status_code == 429:
                logging.error("Rate limit atteint (429).")
                break
                
            if response.status_code != 200:
                logging.error(f"Erreur API RAWG : {response.status_code} - {response.text}")
                break

            data = response.json()
            games = data.get('results', [])

            if not games:
                logging.info("Fin des données disponibles sur RAWG.")
                break

            s3_path = f"raw/rawg/initial_games/games_page_{current_page}.json"
            upload_to_minio(games, s3_path)
            
            total_fetched += len(games)
            
            new_state = {
                "page": current_page + 1, 
                "total_fetched": total_fetched,
                "last_update": datetime.datetime.now().isoformat()
            }
            upload_to_minio(new_state, STATE_FILE)
            
            logging.info(f"Page {current_page} sauvegardée. Total cumulé : {total_fetched} jeux.")
            
            current_page += 1
            
            time.sleep(0.3) 

        except Exception as e:
            logging.error(f"Erreur pendant l'import RAWG : {e}")
            break

    if current_page > MAX_PAGES:
        logging.info(f"Limite de pages fixée ({MAX_PAGES}) atteinte.")

if __name__ == "__main__":
    fetch_all_rawg_games()
