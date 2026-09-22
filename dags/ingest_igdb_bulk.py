import os
import time
import datetime
import logging
from dotenv import load_dotenv

from minio_utils import upload_to_minio, read_from_minio
from igdb_utils import IGDB_FIELDS, query_igdb, BASIC_FILTERING, get_igdb_token

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_all_igdb_games():
    max_records = 50000
    STATE_FILE = "raw/igdb/_metadata/initial_import_state.json"
    try:
        token = get_igdb_token()
        logging.info("Token IGDB récupéré avec succès.")
    except Exception as e:
        logging.error(f"Impossible de récupérer le token : {e}")
        return
    state = read_from_minio(STATE_FILE)
    if state:
        current_offset = state.get("offset", 0)
        total_fetched = state.get("total_fetched", 0)
        logging.info(f"Reprise de l'import : Offset {current_offset}")
    else:
        current_offset = 0
        total_fetched = 0
        logging.info("Premier lancement de l'import initial.")
    
    while True:
        if current_offset >= max_records:
            logging.warning(f"Limite d'OFFSET IGDB {max_records} atteinte.")
            break
        logging.info(f"Récupération du batch : Offset {current_offset}...")
        
        body = f"""
            fields {IGDB_FIELDS};
            where {BASIC_FILTERING};
            sort rating_count desc; 
            limit 500;
            offset {current_offset};
        """
        try:
            games = query_igdb(body, token)
            
            if not games:
                break
            s3_path = f"raw/igdb/initial_games/games_batch_{current_offset}.json"
            upload_to_minio(games, s3_path)
            
            current_offset += len(games)
            total_fetched += len(games)
            
            new_state = {
                "offset": current_offset,
                "total_fetched": total_fetched,
                "last_update": datetime.datetime.now().isoformat()
            }
            upload_to_minio(new_state, STATE_FILE)
            logging.info(f"Batch sauvé. Total récupéré : {total_fetched}")
            time.sleep(0.4)
        except Exception as e:
            logging.error(f"Erreur pendant l'import : {e}")
            break

if __name__ == "__main__":
    fetch_all_igdb_games()
