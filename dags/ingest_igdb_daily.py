import os
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

from minio_utils import upload_to_minio
from igdb_utils import IGDB_FIELDS, query_igdb, BASIC_FILTERING, get_igdb_token

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_igdb_daily(execution_date=None):
    if execution_date:
        logical_dt = datetime.strptime(execution_date, "%Y%m%d")
    else:
        logical_dt = datetime.now()
        
    date_str = logical_dt.strftime("%Y%m%d")
    days_back = 1
    dt = logical_dt - timedelta(days=days_back)
    timestamp = int(dt.timestamp())
    all_games = []
    offset = 0

    try:
        token = get_igdb_token()
        logging.info("Token IGDB récupéré avec succès.")
    except Exception as e:
        logging.error(f"Impossible de récupérer le token : {e}")
        return

    while True:
        body = f"""
            fields {IGDB_FIELDS};
            where {BASIC_FILTERING} & updated_at > {timestamp} & rating_count > 10;
            sort updated_at desc;
            limit 500;
            offset {offset};
        """
        try:
            games = query_igdb(body, token)

            if not games:
                break
            all_games.extend(games)
            if len(games) < 500:
                break
            offset += 500
            if offset > 10000:
                logging.warning("Limite d'offset IGDB atteinte.")
                break
            time.sleep(0.4)
        except Exception as e:
            logging.error(f"Erreur lors de l'ingestion quotidienne IGDB offset {offset} : {e}")
            raise

    if all_games:
        s3_path = f"raw/igdb/daily_games/{date_str}/games.json"
        upload_to_minio(all_games, s3_path)
        logging.info(f"Succès. {len(all_games)} jeux stockés dans {s3_path}")
    else:
        logging.info("Aucun jeu mis à jour trouvé sur cette période.")

if __name__ == "__main__":
    ingest_igdb_daily()
