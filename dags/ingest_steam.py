import os
import time 
import random
import requests
import json
import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "data")
SEED_STEAM_ID = os.getenv("SEED_STEAM_ID")

def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", MINIO_ENDPOINT)
    access_key = os.getenv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
    return boto3.client(
        's3', 
        endpoint_url=endpoint, 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key
    )

def scrape_steam_sequential(**kwargs):
    api_key = os.getenv("STEAM_API_KEY") or STEAM_API_KEY
    seed_id = os.getenv("SEED_STEAM_ID") or SEED_STEAM_ID
    bucket = os.getenv("MINIO_BUCKET_NAME", BUCKET_NAME)

    if not api_key:
        raise ValueError("Missing STEAM_API_KEY in environment variables.")
    if not seed_id:
        raise ValueError("Missing SEED_STEAM_ID in environment variables.")

    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        try:
            s3.create_bucket(Bucket=bucket)
        except Exception as e:
            print(f"Bucket '{bucket}' notice: {e}")
    
    try:
        q_obj = s3.get_object(Bucket=bucket, Key='raw/steam/_metadata/queue.json')
        queue = set(json.loads(q_obj['Body'].read().decode('utf-8')))
    except s3.exceptions.NoSuchKey:
        print(f"Initialisation de la queue avec l'ID Seed : {seed_id}")
        queue = {seed_id}
    
    try:
        h_obj = s3.get_object(Bucket=bucket, Key='raw/steam/_metadata/processed_ids.json')
        processed_ids = set(json.loads(h_obj['Body'].read().decode('utf-8')))
    except s3.exceptions.NoSuchKey:
        processed_ids = set()
    known_ids = queue | processed_ids
    candidates = list(queue - processed_ids)
    if not candidates:
        print("Queue vide.")
        return

    batch = candidates[:50]
    new_discovered_ids = set()
    successfully_processed_in_this_run = []
    total_saved = 0
    print(f"Nombre d'utilisateurs déjà traités : {len(processed_ids)}")
    print(f"Nombre d'utilisateurs dans la queue : {len(queue)}")
    print(f"Début du batch : {len(batch)} utilisateurs")

    for steam_id in batch:
        print(f"--- Profil {steam_id} ---")
        
        try:
            g_url = "http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/"
            g_res = requests.get(g_url, params={'key': api_key, 'steamid': steam_id, 'format': 'json', 'include_played_free_games': 'true', 'include_appinfo':'true'}, timeout=15)
            
            if g_res.status_code == 429:
                print("429 detecté sur GetOwnedGames")
                raise AirflowException("Rate limit Steam atteint (429).")
            
            g_data = g_res.json().get('response', {})
            if 'games' in g_data and g_data['games']:
                s3.put_object(
                    Bucket=bucket,
                    Key=f"raw/steam/users_library/{steam_id}.json",
                    Body=json.dumps({'steam_id': steam_id, 'games': g_data['games'], 'scraped_at': datetime.now().isoformat()})
                )
                total_saved += 1
                print(f"Jeux sauvegardés pour {steam_id}")
            else:
                print(f"Profil {steam_id} privé ou bibliothèque vide (Skip).")

            time.sleep(random.uniform(0.25, 0.4)) 

            f_url = "http://api.steampowered.com/ISteamUser/GetFriendList/v0001/"
            f_res = requests.get(f_url, params={'key': api_key, 'steamid': steam_id}, timeout=15)
            
            if f_res.status_code == 429:
                print("429 detecté sur GetFriendList")
                raise AirflowException("Rate limit Steam atteint (429).")
            
            if f_res.status_code == 401:
                print(f"Liste d'amis privée (401) pour {steam_id}. On continue.")
            elif f_res.status_code == 200:
                friends_data = f_res.json().get('friendslist', {}).get('friends', [])
                
                friend_ids = {f['steamid'] for f in friends_data}
                
                new_ids_from_this_friend = friend_ids - known_ids
                
                new_discovered_ids.update(new_ids_from_this_friend)
                
                known_ids.update(new_ids_from_this_friend)
                
                print(f"Amis totaux: {len(friend_ids)} | Nouveaux profils découverts: {len(new_ids_from_this_friend)}")

            successfully_processed_in_this_run.append(steam_id)
            
            time.sleep(random.uniform(0.25, 0.4))

        except AirflowException as ae:
            save_progress(s3, bucket, processed_ids, successfully_processed_in_this_run, queue, new_discovered_ids)
            raise ae
        except Exception as e:
            print(f"⚠️ Erreur non-critique sur {steam_id}: {e}")
            successfully_processed_in_this_run.append(steam_id)
            continue

    save_progress(s3, bucket, processed_ids, successfully_processed_in_this_run, queue, new_discovered_ids)

    ti = kwargs['ti']
    ti.xcom_push(key='profiles_saved_this_run', value=total_saved)

def save_progress(s3, bucket, old_processed, newly_processed, old_queue, discovered):
    """Fonction utilitaire pour mettre à jour les fichiers JSON sur MinIO"""
    updated_processed = old_processed | set(newly_processed)
    updated_queue = (old_queue | discovered) - updated_processed
    
    queue_list = list(updated_queue)[:10000000]
    
    s3.put_object(Bucket=bucket, Key='raw/steam/_metadata/processed_ids.json', Body=json.dumps(list(updated_processed)))
    s3.put_object(Bucket=bucket, Key='raw/steam/_metadata/queue.json', Body=json.dumps(queue_list))
    print(f"💾 État sauvegardé : {len(newly_processed)} traités, {len(queue_list)} en queue.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'steam_crawler_optimized',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    task = PythonOperator(
        task_id='scrape_steam',
        python_callable=scrape_steam_sequential
    )