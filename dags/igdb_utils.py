import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
CLIENT_SECRET = os.getenv("IGDB_CLIENT_SECRET")

IGDB_FIELDS = """id,
            name,
            slug,
            summary,
            storyline,
            
            similar_games.name,
            remakes.name,
            remasters.name,
            expansions.name,
            standalone_expansions.name,
            parent_game.name,
            version_parent.name,
            
            franchises.name,
            collection.name,
            
            involved_companies.company.name,
            involved_companies.developer,
            involved_companies.publisher,
            
            genres.name,
            themes.name,
            game_modes.name,
            player_perspectives.name,
            keywords.name,
            game_engines.name,
            
            platforms.name,
            platforms.abbreviation,
            
            first_release_date,
            release_dates.human,
            release_dates.platform.name,
            updated_at,
            rating,
            rating_count,
            aggregated_rating,
            aggregated_rating_count,
            total_rating,
            total_rating_count,
            
            hypes,
            follows,
            
            age_ratings.rating,
            age_ratings.category,
            external_games.external_game_source.name,
            external_games.uid"""

BASIC_FILTERING = """
game_type=(0,4,8,9) & external_games.external_game_source=1
"""

def get_igdb_token():
    client_id = os.getenv("IGDB_CLIENT_ID") or CLIENT_ID
    client_secret = os.getenv("IGDB_CLIENT_SECRET") or CLIENT_SECRET
    if not client_id or not client_secret:
        raise ValueError("Missing IGDB_CLIENT_ID or IGDB_CLIENT_SECRET in environment variables.")
    auth_url = f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    response = requests.post(auth_url)
    response.raise_for_status()
    return response.json()['access_token']

def query_igdb(query_body, access_token):
    client_id = os.getenv("IGDB_CLIENT_ID") or CLIENT_ID
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}',
    }
    url = 'https://api.igdb.com/v4/games'
    response = requests.post(url, headers=headers, data=query_body)
    
    if response.status_code != 200:
        print(f"Erreur IGDB : {response.status_code} - {response.text}")
        return []
    return response.json()