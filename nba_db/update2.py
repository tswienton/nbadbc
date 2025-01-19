import nba_db.utils
import pandas as pd
import logging
from datetime import datetime
import os
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nba_db_logger")

# Define our simple proxy function
def get_proxies():
    return pd.DataFrame(columns=['ip:port'])

# Override the original get_proxies
nba_db.utils.get_proxies = get_proxies

# Import necessary functions from the original codebase
from nba_db.extract import (
    get_box_score_summaries,
    get_league_game_log_from_date,
    get_play_by_play
)
from nba_db.utils import (
    download_db,
    dump_db,
    get_db_conn,
    upload_new_db_version
)

def check_and_download_db():
    print("Checking for database...")
    if not os.path.exists("nba-db/nba.sqlite"):
        print("Database not found. Downloading...")
        download_db()
    else:
        print("Database found.")

def check_db_connection():
    check_and_download_db()
    try:
        conn = get_db_conn()
        result = pd.read_sql("SELECT COUNT(*) FROM game", conn)
        print(f"Number of games in DB: {result.iloc[0,0]}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

def check_latest_game():
    conn = check_db_connection()
    result = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn)
    print(f"Latest game date: {result.iloc[0,0]}")
    return result.iloc[0,0]

def daily():
    try:
        print("Starting daily update process...")
        
        # First ensure we have the database
        check_and_download_db()
        
        # get db connection
        print("Getting DB connection...")
        conn = get_db_conn()
        
        # Rest of the code up to box score processing stays the same...
        
        # Modified box score and play by play section
        print("Getting box scores...")
        successful_games = []
        for game_id in games:
            try:
                result = get_box_score_summaries([game_id], proxies=get_proxies(), save_to_db=True, conn=conn)
                if result is not None:
                    successful_games.append(game_id)
            except Exception as e:
                print(f"Failed to get box score for game {game_id}: {str(e)}")
                continue
                
        print(f"Successfully processed {len(successful_games)} out of {len(games)} games")
        
        if successful_games:
            print("Getting play by play...")
            for game_id in successful_games:
                try:
                    get_play_by_play([game_id], proxies=get_proxies(), save_to_db=True, conn=conn)
                except Exception as e:
                    print(f"Failed to get play by play for game {game_id}: {str(e)}")
                    continue
                    
        # dump db tables to csv
        print("Dumping DB to CSV...")
        dump_db(conn)
        
        # upload new db version to Kaggle
        version_message = f"Daily update: {pd.to_datetime('today').strftime('%Y-%m-%d')}"
        print("Uploading new DB version...")
        upload_new_db_version(version_message)
        
        # close db connection
        conn.close()
        print("Update completed successfully")
        
    except Exception as e:
        print(f"Error during update: {str(e)}")
        raise

if __name__ == "__main__":
    daily()
