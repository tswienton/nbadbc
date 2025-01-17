import nba_db.utils
import pandas as pd
import logging
from datetime import datetime
import os
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nba_db_logger")

def get_proxies():
    return pd.DataFrame(columns=['ip:port'])

nba_db.utils.get_proxies = get_proxies

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
    logger.info("Checking for database...")
    if not os.path.exists("nba-db/nba.sqlite"):
        logger.info("Database not found. Downloading...")
        download_db()
    else:
        logger.info("Database found.")

def check_db_connection():
    check_and_download_db()  # Make sure DB exists first
    try:
        conn = get_db_conn()
        result = pd.read_sql("SELECT COUNT(*) FROM game", conn)
        print(f"Number of games in DB: {result.iloc[0,0]}")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

def check_latest_game():
    conn = check_db_connection()  # This will ensure DB exists and is connected
    result = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn)
    print(f"Latest game date: {result.iloc[0,0]}")
    return result.iloc[0,0]

def daily():
    try:
        # First ensure we have the database
        check_and_download_db()
        
        # get db connection
        logger.info("Getting DB connection...")
        conn = get_db_conn()
        
        # Rest of daily function...
        [previous daily function code here]
