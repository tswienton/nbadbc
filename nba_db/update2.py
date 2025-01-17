import nba_db.utils
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nba_db_logger")

# Mock the proxies function
def get_proxies():
    return pd.DataFrame(columns=['ip:port'])

# Inject our proxy-free function
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

def daily():
    try:
        # download db from Kaggle
        logger.info("Downloading DB...")
        download_db()
        
        # get db connection
        logger.info("Getting DB connection...")
        conn = get_db_conn()
        
        # get latest date in db and add a day
        logger.info("Getting latest date from DB...")
        latest_db_date = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn).iloc[0, 0]
        logger.info(f"Latest date in DB: {latest_db_date}")
        
        # check if today is a game day
        if pd.to_datetime(latest_db_date) >= pd.to_datetime(datetime.today().date()):
            logger.info("No new games today. Exiting...")
            return
            
        # add a day to latest db date
        latest_db_date = (pd.to_datetime(latest_db_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"Fetching games from: {latest_db_date}")
        
        # get new games and add to db
        logger.info("Getting league game log...")
        df = get_league_game_log_from_date(latest_db_date, proxies=get_proxies(), save_to_db=True, conn=conn)
        
        if df is None:
            logger.error("No data returned from get_league_game_log_from_date")
            conn.close()
            return
            
        if len(df) == 0:
            logger.info("No new games found")
            conn.close()
            return 0
            
        games = df["game_id"].unique().tolist()
        logger.info(f"Found {len(games)} new games")
        
        # get box score summaries and play by play for new games
        logger.info("Getting box scores...")
        get_box_score_summaries(games, proxies=get_proxies(), save_to_db=True, conn=conn)
        logger.info("Getting play by play...")
        get_play_by_play(games, proxies=get_proxies(), save_to_db=True, conn=conn)
        
        # dump db tables to csv
        logger.info("Dumping DB to CSV...")
        dump_db(conn)
        
        # upload new db version to Kaggle
        version_message = f"Daily update: {pd.to_datetime('today').strftime('%Y-%m-%d')}"
        logger.info("Uploading new DB version...")
        upload_new_db_version(version_message)
        
        # close db connection
        conn.close()
        logger.info("Update completed successfully")
        
    except Exception as e:
        logger.error(f"Error during update: {str(e)}")
        raise

# Add these diagnostic functions
def check_db_connection():
    conn = get_db_conn()
    result = pd.read_sql("SELECT COUNT(*) FROM game", conn)
    print(f"Number of games in DB: {result.iloc[0,0]}")
    return conn

def check_latest_game():
    conn = get_db_conn()
    result = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn)
    print(f"Latest game date: {result.iloc[0,0]}")
    return result.iloc[0,0]
