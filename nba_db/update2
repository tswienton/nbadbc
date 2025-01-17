from nba_db.update import daily
import nba_db.utils
import pandas as pd
import logging
from datetime import datetime

# Mock the proxies function to return empty DataFrame
def get_proxies():
    return pd.DataFrame(columns=['ip:port'])

# Inject our proxy-free function into the utils module
nba_db.utils.get_proxies = get_proxies

# Import required functions, keeping original function references
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

# Keep original logger
logger = logging.getLogger("nba_db_logger")

# Define daily function with same name but without proxy dependency
def daily():
    # download db from Kaggle
    download_db()
    
    # get db connection
    conn = get_db_conn()
    
    # get latest date in db and add a day
    latest_db_date = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn).iloc[0, 0]
    
    # check if today is a game day
    if pd.to_datetime(latest_db_date) >= pd.to_datetime(datetime.today().date()):
        logger.info("No new games today. Exiting...")
        return
        
    # add a day to latest db date
    latest_db_date = (pd.to_datetime(latest_db_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    
    # get new games and add to db (using empty proxies)
    df = get_league_game_log_from_date(latest_db_date, proxies=get_proxies(), save_to_db=True, conn=conn)
    
    if len(df) == 0:
        conn.close()
        return 0
        
    games = df["game_id"].unique().tolist()
    
    # get box score summaries and play by play for new games
    get_box_score_summaries(games, proxies=get_proxies(), save_to_db=True, conn=conn)
    get_play_by_play(games, proxies=get_proxies(), save_to_db=True, conn=conn)
    
    # dump db tables to csv
    dump_db(conn)
    
    # upload new db version to Kaggle
    version_message = f"Daily update: {pd.to_datetime('today').strftime('%Y-%m-%d')}"
    upload_new_db_version(version_message)
    
    # close db connection
    conn.close()

# Override the original daily function in the module
import nba_db.update
nba_db.update.daily = daily
