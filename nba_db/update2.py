import nba_db.utils
import pandas as pd
import logging
from datetime import datetime
import os
import sqlite3
from nba_api.stats.endpoints import leaguegamelog

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("nba_db_logger")

def get_proxies():
    return pd.DataFrame(columns=['ip:port'])

nba_db.utils.get_proxies = get_proxies

from nba_db.extract import (
    get_box_score_summaries,
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

def get_league_game_log_from_date(start_date, proxies=None, save_to_db=True, conn=None):
    """Modified function to use NBA API directly and transform data to match DB schema"""
    print(f"Fetching games starting from {start_date}")
    try:
        # Get current season's games
        gamelog = leaguegamelog.LeagueGameLog(season='2023-24')
        df = gamelog.get_data_frames()[0]
        print(f"Found {len(df)} games from current season")
        
        # Filter for games after start_date
        df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
        df = df[df['GAME_DATE'] >= pd.to_datetime(start_date)]
        print(f"Found {len(df)} games after {start_date}")
        
        # Transform data to match database schema
        games_list = []
        for game_id in df['GAME_ID'].unique():
            game_data = df[df['GAME_ID'] == game_id]
            
            # There should be exactly 2 rows per game (home and away)
            if len(game_data) != 2:
                continue
                
            # Determine home and away teams based on MATCHUP
            home_team = game_data[game_data['MATCHUP'].str.contains(' vs. ')].iloc[0]
            away_team = game_data[game_data['MATCHUP'].str.contains(' @ ')].iloc[0]
            
            # Create a row matching our database schema
            game_row = {
                'season_id': home_team['SEASON_ID'],
                'team_id_home': str(home_team['TEAM_ID']),
                'team_abbreviation_home': home_team['TEAM_ABBREVIATION'],
                'team_name_home': home_team['TEAM_NAME'],
                'game_id': game_id,
                'game_date': home_team['GAME_DATE'],
                'matchup_home': home_team['MATCHUP'],
                'wl_home': home_team['WL'],
                'min': home_team['MIN'],
                'fgm_home': home_team['FGM'],
                'fga_home': home_team['FGA'],
                'fg_pct_home': home_team['FG_PCT'],
                'fg3m_home': home_team['FG3M'],
                'fg3a_home': home_team['FG3A'],
                'fg3_pct_home': home_team['FG3_PCT'],
                'ftm_home': home_team['FTM'],
                'fta_home': home_team['FTA'],
                'ft_pct_home': home_team['FT_PCT'],
                'oreb_home': home_team['OREB'],
                'dreb_home': home_team['DREB'],
                'reb_home': home_team['REB'],
                'ast_home': home_team['AST'],
                'stl_home': home_team['STL'],
                'blk_home': home_team['BLK'],
                'tov_home': home_team['TOV'],
                'pf_home': home_team['PF'],
                'pts_home': home_team['PTS'],
                'plus_minus_home': home_team['PLUS_MINUS'],
                'video_available_home': home_team['VIDEO_AVAILABLE'],
                
                'team_id_away': str(away_team['TEAM_ID']),
                'team_abbreviation_away': away_team['TEAM_ABBREVIATION'],
                'team_name_away': away_team['TEAM_NAME'],
                'matchup_away': away_team['MATCHUP'],
                'wl_away': away_team['WL'],
                'fgm_away': away_team['FGM'],
                'fga_away': away_team['FGA'],
                'fg_pct_away': away_team['FG_PCT'],
                'fg3m_away': away_team['FG3M'],
                'fg3a_away': away_team['FG3A'],
                'fg3_pct_away': away_team['FG3_PCT'],
                'ftm_away': away_team['FTM'],
                'fta_away': away_team['FTA'],
                'ft_pct_away': away_team['FT_PCT'],
                'oreb_away': away_team['OREB'],
                'dreb_away': away_team['DREB'],
                'reb_away': away_team['REB'],
                'ast_away': away_team['AST'],
                'stl_away': away_team['STL'],
                'blk_away': away_team['BLK'],
                'tov_away': away_team['TOV'],
                'pf_away': away_team['PF'],
                'pts_away': away_team['PTS'],
                'plus_minus_away': away_team['PLUS_MINUS'],
                'video_available_away': away_team['VIDEO_AVAILABLE'],
                
                'season_type': 'Regular Season'  # Add logic for playoffs if needed
            }
            games_list.append(game_row)
        
        # Convert to DataFrame
        transformed_df = pd.DataFrame(games_list)
        print(f"Transformed {len(transformed_df)} games to match database schema")
        
        if save_to_db and conn is not None and not transformed_df.empty:
            print("Saving to database...")
            transformed_df.to_sql('game', conn, if_exists='append', index=False)
            
        return transformed_df
    except Exception as e:
        print(f"Error fetching game log: {str(e)}")
        return None

def daily():
    try:
        print("Starting daily update process...")
        
        # First ensure we have the database
        check_and_download_db()
        
        # get db connection
        print("Getting DB connection...")
        conn = get_db_conn()
        
        # get latest date in db and add a day
        print("Getting latest date from DB...")
        latest_db_date = pd.read_sql("SELECT MAX(GAME_DATE) FROM game", conn).iloc[0, 0]
        print(f"Latest date in DB: {latest_db_date}")
        
        # check if today is a game day
        if pd.to_datetime(latest_db_date) >= pd.to_datetime(datetime.today().date()):
            print("No new games today. Exiting...")
            return
            
        # add a day to latest db date
        latest_db_date = (pd.to_datetime(latest_db_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Fetching games from: {latest_db_date}")
        
        # get new games and add to db using our modified function
        print("Getting league game log...")
        df = get_league_game_log_from_date(latest_db_date, proxies=get_proxies(), save_to_db=True, conn=conn)
        
        if df is None:
            print("No data returned from get_league_game_log_from_date")
            conn.close()
            return
            
        if len(df) == 0:
            print("No new games found")
            conn.close()
            return 0
            
        games = df["GAME_ID"].unique().tolist()  # Note: column name might be different in API response
        print(f"Found {len(games)} new games")
        
        # get box score summaries and play by play for new games
        print("Getting box scores...")
        get_box_score_summaries(games, proxies=get_proxies(), save_to_db=True, conn=conn)
        print("Getting play by play...")
        get_play_by_play(games, proxies=get_proxies(), save_to_db=True, conn=conn)
        
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
