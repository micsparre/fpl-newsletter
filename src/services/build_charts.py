from services.charts import *
from etl_scripts.api import get_gameweek
from services.sql import connect, close_connection, execute_query

NEWSLETTER_DB = "newsletter"
gameweek, events_df = get_gameweek()

# builds the charts and returns the paths
def build_charts():
    if check_status() or True:
    
        standings_path = chart_league_standings_history()
        
        top_players_path = chart_top_n_players()
        
        streaks_path = chart_current_streaks()
        
        transfers_path = chart_net_xfer_value(gameweek)
        
        margins_path = chart_margins_multi()
        
        return [standings_path, top_players_path, streaks_path, transfers_path, margins_path]
    return []

# determines whether the gameweek has finished and the charts should be sent
def check_status():
    SQL = f"SELECT charts_sent_status from {NEWSLETTER_DB} where gameweek={gameweek}"
    conn, cursor = connect()
    rows = execute_query(SQL)
    stale_status = int(rows[0][0])
    curr_week = events_df[events_df["gameweek"] == gameweek]
    status = curr_week["charts_sent_status"].values[0]
    if stale_status == 0 and status == 1: # charts need to be sent
        UPDATE_SQL = f"UPDATE {NEWSLETTER_DB} SET charts_sent_status = 1 where gameweek={gameweek}"
        execute_query(UPDATE_SQL)
        return True
    close_connection(cursor, conn)
    return False