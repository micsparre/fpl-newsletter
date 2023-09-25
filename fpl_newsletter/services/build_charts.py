from services.charts import *
from etl_scripts.api import get_gameweek
from services.sql import connect, close_connection, execute_query

NEWSLETTER_DB = "newsletter"
gameweek, events_df = get_gameweek()


def build_charts(league_number, subscriber_id):
    """
    Builds the charts and returns the paths
    """
    if check_status(subscriber_id):
        print("")
        standings_path = chart_league_standings_history(league_number)
        top_players_path = chart_top_n_players(league_number)
        streaks_path = chart_current_streaks(league_number)
        transfers_path = chart_net_xfer_value(gameweek, league_number)
        margins_path = chart_margins_multi(league_number)
        update_status(gameweek, subscriber_id)
        return [standings_path, top_players_path, streaks_path, transfers_path, margins_path]
    return []


def check_status(subscriber_id):
    """
    Checks the status of the gameweek to see if the charts need to be sent
    """
    SQL = f"SELECT charts_sent_status from {NEWSLETTER_DB} where gameweek={gameweek} and subscriber_id={subscriber_id}"
    conn, cursor = connect()
    rows = execute_query(SQL)
    stale_status = int(rows[0][0])
    curr_week = events_df[events_df["gameweek"] == gameweek]
    status = curr_week["charts_sent_status"].values[0]
    if stale_status == 0 and status == 1:  # charts need to be sent
        return True
    close_connection(cursor, conn)
    return False


def update_status(gameweek, subscriber_id):
    """
    Updates the status of the gameweek to indicate that the charts have been sent
    """
    SQL = f"UPDATE {NEWSLETTER_DB} SET charts_sent_status = 1 where gameweek={gameweek} and subscriber_id={subscriber_id}"
    conn, cursor = connect()
    execute_query(SQL)
    close_connection(cursor, conn)
    return True
