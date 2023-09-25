import pandas as pd
import os
from services.utils import load_json
from etl_scripts.api import get_dataframe
from services.sql import connect, close_connection, get_df_from_table

API_RESULTS_FOLDER = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "data", "api_results")

PLAYERS_DB = "players"


def process_players(league_number):
    """
    Process the api data and load into the players table
    """
    elements_df = process_elements("api", league_number)
    status_df = process_status(league_number)
    owners_df = process_owners(league_number)
    teams_df = process_teams()
    positions_df = process_positions()

    merged_df = elements_df.merge(
        status_df, left_on="id", right_on="element", how="left")
    merged_df.drop(columns=["element"], inplace=True)
    merged_df.rename(columns={"id": "player_id"}, inplace=True)

    merged_df = merged_df.merge(
        owners_df, left_on="owner", right_on="entry_id", how="left")
    merged_df.drop(columns=["owner", "entry_id"], inplace=True)
    merged_df.rename(columns={"entry_name": "owner"}, inplace=True)

    merged_df = merged_df.merge(
        teams_df, left_on="team", right_on="team_id", how="left")
    merged_df.drop(columns=["team_id", "team"], inplace=True)
    merged_df.rename(columns={"name": "team"}, inplace=True)

    merged_df = merged_df.merge(
        positions_df, left_on="element_type", right_on="position_id", how="left")
    merged_df.drop(columns=["position_id", "element_type"], inplace=True)
    merged_df.rename(columns={"singular_name": "position"}, inplace=True)
    merged_df.sort_values(
        by=["owner", "status", "draft_rank"], inplace=True, na_position='first')

    db_players_df = process_elements("table", league_number)
    new_players_df = identify_new_players(
        merged_df, db_players_df.loc[:, ['player_id']])
    status_updates_df = identify_status_updates(
        merged_df.copy(), db_players_df.loc[:, ['player_id', 'status']])

    REPORT_PATH = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "data", "generated_reports", str(league_number), "players.xlsx")
    with pd.ExcelWriter(REPORT_PATH) as writer:
        merged_df.to_excel(writer, sheet_name='player info', index=False)
        new_players_df.to_excel(writer, sheet_name='new players', index=False)
        status_updates_df.to_excel(
            writer, sheet_name='status updates', index=False)

    num_new_players, num_status_updates = len(
        new_players_df), len(status_updates_df)

    message_body = f"""
{num_new_players or 0} new player{"s" if num_new_players != 1 else ""} {"have" if num_new_players != 1 else "has"} joined since the last newsletter.

{num_status_updates or 0} player status update{"s" if num_status_updates != 1 else ""} since the last newsletter.
""" if (num_new_players + num_status_updates) > 0 else ""

    conn, cursor = connect()
    merged_df.to_sql(PLAYERS_DB, conn, if_exists='replace', index=False)
    close_connection(cursor, conn)
    return [REPORT_PATH] if message_body else [], message_body


def process_elements(method, league_number):
    """
    Process the elements object and return a dataframe
    """
    if method == "api":
        elements_df = get_dataframe("elements", league_number)
        elements_columns = ["id", "first_name", "second_name",
                            "team", "element_type", "draft_rank", "status"]
        value_map = {'a': 'Active', 'u': 'Transferred',
                     'i': 'Bad Injury', 's': 'Suspended', 'd': 'Injury', 'n': 'Not Training'}
        elements_df = elements_df[elements_columns]
        elements_df.rename(columns={"second_name": "last_name"}, inplace=True)
        elements_df['status'].replace(value_map, inplace=True)
    elif method == "table":
        elements_df = get_df_from_table("players")
    else:
        raise Exception("Invalid method passed")
    return elements_df


def process_status(league_number):
    """
    Process the element_status object and return a dataframe
    """
    filename = "element-status.json"
    element_status_json = load_json(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), API_RESULTS_FOLDER, str(league_number), filename))
    status_df = pd.json_normalize(element_status_json["element_status"])
    status_columns = ["element", "owner"]
    status_df = status_df[status_columns]
    return status_df


def process_owners(league_number):
    """
    Process the league_entries object and return a dataframe
    """
    filename = "details.json"
    owners_json = load_json(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), API_RESULTS_FOLDER, str(league_number), filename))
    owners_df = pd.json_normalize(owners_json["league_entries"])
    owners_columns = ["entry_id", "entry_name"]
    filtered_owners_df = owners_df[owners_columns]
    return filtered_owners_df


def process_teams():
    """
    Process the teams object and return a dataframe
    """
    filename = "bootstrap-static.json"
    elements_json = load_json(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), API_RESULTS_FOLDER, filename))
    teams_df = pd.json_normalize(elements_json["teams"])
    teams_columns = ["id", "name"]
    teams_df = teams_df[teams_columns]
    teams_df.rename(columns={"id": "team_id"}, inplace=True)
    return teams_df


def process_positions():
    """
    Process the element_types object and return a dataframe
    """
    filename = "bootstrap-static.json"
    elements_json = load_json(os.path.join(API_RESULTS_FOLDER, filename))
    positions_df = pd.json_normalize(elements_json["element_types"])
    positions_columns = ["id", "singular_name"]
    positions_df = positions_df[positions_columns]
    positions_df.rename(columns={"id": "position_id"}, inplace=True)
    return positions_df


def identify_new_players(api_players_df, db_players_df):
    """
    Returns a dataframe of new players
    """
    new_players_df = api_players_df[~api_players_df['player_id'].isin(
        db_players_df['player_id'])].copy()
    new_players_df.sort_values(by=['draft_rank'], inplace=True)
    return new_players_df


def identify_status_updates(api_players_df, db_players_df):
    """
    Returns a dataframe of status updates
    """
    merged_df = pd.merge(api_players_df, db_players_df,
                         on='player_id', suffixes=["_new", "_old"], how='right')
    status_updates_df = merged_df[merged_df['status_old']
                                  != merged_df['status_new']].copy()
    status_updates_df.sort_values(by=['status_old'], inplace=True)
    status_updates_df = status_updates_df[['player_id', 'first_name', 'last_name', 'draft_rank', 'owner',
                                           'team', 'position', 'status_old', 'status_new']]
    return status_updates_df
