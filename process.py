import pandas as pd
import os
from utils import load_json
from api import get_data
from sql import connect, close_connection, get_df_from_table
from sms import send_sms
from send_email import send_email

FOLDER = "data"
REPORT_PATH = 'players.xlsx'

# process the api data into the players table
def process_players():
    DB = "players"
    elements_df = process_elements("api")
    status_df = process_status()
    owners_df = process_owners()
    teams_df = process_teams()
    positions_df = process_positions()
    
    merged_df = elements_df.merge(status_df, left_on="id", right_on="element", how="left")
    merged_df.drop(columns=["element"], inplace=True)
    
    merged_df = merged_df.merge(owners_df, left_on="owner", right_on="entry_id", how="left")
    merged_df.drop(columns=["owner", "entry_id"], inplace=True)
    merged_df.rename(columns={"entry_name" : "owner"}, inplace=True)
    
    merged_df = merged_df.merge(teams_df, left_on="team", right_on="team_id", how="left")
    merged_df.drop(columns=["team_id", "team"], inplace=True)
    merged_df.rename(columns={"name" : "team"}, inplace=True)
    
    merged_df = merged_df.merge(positions_df, left_on="element_type", right_on="position_id", how="left")
    merged_df.drop(columns=["position_id", "element_type"], inplace=True)
    merged_df.rename(columns={"singular_name" : "position"}, inplace=True)
    merged_df.sort_values(by=["owner", "status", "draft_rank"], inplace=True, na_position='first')
    
    db_players_df = process_elements("table")
    new_players_df = identify_new_players(merged_df, db_players_df.loc[:, ['id']])
    status_updates_df = identify_status_updates(merged_df.copy(), db_players_df.loc[:, ['id', 'status']])
        
    # print(f"merged_df columns: {merged_df.columns}")
    # print(f"num rows: {len(merged_df)}")
    
    with pd.ExcelWriter(REPORT_PATH) as writer:
        merged_df.to_excel(writer, sheet_name='player info', index=False)
        new_players_df.to_excel(writer, sheet_name='new players', index=False)
        status_updates_df.to_excel(writer, sheet_name='status updates', index=False)
    
    rc = send_alert(len(new_players_df), len(status_updates_df))
    
    conn, cursor = connect()
    merged_df.to_sql(DB, conn, if_exists='replace', index=False)
    close_connection(cursor, conn)
    return rc

# 'elements' object processed and returned as df
def process_elements(method):
    if method == "api":
        elements_df = get_data("elements")
        elements_columns = ["id", "first_name", "second_name", "team", "element_type", "draft_rank", "status"]
        value_map = {'a': 'Active', 'u': 'Transferred', 'i': 'Bad Injury', 's': 'Suspended', 'd': 'Injury'}
        elements_df = elements_df[elements_columns]
        elements_df.rename(columns={"second_name" : "last_name"}, inplace=True)
        elements_df['status'].replace(value_map, inplace=True)
    elif method == "table":
        elements_df = get_df_from_table("players")
    else:
        raise Exception("Invalid method passed")    
    return elements_df

# 'element_status' object processed and returned as df
def process_status():
    filename = "element-status.json"
    element_status_json = load_json(os.path.join(FOLDER, filename))
    status_df = pd.json_normalize(element_status_json["element_status"])
    status_columns = ["element", "owner"]
    status_df = status_df[status_columns]
    return status_df

# 'league_entries' object processed and returned as df
def process_owners():
    DB = "owners"
    filename = "details.json"
    owners_json = load_json(os.path.join(FOLDER, filename))
    owners_df = pd.json_normalize(owners_json["league_entries"])
    owners_columns = ["entry_id", "entry_name"]
    # owners_columns = ["entry_id", "entry_name", "player_first_name", "player_last_name"]
    owners_df = owners_df[owners_columns]
    
    # alt_owners_df = owners_df.rename(columns={"entry_id" : "id", "entry_name" : "team", "player_first_name" : "first_name", "player_last_name" : "last_name"})
    # conn, cursor = connect()
    # alt_owners_df.to_sql(DB, conn, if_exists='replace', index=False)
    # close_connection(cursor, conn)

    return owners_df

# 'teams' object processed and returned as df
def process_teams():
    filename = "bootstrap-static.json"
    elements_json = load_json(os.path.join(FOLDER, filename))
    teams_df = pd.json_normalize(elements_json["teams"])
    teams_columns = ["id", "name"]
    teams_df = teams_df[teams_columns]
    teams_df.rename(columns={"id" : "team_id"}, inplace=True)
    return teams_df  

# 'element_types' object processed and returned as df
def process_positions():
    filename = "bootstrap-static.json"
    elements_json = load_json(os.path.join(FOLDER, filename))
    positions_df = pd.json_normalize(elements_json["element_types"])
    positions_columns = ["id", "singular_name"]
    positions_df = positions_df[positions_columns]
    positions_df.rename(columns={"id" : "position_id"}, inplace=True)
    return positions_df

# compare api get to db df
def identify_new_players(api_players_df, db_players_df):
    new_players_df = api_players_df[~api_players_df['id'].isin(db_players_df['id'])].copy()
    new_players_df.sort_values(by=['draft_rank'], inplace=True)
    return new_players_df

# compare api get to db df
def identify_status_updates(api_players_df, db_players_df):
    merged_df = pd.merge(api_players_df, db_players_df, on='id', suffixes=["_old", "_new"], how='right')
    status_updates_df = merged_df[merged_df['status_old'] != merged_df['status_new']].copy()
    status_updates_df.sort_values(by=['status_old'], inplace=True)
    status_updates_df = status_updates_df[['id', 'first_name', 'last_name', 'draft_rank', 'owner',
       'team', 'position', 'status_old', 'status_new']]
    return status_updates_df

# send email alert
def send_alert(num_new_players, num_status_updates):
    rc = None
    if num_new_players or num_status_updates:
        message_body = f"""FPL DRAFT UPDATES\n
There {"are" if num_new_players != 1 else "is"} {num_new_players or 0} new player{"s" if num_new_players != 1 else ""} this week.

There {"are" if num_new_players != 1 else "is"} {num_status_updates or 0} player status update{"s" if num_new_players != 1 else ""} this week.
"""
        rc = send_sms(message_body)
        send_email(REPORT_PATH, message_body)
    else:
        rc = "No updates to send out"
    return rc