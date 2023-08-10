import pandas as pd
import os
from utils import load_json
from sql import connect, close_connection

FOLDER = "data"

# process the api data into the players table
def process_players():
    DB = "players"
    elements_df = process_elements()
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
    
    # print(f"merged_df columns: {merged_df.columns}")
    # print(f"num rows: {len(merged_df)}")
    excel_filename = 'players.xlsx'
    merged_df.to_excel(excel_filename, index=False)
    
    conn, cursor = connect()
    merged_df.to_sql(DB, conn, if_exists='replace', index=False)
    close_connection(cursor, conn)
    return

# 'elements' object processed and returned as df
def process_elements():
    elements_filename = "bootstrap-static.json"
    elements_json = load_json(os.path.join(FOLDER, elements_filename))
    elements_df = pd.json_normalize(elements_json["elements"])
    elements_columns = ["id", "first_name", "second_name", "team", "element_type", "draft_rank", "status"]
    elements_df = elements_df[elements_columns]
    elements_df.rename(columns={"second_name" : "last_name"}, inplace=True)
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

