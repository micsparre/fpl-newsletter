import pandas as pd
import os
from utils import load_json
from sql import connect, close_connection

# process the api data into the players table
def process_players():
    folder = "data"
    filename = "elements.json"
    player_json = load_json(os.path.join(folder, filename))
    
    players_df = pd.json_normalize(player_json["elements"])
    teams_df = pd.json_normalize(player_json["teams"])
    positions_df = pd.json_normalize(player_json["element_types"])
    
    players_columns = ["id", "first_name", "second_name", "team", "element_type", "draft_rank"]
    teams_columns = ["id", "name"]
    positions_columns = ["id", "singular_name"]
    
    players_df = players_df[players_columns]
    teams_df = teams_df[teams_columns]
    positions_df = positions_df[positions_columns]
    
    players_df.rename(columns={"second_name" : "last_name"}, inplace=True)
    teams_df.rename(columns={"id" : "team_id"}, inplace=True)
    positions_df.rename(columns={"id" : "position_id"}, inplace=True)
    
    # print(f"players_df columns: {players_df.columns}")
    # print(f"teams_df columns: {teams_df.columns}")
    
    merged_df = players_df.merge(teams_df, left_on="team", right_on="team_id", how="left")
    merged_df.drop(columns=["team_id", "team"], inplace=True)
    merged_df.rename(columns={"name" : "team"}, inplace=True)
    
    merged_df = merged_df.merge(positions_df, left_on="element_type", right_on="position_id", how="left")
    merged_df.drop(columns=["position_id", "element_type"], inplace=True)
    merged_df.rename(columns={"singular_name" : "position"}, inplace=True)
    
    # print(f"merged_df columns: {merged_df.columns}")
    
    conn, cursor = connect()
    merged_df.to_sql('players', conn, if_exists='replace', index=False)
    
    close_connection(cursor, conn)
    return
    