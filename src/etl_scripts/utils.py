import requests
import json
import pandas as pd
from pandas import json_normalize
import os
from lib.utils import load_json

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)

PREM_URL = "https://draft.premierleague.com/"
API_RESULTS_FOLDER = os.path.join("data", "api_results")

def get_json():
    apis = ['api/draft/entry/301781/transactions',
            'api/bootstrap-static',
            'api/bootstrap-dynamic',
            'api/league/73698/details',
            'api/league/73698/element-status'
        ]
    
    # Post credentials for authentication
    session = requests.session()
    url = 'https://users.premierleague.com/accounts/login/'
    payload = {
     'password': CONFIG.get('api', 'password'),
     'login': CONFIG.get('api', 'username'),
     'redirect_uri': 'https://fantasy.premierleague.com/a/login',
     'app': 'plfpl-web'
    }
    session.post(url, data=payload)
    
    # Loop over the api(s), call them and capture the response(s)
    for url in apis:
        r = session.get(PREM_URL + url)
        jsonResponse = r.json()
        file_path = os.path.join(API_RESULTS_FOLDER, os.path.basename(url))
        with open(f"{file_path}.json", 'w') as outfile:
            json.dump(jsonResponse, outfile)
            

def get_dataframe(df_name):
    
    # Dataframes from the details.json
    if df_name == 'league_entries':
        with open(os.path.join(API_RESULTS_FOLDER, 'details.json')) as json_data:
            d = json.load(json_data)
            league_entry_df = json_normalize(d['league_entries'])
            
        return league_entry_df
    
    elif df_name == 'matches':
        with open(os.path.join(API_RESULTS_FOLDER, 'details.json')) as json_data:
            d = json.load(json_data)
            matches_df = json_normalize(d['matches'])
            
        return matches_df
    
    elif df_name == 'standings':
        with open(os.path.join(API_RESULTS_FOLDER, 'details.json')) as json_data:
            d = json.load(json_data)
            standings_df = json_normalize(d['standings'])
            
        return standings_df
    
    # Dataframes from the bootstrap-static.json
    elif df_name == 'elements':
        with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
            d = json.load(json_data)
            elements_df = json_normalize(d['elements'])
            
        return elements_df
    
    elif df_name == 'element_types':
        with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
            d = json.load(json_data)
            element_types_df = json_normalize(d['element_types'])
            
        return element_types_df
    
    # Dataframes from the transactions.json
    elif df_name == 'transactions':
        with open(os.path.join(API_RESULTS_FOLDER, 'transactions.json')) as json_data:
            d = json.load(json_data)
            transactions_df = json_normalize(d['transactions'])
            
        return transactions_df
    
    # Dataframes from the element-status.json
    elif df_name == 'element_status':
        with open(os.path.join(API_RESULTS_FOLDER, 'element-status.json')) as json_data:
            d = json.load(json_data)
            element_status_df = json_normalize(d['element_status'])
            
        return element_status_df
    
# pulls player gameweek data for a specified list of players
def get_player_data(elements):
    for element in elements:
        
        # Write the api call
        apis = [f"/api/element-summary/{str(element)}"]

        # Post credentials for authentication
        session = requests.session()
        url = 'https://users.premierleague.com/accounts/login/'
        payload = {
         'password': CONFIG.get('api', 'password'),
         'login': CONFIG.get('api', 'username'),
         'redirect_uri': 'https://fantasy.premierleague.com/a/login',
         'app': 'plfpl-web'
        }
        session.post(url, data=payload)

        # Loop over the api(s), call them and capture the response(s)
        for url in apis:
            r = session.get(PREM_URL + url)
            jsonResponse = r.json()
            file_path = os.path.join(API_RESULTS_FOLDER, os.path.basename(url))
            with open(f"{file_path}.json", 'w') as outfile:
                json.dump(jsonResponse, outfile)
    
    
def get_team_players_agg_data():
    
    # Pull the required dataframes
    element_status_df = get_dataframe('element_status')
    elements_df = get_dataframe('elements')
    element_types_df = get_dataframe('element_types')
    league_entry_df = get_dataframe('league_entries')
    matches_df = get_dataframe('matches')
    standings_df = get_dataframe('standings')
    
    # Built the initial player -> team dataframe
    players_df = (pd.merge(element_status_df,
                           league_entry_df,
                           left_on='owner',
                           right_on='entry_id'
                        )
              .drop(columns=['in_accepted_trade',
                            'owner',
                            'status',
                            'entry_id',
                            'entry_name',
                            'id',
                            'joined_time',
                            'player_last_name',
                            'short_name',
                            'waiver_pick'])
              .rename(columns={'player_first_name':'team'})
             )
    
    # Get the element details
    players_df = pd.merge(players_df, elements_df, left_on='element', right_on='id')
    players_df = players_df[['team_x',
                             'element',
                             'web_name',
                             'total_points',
                             'goals_scored',
                             'goals_conceded',
                             'clean_sheets',
                             'assists',
                             'bonus',
                             'draft_rank',
                             'element_type',
                             'points_per_game',
                             'red_cards',
                             'yellow_cards'
                            ]]
    
    # Get the player types (GK, FWD etc.)
    players_df = (pd.merge(players_df,
                         element_types_df,
                         how='left',
                         left_on='element_type',
                         right_on='id')
                 .drop(columns=['id',
                                'plural_name_short',
                                'singular_name',
                                'singular_name_short'])
                )

    return players_df


def get_team_players_gw_data():
    
    df = get_team_players_agg_data()
    elements_to_pull = df['element']
    players_dict = {}
    
    for element in elements_to_pull:
        with open(f'../data/elements/{element}.json') as json_data:
            d = json.load(json_data)
            players_dict[element] = json_normalize(d['history'])
            players_df = pd.concat(players_dict, ignore_index=True)
            
    return players_df


def get_num_gameweeks():
    
    matches_df = get_dataframe('matches')       
    num_gameweeks = matches_df[matches_df['finished'] == True]['event'].max()
    
    return num_gameweeks

# Pulls gameweek data for a given list of players
def get_player_gameweek_data(elements, gameweek):
    players_dict = {}
    
    # For each element we want to pull
    for element in elements:
        
        # Load the json data and put into players_df
        with open(f'data/elements/{element}.json') as json_data:
            d = json.load(json_data)
            players_dict[element] = json_normalize(d['history'])
            players_df = pd.concat(players_dict, ignore_index=True)
            players_df = players_df[players_df['event'] == 28]
            
    return players_df
