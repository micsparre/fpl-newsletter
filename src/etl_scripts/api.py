import os
import requests
import json
from pandas import json_normalize
from services.utils import load_json
from services.sql import connect, close_connection

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)

# BASE API URL
PREM_URL = "https://draft.premierleague.com/"

# Specify the path to the parent directory
# PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

API_RESULTS_FOLDER = os.path.join("data", "api_results")

def get_player_summary(player_id):
    PLAYER_API = 'api/element-summary'
    # Post credentials for authentication
    session = requests.session()
    url = 'https://users.premierleague.com/accounts/login/'
    payload = {
     'password': CONFIG.get('api').get('password'),
     'login': CONFIG.get('api').get('username'),
     'redirect_uri': 'https://fantasy.premierleague.com/a/login',
     'app': 'plfpl-web'
    }
    session.post(url, data=payload)
    r = session.get(os.path.join(PREM_URL, PLAYER_API, str(player_id)))
    json_response = r.json()
    file_path = os.path.join(API_RESULTS_FOLDER, "element-summary", str(player_id))
    with open(f"{file_path}.json", 'w') as outfile:
        json.dump(json_response, outfile)
    return json_response

def get_transactions():
    data = {
            "login": CONFIG.get('api').get('password'),
            "password": CONFIG.get('api').get('username'),
            "app": "plfpl-web",
            "redirect_uri": "https://fantasy.premierleague.com/a/login",
    }
    session = requests.session()
    url = 'api/draft/league/73698/transactions'
    r = session.get(
        PREM_URL + url, data=data, headers={}
    )
    json_response = r.json()
    if r.status_code == 200:
        file_path = os.path.join(API_RESULTS_FOLDER, os.path.basename(url))
        with open(f"{file_path}.json", 'w') as outfile:
            json.dump(json_response, outfile)
    return

def get_players():
    """
    Pulls fpl draft league data using an api call, and stores the output
    in a json file at the specified location.
    
    To get the FPL Draft api call, I followed advice on [Reddit here](https://www.reddit.com/r/FantasyPL/comments/9rclpj/python_for_fantasy_football_using_the_fpl_api/e8g6ur4?utm_source=share&utm_medium=web2x)
    which basically said you can derive the API calls by using Chrome's developer window under network you can see the "fetches" that are made, and the example response data. Very cool!
    
    :param file_path: The file path and name of the json file you wish to create
    :param api: The api call for your fpl draft league
    :returns: 
    """

    apis = [
       'api/bootstrap-static',
       'api/bootstrap-dynamic',
       'api/league/73698/details',
       'api/league/73698/element-status'
       ]
    
    # Post credentials for authentication
    session = requests.session()
    url = 'https://users.premierleague.com/accounts/login/'
    payload = {
     'password': CONFIG.get('api').get('password'),
     'login': CONFIG.get('api').get('username'),
     'redirect_uri': 'https://fantasy.premierleague.com/a/login',
     'app': 'plfpl-web'
    }
    session.post(url, data=payload)
    
    # Loop over the api(s), call them and capture the response(s)
    for url in apis:
        r = session.get(os.path.join(PREM_URL, url))
        json_response = r.json()
        file_path = os.path.join(API_RESULTS_FOLDER, os.path.basename(url))
        with open(f"{file_path}.json", 'w') as outfile:
            json.dump(json_response, outfile)
    return
            
# returns dataframe of api object denoted by df_name
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
    
    elif df_name == 'events':
        with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
            d = json.load(json_data)
            events_df = json_normalize(d['events'])    
        return events_df
    
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

def get_gameweek():
    with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
            events = json.load(json_data)
            events = events["events"]

    current = events["current"]
    events_df = json_normalize(events["data"])
    events_df = events_df[["id", "finished"]]
    events_df["finished"] = events_df['finished'].replace({True: 1, False: 0})
    events_df.rename(columns={"id":"gameweek", "finished":"charts_sent_status"}, inplace=True)
    return current, events_df