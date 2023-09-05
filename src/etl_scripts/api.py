import os
import requests
import json
from pandas import json_normalize
from services.utils import load_json

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)

# BASE API URL
PREM_URL = "https://draft.premierleague.com/"

# folder to store api data
API_RESULTS_FOLDER = os.path.join("data", "api_results")

# retrieves data from 'element-summary' api endpoint
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

# retrieves transaction data based on league number
def get_transactions():
    league_number = 73698
    data = {
            "login": CONFIG.get('api').get('password'),
            "password": CONFIG.get('api').get('username'),
            "app": "plfpl-web",
            "redirect_uri": "https://fantasy.premierleague.com/a/login",
    }
    session = requests.session()
    url = f"api/draft/league/{league_number}/transactions"
    r = session.get(
        PREM_URL + url, data=data, headers={}
    )
    json_response = r.json()
    if r.status_code == 200:
        file_path = os.path.join(API_RESULTS_FOLDER, os.path.basename(url))
        with open(f"{file_path}.json", 'w') as outfile:
            json.dump(json_response, outfile)
    return

# reterieves general fpl data from multiple api endpoints
def get_fpl():

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
    if df_name == 'league_entries' or df_name == 'matches' or df_name == 'standings':
        with open(os.path.join(API_RESULTS_FOLDER, 'details.json')) as json_data:
            d = json.load(json_data)
            league_entry_df = json_normalize(d[df_name])    
        return league_entry_df
    
    # Dataframes from the bootstrap-static.json
    elif df_name == 'elements' or df_name == 'element_types' or df_name == 'events':
        with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
            d = json.load(json_data)
            elements_df = json_normalize(d['elements'])    
        return elements_df
    
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

# returns current gameweek and processed events dataframe
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

# composite function to retrieve all data from api
def get_data():
    get_transactions()
    get_fpl()
    return