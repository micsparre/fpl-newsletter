import os
import requests
import json
from pandas import json_normalize
from services.utils import load_json
import logging

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)

PREM_URL = "https://draft.premierleague.com/"

API_RESULTS_FOLDER = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "data", "api_results")  # folder to store api data


def get_player_summary(player_id):
    """
    Retrieves data from 'element-summary' api endpoint
    """

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
    file_path = os.path.join(
        API_RESULTS_FOLDER, "element-summary", str(player_id))
    with open(f"{file_path}.json", 'w') as outfile:
        json.dump(json_response, outfile)
    return json_response


def get_league_data(league_number):
    """
    Retrieves data from api endpoints specific to a league number
    """
    apis = [
        f'api/league/{league_number}/details',
        f'api/league/{league_number}/element-status',
        f'api/draft/league/{league_number}/transactions'
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
        if r.status_code != 200:
            logger.error(f"Error retrieving data from {url}")
            raise Exception(f"Error retrieving data from {url}")
        json_response = r.json()
        file_path = os.path.join(API_RESULTS_FOLDER, str(
            league_number), os.path.basename(url))
        with open(f"{file_path}.json", 'w') as outfile:
            json.dump(json_response, outfile)
    return


def get_fpl_data():
    """
    Retrieves data from multiple api endpoints
    """

    apis = [
        'api/bootstrap-static',
        'api/bootstrap-dynamic'
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


def get_dataframe(df_name, league_number):
    """
    Returns a dataframe from the api results folder
    """

    # Dataframes from the details.json
    if df_name == 'league_entries' or df_name == 'matches' or df_name == 'standings':
        with open(os.path.join(API_RESULTS_FOLDER, str(league_number), 'details.json')) as json_data:
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
        with open(os.path.join(API_RESULTS_FOLDER, str(league_number), 'transactions.json')) as json_data:
            d = json.load(json_data)
            transactions_df = json_normalize(d['transactions'])
        return transactions_df

    # Dataframes from the element-status.json
    elif df_name == 'element_status':
        with open(os.path.join(API_RESULTS_FOLDER, str(league_number), 'element-status.json')) as json_data:
            d = json.load(json_data)
            element_status_df = json_normalize(d['element_status'])
        return element_status_df


def get_gameweek():
    """
    Returns the current gameweek and a dataframe of processed events
    """
    with open(os.path.join(API_RESULTS_FOLDER, 'bootstrap-static.json')) as json_data:
        events = json.load(json_data)
        events = events["events"]

    current = events["current"]
    events_df = json_normalize(events["data"])
    events_df = events_df[["id", "finished"]]
    events_df["finished"] = events_df['finished'].replace({True: 1, False: 0})
    events_df.rename(
        columns={"id": "gameweek", "finished": "charts_sent_status"}, inplace=True)
    return current, events_df
