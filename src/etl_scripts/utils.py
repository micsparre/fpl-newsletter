import pandas as pd
import os
from pandas import json_normalize
from etl_scripts.api import get_dataframe, get_player_summary, get_gameweek
from services.utils import load_json

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)


def get_team_players_agg_data():
    """
    Aggregate data for owned players
    """

    # Pull the required dataframes
    element_status_df = get_dataframe('element_status')
    elements_df = get_dataframe('elements')
    element_types_df = get_dataframe('element_types')
    league_entry_df = get_dataframe('league_entries')

    # Build the initial player -> team dataframe
    players_df = (pd.merge(element_status_df,
                           league_entry_df,
                           left_on='owner',
                           right_on='entry_id'
                           )
                  .drop(columns=['in_accepted_trade', 'owner', 'status', 'entry_id', 'entry_name', 'id', 'joined_time', 'player_last_name', 'short_name', 'waiver_pick'])
                  .rename(columns={'player_first_name': 'team'})
                  )

    # Get the element details
    players_df = pd.merge(players_df, elements_df,
                          left_on='element', right_on='id')
    players_df = players_df[['team_x', 'element', 'web_name', 'total_points', 'goals_scored', 'goals_conceded',
                             'clean_sheets', 'assists', 'bonus', 'draft_rank', 'element_type', 'points_per_game',
                             'red_cards', 'yellow_cards'
                             ]]

    # Get the player types (GK, FWD etc.)
    players_df = (pd.merge(players_df,
                           element_types_df,
                           how='left',
                           left_on='element_type',
                           right_on='id')
                  .drop(columns=['id'])
                  )

    return players_df


def get_team_players_gw_data():
    """
    Aggregate data based on gameweek
    """

    df = get_team_players_agg_data()
    elements_to_pull = df['element']
    players_dict = {}

    for element in elements_to_pull:
        d = get_player_summary(element)
        players_dict[element] = json_normalize(d['history'])
        players_df = pd.concat(players_dict, ignore_index=True)

    return players_df


def get_player_gameweek_data(elements, gameweek):
    """
    Pulls gameweek data for a given list of players
    """
    columns = ['id', 'detail', 'event', 'assists', 'bonus', 'bps', 'clean_sheets',
               'creativity', 'goals_conceded', 'goals_scored', 'ict_index',
               'influence', 'minutes', 'own_goals', 'penalties_missed',
               'penalties_saved', 'red_cards', 'saves', 'threat', 'yellow_cards',
               'starts', 'expected_goals', 'expected_assists',
               'expected_goal_involvements', 'expected_goals_conceded', 'total_points',
               'element', 'fixture', 'opponent_team']
    players_df = pd.DataFrame(columns=columns)

    for element in elements:
        d = get_player_summary(element)
        players = json_normalize(d['history'])
        players_df = pd.concat([players_df, players], ignore_index=True)

    players_df = players_df[players_df["event"] == gameweek]
    return players_df
