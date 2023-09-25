import pandas as pd
from etl_scripts.utils import get_dataframe


def get_transactions_df(gameweek, accepted=True):
    """
    Returns a dataframe of transactions for a given gameweek
    """

    entries_df = get_dataframe('league_entries')
    elements_df = get_dataframe('elements')
    transactions_df = get_dataframe('transactions')

    entries_df = entries_df[['entry_id', 'player_first_name']]
    elements_df = elements_df[['web_name', 'id']]
    transactions_df = transactions_df[[
        'element_in', 'element_out', 'event', 'entry', 'result', 'kind']]
    transactions_df = transactions_df[transactions_df['event'] == gameweek]

    # Left join to get league player name
    df = (pd.merge(transactions_df, entries_df, how='left', left_on='entry', right_on='entry_id')
            .drop(columns=['entry', 'entry_id']))

    # Left join to get transfer in name
    df = pd.merge(df, elements_df, how='left',
                  left_on='element_in', right_on='id')

    # Left join to get transfer out name
    df = pd.merge(df, elements_df, how='left',
                  left_on='element_out', right_on='id')

    # Cleaning data
    df = (df.rename(columns={'player_first_name': 'team', 'web_name_x': 'player_in', 'web_name_y': 'player_out', 'id_x': 'player_in_id', 'id_y': 'player_out_id'})
          # Reorder columns
          [[
              'team',
              'event',
              'kind',
              'player_in',
              'player_in_id',
              'player_out',
              'player_out_id',
              'result'
          ]]
          )

    if accepted == True:  # only return accepted transactions
        df = df[df['result'] == 'a']
    return df


def get_trxn_rankings(df, accepted=True, event=None):
    """
    Returns a dataframe of transactions for a given gameweek
    """

    if accepted == True:
        df = df[df['result'] == 'a']

    if event:
        df = df[df['event'] == event]

    df = (df['team'].reset_index()
          .groupby('team')
          .count()
          .sort_values(by='index', ascending=False)
          .rename(columns={'index': 'count'})
          )

    return df
