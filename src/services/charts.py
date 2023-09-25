import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from services.transactions import get_transactions_df
from etl_scripts.api import get_gameweek
from etl_scripts.utils import get_dataframe, get_team_players_gw_data, get_player_gameweek_data

COLORS = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:purple',
          'tab:brown', 'tab:pink', 'tab:olive', 'tab:cyan', 'tab:gray']
NAMES = list(get_dataframe('league_entries')['player_first_name'])

COLOR_MAPPINGS = {name: color for name, color in zip(NAMES, COLORS)}


def chart_league_standings_history():
    """
    Creates a chart of the league standings over time
    """

    # Pull required data
    matches_df = get_dataframe('matches')
    matches_df = matches_df[matches_df['finished'] == True]
    league_entry_df = get_dataframe('league_entries')

    # Join to get team names
    matches_df = pd.merge(matches_df,
                          league_entry_df[['id', 'player_first_name']],
                          how='left',
                          left_on='league_entry_1',
                          right_on='id')

    # Join to get team names and player names of entry 2 (away team)
    matches_df = pd.merge(matches_df,
                          league_entry_df[['id', 'player_first_name']],
                          how='left',
                          left_on='league_entry_2',
                          right_on='id')

    # Drop unused columns, rename for clearer columns
    matches_df = (matches_df
                  .drop(['finished', 'started', 'id_x', 'id_y', 'league_entry_1', 'league_entry_2'], axis=1)
                  .rename(columns={'event': 'match',
                                   'player_first_name_x': 'home_player',
                                   'league_entry_1_points': 'home_score',
                                   'player_first_name_y': 'away_player',
                                   'league_entry_2_points': 'away_score',
                                   }))

    def calc_points(df):
        if df['home_score'] == df['away_score']:
            df['home_points'] = 1
            df['away_points'] = 1
        elif df['home_score'] > df['away_score']:
            df['home_points'] = 3
            df['away_points'] = 0
        else:
            df['home_points'] = 0
            df['away_points'] = 3

        return df

    matches_df = matches_df.apply(calc_points, axis=1)

    home_df = matches_df[['match', 'home_player', 'home_score', 'home_points']]
    home_df = home_df.rename(
        columns={'home_player': 'team', 'home_score': 'score', 'home_points': 'points'})

    away_df = matches_df[['match', 'away_player', 'away_score', 'away_points']]
    away_df = away_df.rename(
        columns={'away_player': 'team', 'away_score': 'score', 'away_points': 'points'})

    matches_df_stacked = pd.concat([home_df, away_df], ignore_index=True)
    matches_df_stacked = matches_df_stacked.sort_values(
        by='match').reset_index().drop(['index'], axis=1)

    pivot_df = matches_df_stacked.pivot(
        index='match', columns='team', values=['points'])

    output_df = pivot_df.cumsum()
    gameweek_zero = pd.Series(
        [0] * len(output_df.columns), index=output_df.columns).to_frame().T
    output_df = pd.concat([gameweek_zero, output_df], ignore_index=True)

    names = list(league_entry_df['player_first_name'])

    plt.figure(figsize=[15, 6])

    for name in names:
        plt.plot(output_df['points'][name], label=name,
                 marker='o', linewidth=2, color=COLOR_MAPPINGS[name])

    ax = plt.gca()

    ax.set_xticks(range(0, len(output_df) + 1, 1))
    ax.set_xticklabels(range(0, len(output_df) + 1, 1))
    ax.set_xlabel('Gameweek #')
    ax.set_ylabel('Points total')
    ax.set_title('FPL Draft League - Points over time')

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    plt.legend(loc='upper right')

    standings_path = os.path.join("data", "charts", "standings.png")
    plt.savefig(standings_path)

    return standings_path


def chart_top_n_players(n=10):
    """
    Creates a chart of the top N players for the current gameweek
    """

    # Element status and filter to owned players only
    element_status_df = get_dataframe('element_status')
    element_status_df = element_status_df[element_status_df['status'] == 'o']
    element_status_df = element_status_df[['element', 'owner']]

    # League entries
    le_df = get_dataframe('league_entries')
    le_df = le_df[['player_first_name', 'entry_id']]

    # Join owner players with league entries (cleaning)
    owner_df = pd.merge(element_status_df, le_df, how='left',
                        left_on='owner', right_on='entry_id')
    owner_df = owner_df.drop(columns=['owner', 'entry_id'])

    # Get the actual element data
    elements_df = get_dataframe('elements')
    elements_df = elements_df[['id', 'web_name']]

    # Intermediate player ownership df, merging owners with element details
    po_df = pd.merge(owner_df, elements_df, left_on='element', right_on='id')
    po_df = po_df.drop(columns=['id'])

    # Pull all the teams' players gameweek data
    df = get_team_players_gw_data()

    gameweek, _ = get_gameweek()
    # Limit to just the latest completed gameweek
    df = df[df['event'] == gameweek]

    # Build the final players_df in the clean form we want
    players_df = pd.merge(df,
                          po_df,
                          how='left',
                          left_on='element',
                          right_on='element')

    players_df = players_df[['web_name', 'player_first_name', 'total_points',
                             'goals_scored', 'goals_conceded', 'assists', 'bonus', 'event']]

    players_df = players_df.groupby(
        by=['web_name', 'player_first_name'], as_index=False).sum()

    # The final df we need :D filtered to specified top 'n'
    players_df = players_df.sort_values(
        by='total_points', ascending=False).head(n)

    # Get list of league entry players
    player_list = list(players_df['player_first_name'])

    plt.figure(figsize=[10, 5])

    mybar = plt.bar(range(n),
                    players_df['total_points'],
                    tick_label=players_df['web_name']
                    )

    # Add value labels on top of the bars
    for i, v in enumerate(list(players_df['total_points'])):
        plt.text(i, v + 0.5, str(v), ha='center')

    for i, player in zip(range(len(player_list)), player_list):
        mybar[i].set_color(COLOR_MAPPINGS[player])
        mybar[i].set_label(player)

    ax = plt.gca()
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    # plt.legend(by_label.values(), by_label.keys(), loc='upper right')
    plt.legend(by_label.values(), by_label.keys(), bbox_to_anchor=(
        1.1, 1), loc='upper right', borderaxespad=0)
    plt.xticks(rotation=80)

    ax.set_xlabel('Top players')
    ax.set_ylabel('Points total')
    ax.set_title('Top 10 owned players this week')
    ax.set_yticks(range(0, 0))

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    plt.tight_layout()
    top_players_path = os.path.join("data", "charts", "top_n_players.png")
    plt.savefig(top_players_path)

    return top_players_path


def chart_current_streaks():
    """
    Creates a chart of the current winning / losing streaks for every fpl manager
    """

    league_entry_df = get_dataframe('league_entries')
    matches_df = get_dataframe('matches')
    stacked_df = get_matches_stacked(matches_df, league_entry_df)
    streaks_df = get_streaks(stacked_df)

    final_df = streaks_df[streaks_df['match'] == streaks_df.match.max()].sort_values(
        by='streak', ascending=False)[['team', 'streak']]
    final_df = final_df.sort_values(by='streak', ascending=True)

    # Setup the colors to apply
    colors = []

    for team in final_df['team']:
        if final_df[final_df['team'] == team]['streak'].values < 0:
            colors.append('#e90052')

        elif final_df[final_df['team'] == team]['streak'].values == 0:
            colors.append('#04f5ff')

        elif final_df[final_df['team'] == team]['streak'].values > 0:
            colors.append('#00ff85')

    # Build the plot
    plt.figure()
    plt.barh(range(len(NAMES)), final_df['streak'], color=colors)
    ax = plt.gca()

    ax.set_xlabel('Current Streak')
    ax.set_title('Current Team Streaks')

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    ax.set_yticks(range(len(NAMES)))
    ax.set_yticklabels(final_df['team'], va='center')

    streaks_path = os.path.join("data", "charts", "streaks.png")
    plt.savefig(streaks_path)

    return streaks_path


def chart_net_xfer_value(gameweek):
    """
    Creates a chart of the net transfer value for each transfer made in the given gameweek
    """
    trxns_df = get_transactions_df(gameweek, accepted=True)
    elements_to_pull = list(
        trxns_df['player_in_id']) + list(trxns_df['player_out_id'])
    gw_data_df = get_player_gameweek_data(elements_to_pull, gameweek)

    trxns_df = (
        pd.merge(trxns_df, gw_data_df,
                 left_on='player_in_id', right_on='element')
        .drop(columns=['element'])
        .rename(columns={'total_points': 'player_in_points'})
    )

    trxns_df = (
        pd.merge(trxns_df, gw_data_df,
                 left_on='player_out_id', right_on='element')
        .drop(columns=['element'])
        .rename(columns={'total_points': 'player_out_points'})
    )

    trxns_df = trxns_df[[
        'team',
        'event',
        'kind',
        # Player details
        'player_in',
        'player_in_id',
        'player_out',
        'player_out_id',
        # Player points
        'player_in_points',
        'player_out_points',
        'result'
    ]]
    trxns_df = trxns_df.drop_duplicates()
    trxns_df['net_xfer_value'] = trxns_df['player_in_points'] - \
        trxns_df['player_out_points']

    plt.figure(figsize=(8, 8))
    my_bar = plt.barh(trxns_df.player_out + ':\n' +
                      trxns_df.player_in, trxns_df['net_xfer_value'])

    for i, team in enumerate(trxns_df['team']):
        my_bar[i].set_label(team)
        my_bar.patches[i].set_edgecolor('white')
        my_bar.patches[i].set_facecolor(COLOR_MAPPINGS[team])

    values = list(trxns_df['net_xfer_value'])
    min_val = min(values)
    max_val = max(values)
    plt.axvline(x=0, color='grey')
    for i, v in enumerate(values):
        if v < 0:
            plt.text(v - .5, i, str(v), color='black',
                     va='center', fontsize=10)
        else:
            plt.text(v + .15, i, str(v), color='black',
                     va='center', fontsize=10)

    ax = plt.gca()
    ax.set_xticks(np.arange(min(0, ((min_val - 1) // 2) * 2),
                  max(2, ((max_val + 1) // 2) * 2) + 1, step=2))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_title("Gameweek net transfer value", y=1.08)
    ax.set_ylabel("Player out : Player in")
    ax.set_xlabel("Net transfer value")

    handles, labels = ax.get_legend_handles_labels()
    unique = [(h, l) for i, (h, l) in enumerate(
        zip(handles, labels)) if l not in labels[:i]]
    ax.legend(*zip(*unique))

    # Adjust the plot layout to make room for the legend
    plt.tight_layout()

    transfers_path = os.path.join("data", "charts", "transfers.png")
    plt.savefig(transfers_path)

    return transfers_path


def get_matches_stacked(matches_df, league_entry_df):
    """
    Returns a dataframe of all finished matches for every fpl manager
    """

    # Limit to finished games only
    matches_df = matches_df[matches_df['started'] == True]

    if not matches_df.empty:

        # Join to get team names and player names of entry 1 (home team)
        matches_df = pd.merge(matches_df,
                              league_entry_df[['id', 'player_first_name']],
                              how='left',
                              left_on='league_entry_1',
                              right_on='id')

        # Join to get team names and player names of entry 2 (away team)
        matches_df = pd.merge(matches_df,
                              league_entry_df[['id', 'player_first_name']],
                              how='left',
                              left_on='league_entry_2',
                              right_on='id')

        # Drop unused columns, rename for clearer columns
        matches_df = (matches_df
                      .drop(['finished', 'started', 'id_x', 'id_y', 'league_entry_1', 'league_entry_2'], axis=1)
                      .rename(columns={'event': 'match',
                                       'player_first_name_x': 'home_player',
                                       'league_entry_1_points': 'home_score',
                                       'player_first_name_y': 'away_player',
                                       'league_entry_2_points': 'away_score',
                                       })
                      )

        matches_df = matches_df.apply(calc_points, axis=1)

        home_df = matches_df[['match', 'home_player',
                              'home_score', 'home_points', 'home_margin']]
        home_df = home_df.rename(columns={
                                 'home_player': 'team', 'home_score': 'score', 'home_points': 'points', 'home_margin': 'margin'})

        away_df = matches_df[['match', 'away_player',
                              'away_score', 'away_points', 'away_margin']]
        away_df = away_df.rename(columns={
                                 'away_player': 'team', 'away_score': 'score', 'away_points': 'points', 'away_margin': 'margin'})

        matches_df_stacked = pd.concat([home_df, away_df], ignore_index=True)
        matches_df_stacked = matches_df_stacked.sort_values(
            by='match').reset_index().drop(['index'], axis=1)

        return matches_df_stacked


def calc_points(df):
    """
    Calculates the points won for each fpl manager
    """
    if df['home_score'] == df['away_score']:
        df['home_points'] = 1
        df['away_points'] = 1
    elif df['home_score'] > df['away_score']:
        df['home_points'] = 3
        df['away_points'] = 0
    else:
        df['home_points'] = 0
        df['away_points'] = 3

    df['home_margin'] = df['home_score'] - df['away_score']
    df['away_margin'] = df['away_score'] - df['home_score']

    return df


def chart_margins_single(df, player):
    """
    Creates a chart of the margin victory / loss for a single fpl manager
    """

    df = df[df['team'] == player]
    my_color = np.where(df['margin'] > 0, '#00ff85', '#e90052')
    plt.figure(figsize=(10, 6))

    plt.bar(df['match'], df['margin'], color=my_color)

    ax = plt.gca()
    ax.set_xticks(range(1, len(df) + 1, 1))
    ax.set_yticks(range(-50, 50, 10))
    ax.set_xticklabels(range(1, len(df) + 1, 1))
    ax.set_xlabel('Gameweek #')
    ax.set_ylabel('Points margin')
    ax.set_title('Gameweek Points Margins')
    ax.grid(True, color='#778899', alpha=0.1, axis='y')
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)


def chart_margins_multi():
    """
    Creates a chart of the margin victory / loss for every fpl manager
    """

    league_entry_df, matches_df = get_dataframe(
        'league_entries'), get_dataframe('matches')
    stacked_df = get_matches_stacked(matches_df, league_entry_df)

    names = list(stacked_df.team.unique())
    name_dict = dict(enumerate(names))

    rows, cols = 2, 4

    fig1, ax = plt.subplots(rows, cols, figsize=(28, 9), sharex='col')

    fig1.suptitle("Gameweek Margins by player", fontsize=30)

    counter = 0
    for i in range(rows):
        for j in range(cols):
            my_color = np.where(
                stacked_df[stacked_df['team'] == name_dict[counter]]['margin'] > 0, '#00ff85', '#e90052')
            ax[i, j].bar(stacked_df[stacked_df['team'] == name_dict[counter]]['match'],
                         stacked_df[stacked_df['team'] ==
                                    name_dict[counter]]['margin'],
                         color=my_color
                         )
            ax[i, j].set_title(f"{name_dict[counter]}", fontsize=20)
            ax[i, j].set_xticks(
                range(1, len(stacked_df[stacked_df['team'] == name_dict[counter]]) + 1, 1))
            ax[i, j].set_yticks(range(-60, 60, 10))
            ax[i, j].set_xticklabels(range(1, len(
                stacked_df[stacked_df['team'] == name_dict[counter]]) + 1, 1), fontsize=10)
            ax[i, j].set_xlabel('Gameweek #', fontsize=15)
            ax[i, j].set_ylabel('Points margin', fontsize=15)
            ax[i, j].grid(True, color='#778899', alpha=0.1, axis='y')
            ax[i, j].spines['right'].set_visible(False)
            ax[i, j].spines['top'].set_visible(False)

            counter += 1
    margins_path = os.path.join("data", "charts", "margin_chart.png")
    plt.savefig(margins_path)

    return margins_path


def get_streaks(matches_df_stacked):
    """
    Returns a dataframe of the winning / losing streaks of each fpl manager
    """

    df = matches_df_stacked

    def win_lose_bin(df):
        if df['points'] == 3:
            df['binary'] = 1
        elif df['points'] == 1:
            df['binary'] = 0
        elif df['points'] == 0:
            df['binary'] = -1

        return df

    df = (df.apply(win_lose_bin, axis=1)
          .sort_values(by=['team', 'match'])
          )

    teams_grpd = df.groupby('team', as_index=False)

    def get_team_streaks(group):

        grouper = (group.binary != group.binary.shift()).cumsum()
        group['streak'] = group['binary'].groupby(grouper).cumsum()

        return group

    df = teams_grpd.apply(get_team_streaks)

    return df


def chart_trxn_vol(df):
    """
    Creates a chart of the transaction volume by gameweek
    """
    df = (df[['event', 'player_in']]
          .groupby('event')
          .count()
          .reset_index()
          )

    plt.figure()

    plt.bar(df['event'], df['player_in'])

    ax = plt.gca()
    gameweek, _ = get_gameweek()
    ax.set_xticks(range(1, gameweek + 1))
    ax.set_xticklabels(range(1, gameweek + 1))
    ax.set_title('FPL Draft League - Transfer volume by gameweek')

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    plt.show()

    return df
