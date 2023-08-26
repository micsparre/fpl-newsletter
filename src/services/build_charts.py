from services.charts import *

def build_charts(gameweek):
    standings_path = chart_league_standings_history()
    
    top_players_path = chart_top_n_players()
    
    streaks_path = chart_current_streaks()
    
    transfers_path = chart_net_xfer_value(gameweek)
    
    margins_path = chart_margins_multi()
    
    return [standings_path, top_players_path, streaks_path, transfers_path, margins_path]
    