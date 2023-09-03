import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from services.sql import query_table
from process import process_players
from etl_scripts.api import get_players, get_transactions
from services.build_charts import build_charts
from services.send_email import send_email

GAMEWEEK = 4

# entry point to program
def main():
    
    get_transactions()
    get_players()
    report_path, message_body = process_players()
    
    paths = build_charts(GAMEWEEK)

    rc = send_email(paths + [report_path], message_body)
    print(rc)

    return
       
    
if __name__ == "__main__":
    main()