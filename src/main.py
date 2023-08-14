import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from services.sql import query_table
from process import process_players
from etl_scripts.api import get_players

# entry point to program
def main():
    get_players()
    rc = process_players()
    print(rc)
    # query_table("players")
       
    
if __name__ == "__main__":
    main()