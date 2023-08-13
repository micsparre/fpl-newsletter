from sql import create_tables, print_tables, query_table
from process import process_players
from api import get_players
from db import Players

def main():
    # create_tables()
    # print_tables()
    # get_players()
    rc = process_players()
    print(rc)
    # query_table("players")
       
    
if __name__ == "__main__":
    main()