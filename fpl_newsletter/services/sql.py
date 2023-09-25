import os
import glob
import sqlite3
import pandas as pd
from services.utils import load_json

DB_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "data", "fpl-draft.db")


def close_connection(cursor, conn):
    """
    Closes the connection and cursor
    """
    if cursor:
        cursor.close()
    if conn:
        conn.close
    return


def connect():
    """
    Connects to the sqlite database and returns the connection and cursor objects
    """
    # Connect to an SQLite database (creates a new file if it doesn't exist)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    return conn, cursor


def create_sql(json_file):
    """
    Creates the sql statement based on the schema json file
    """
    table_json = load_json(json_file)
    table_name = os.path.basename(json_file).replace(".json", "")
    fields = table_json["fields"]
    foreign_key = table_json.get("foreign_key")
    sql = f"""\
CREATE TABLE IF NOT EXISTS {table_name}
    ({", ".join([f"{key} {fields[key]}" for key in fields])} {f", {foreign_key}" if foreign_key else ""} )
"""
    return sql


def create_tables():
    """
    Creates the tables based on the schema defined in the schema folder
    """
    table_wildcard = "*.json"
    filenames = glob.glob(os.path.join("data", "schema", table_wildcard))
    conn, cursor = connect()
    for file in filenames:
        sql = create_sql(file)
        print(f"sql: {sql}")
        cursor.execute(sql)
        conn.commit()
    close_connection(cursor, conn)
    return


def print_tables():
    """
    Prints the table names in the database
    """
    PRINT_SQL = "SELECT name FROM sqlite_master WHERE type='table';"
    conn, cursor = connect()
    cursor.execute(PRINT_SQL)
    tables = cursor.fetchall()
    for table in tables:
        print(f"table: {table[0]}")
    close_connection(cursor, conn)
    return


def query_table(table_name):
    """
    Prints 10 rows from a given table_name
    """
    SQL = f"SELECT * FROM {table_name} LIMIT 10;"
    rows = execute_query(SQL)
    print(rows)
    return


def count_table(table_name):
    """
    Prints the number of rows in a given table_name
    """
    SQL = f"SELECT COUNT(*) FROM {table_name};"
    rows = execute_query(SQL)
    print(f"num rows in {table_name}: {len(rows)}")
    return


def get_df_from_table(table_name):
    """
    Returns a dataframe from a given table_name
    """
    conn, cursor = connect()
    SQL = f"SELECT * from {table_name}"
    df = pd.read_sql_query(SQL, conn)
    close_connection(cursor, conn)
    return df


def execute_query(SQL):
    """
    Executes a given SQL query and returns the result
    """
    conn, cursor = connect()
    cursor.execute(SQL)
    rows = cursor.fetchall()
    conn.commit()
    close_connection(cursor, conn)
    return rows


def reset_gameweek(gameweek, subscriber_id):
    """
    Resets the charts_sent_status for a given gameweek and subscriber_id
    """
    SQL = f"UPDATE newsletter SET charts_sent_status = 0 where gameweek={gameweek} and subscriber_id={subscriber_id};"
    execute_query(SQL)
    return


def add_owners(league_number):
    """
    Adds managers to the owners table
    """
    DB = "owners"
    filename = "details.json"
    owners_json = load_json(os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "data", "api_results", str(league_number), filename))
    owners_df = pd.json_normalize(owners_json["league_entries"])
    owners_df.rename(columns={'entry_id': 'owner_id', 'entry_name': 'team_name',
                     'player_first_name': 'first_name', 'player_last_name': 'last_name'}, inplace=True)
    owners_columns = ["owner_id", "team_name", "first_name", "last_name"]
    owners_df = owners_df[owners_columns]
    owners_df[league_number] = league_number
    conn, cursor = connect()
    owners_df.to_sql(DB, conn, if_exists='replace', index=False)
    close_connection(cursor, conn)
    return
