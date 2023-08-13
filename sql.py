import os
import glob
import sqlite3
import pandas as pd
from api import get_data
from utils import load_json

DB_NAME = "fpl-draft.db"

# close db connection and cursor
def close_connection(cursor, conn):
    if cursor:
        cursor.close()
    if conn:
        conn.close
    return

# get cursor and connection objects
def connect():
    # Connect to an SQLite database (creates a new file if it doesn't exist)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    return conn, cursor


# create file based on schema json file
def create_sql(json_file):
    table_json = load_json(json_file)
    table_name = os.path.basename(json_file).replace(".json", "")
    fields = table_json["fields"]
    sql = f"""\
CREATE TABLE IF NOT EXISTS {table_name}
    ({", ".join([f"{key} {fields[key]}" for key in fields])}, PRIMARY KEY({table_json["primary_key"]}))
"""
    return sql

# create tables based on schema defined in the schema folder
def create_tables():
    folder_path = "schema"
    table_wildcard = "*.json"
    filenames = glob.glob(os.path.join(folder_path, table_wildcard))
    conn, cursor = connect()
    for file in filenames:
        sql = create_sql(file)
        print(f"sql: {sql}")
        cursor.execute(sql)
        conn.commit()
        
    close_connection(cursor, conn)
    return
    
# prints table names in db
def print_tables():
    PRINT_SQL = "SELECT name FROM sqlite_master WHERE type='table';"
    conn, cursor = connect()
    cursor.execute(PRINT_SQL)
    tables = cursor.fetchall()
    for table in tables:
        print(f"table: {table[0]}")
        
# print 10 rows from a given table_name
def query_table(table_name):
    SQL = f"SELECT * FROM {table_name} LIMIT 10;"
    # SQL = f"SELECT distinct status FROM {table_name} LIMIT 10;"
    conn, cursor = connect()
    cursor.execute(SQL)
    rows = cursor.fetchall()
    for row in rows:
        print(f"row: {row}")
    close_connection(cursor, conn)
        
# print count of records from a given table_name
def count_table(table_name):
    SQL = f"SELECT COUNT(*) FROM {table_name};"
    conn, cursor = connect()
    cursor.execute(SQL)
    num_rows = cursor.fetchall()
    print(f"num rows in {table_name}: {num_rows}")
    close_connection(cursor, conn)

# return df loaded from sqlite    
def get_df_from_table(table_name):
    conn, cursor = connect()
    SQL = f"SELECT * from {table_name}"
    df = pd.read_sql_query(SQL, conn)
    close_connection(cursor, conn)
    return df

# creates sqlite connection and executes query and returns result
def execute_query(SQL):
    conn, cursor = connect()
    cursor.execute(SQL)
    rows = cursor.fetchall()
    conn.commit()
    close_connection(cursor, conn)
    for row in rows:
        print(row)
    return rows