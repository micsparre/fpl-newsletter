from utils import load_json
import os
import json
from sql import count_table, connect, close_connection

# ----- print columns of table -----
conn, cursor = connect()
table_name = "players"
SQL = f"PRAGMA table_info({table_name})"
cursor.execute(SQL)
cols = cursor.fetchall()
for col in cols:
    print(col[1]) # column name is in the second element of each row
close_connection(cursor, conn)


# ----- check rows in status obj == rows in players table -----
# status_json = load_json(os.path.join("data", "element-status.json"))
# print(f"num objs: {len(status_json['element_status'])}")
# count_table('players')
# -------------------------------------------------------------


# ------ example objs from elements.json -----
# player_json = load_json(os.path.join("data", "elements.json"))
# for key in player_json.keys():
#     filename = os.path.join("example-json", f"example_key_{key}.json")
#     with open(filename, 'w') as f:
#         json.dump(player_json[key], f)
# --------------------------------------------