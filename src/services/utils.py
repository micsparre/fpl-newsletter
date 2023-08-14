import json
import os

# load json based on filename
def load_json(filename):
    # print(filename)
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            table_json = json.load(f)
        return table_json
    else:
        print("invalid filename")
        raise Exception("File issue during json load")