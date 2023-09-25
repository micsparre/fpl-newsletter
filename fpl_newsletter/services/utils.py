import json
import os


def load_json(filename):
    """
    Loads a json file based on the filename
    """
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            table_json = json.load(f)
        return table_json
    else:
        print(f"invalid filename: {filename}")
        raise Exception("File issue during json load")
