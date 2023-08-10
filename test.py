from utils import load_json
import os
import json

player_json = load_json(os.path.join("data", "elements.json"))

for key in player_json.keys():
    filename = f"example_key_{key}"
    with open(filename, 'w') as f:
        json.dump(player_json[key], f)