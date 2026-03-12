import json
import os

def get_mock_study_response():
    current_dir = os.path.dirname(__file__)
    json_path = os.path.join(current_dir, "../data/noaa/response.json")
    with open(json_path, "r") as f:
        return json.load(f)
