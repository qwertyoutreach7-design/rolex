import json
import os

DATA_FILE = "data/projects.json"

def load_projects():
    if not os.path.exists(DATA_FILE):
        return {"projects": []}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"projects": []}

def save_projects(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
