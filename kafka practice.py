from pathlib import Path
import json
import time
import pandas as pd
from kafka import KafkaProducer

RAW_DIR = Path("/opt/airflow/data/fifa23_raw")

FILE_TOPIC_MAP = {
    "male_coaches.csv": "raw_male_coaches",
    "male_players.csv": "raw_male_players",
    "male_players_legacy.csv": "raw_male_players_legacy",
    "male_teams.csv": "raw_male_teams",
}

CHUNK_SIZE = 1000
producer = KafkaProducer(bootstrap_server='kafka:9092',value_serializer=lambda v: json.dumps(v,default=str).encode("utf-8"),)

reader = {}

for file_path in RAW_DIR.glob("*.csv"):
    topic = FILE_TOPIC_MAP.get()

    if topic is None:
        print(f"Skip {file_path.name}")
        continue
reader[file_path.name] = {
    
}
