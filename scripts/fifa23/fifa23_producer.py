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

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

readers = {}

for file_path in RAW_DIR.glob("*.csv"):
    topic = FILE_TOPIC_MAP.get(file_path.name)

    if topic is None:
        print(f"Skip {file_path.name}")
        continue

    readers[file_path.name] = {
        "reader": pd.read_csv(file_path, chunksize=CHUNK_SIZE),
        "topic": topic,
        "done": False,
        "chunk_no": 0,
    }

print("Files loaded:")
for fname, info in readers.items():
    print(f"- {fname} → {info['topic']}")

while True:
    all_done = True

    for fname, info in readers.items():
        if info["done"]:
            continue

        try:
            chunk = next(info["reader"])
            topic = info["topic"]

            chunk["source_file"] = fname
            chunk["chunk_no"] = info["chunk_no"]

            records = chunk.to_dict(orient="records")

            for row in records:
                producer.send(topic, row)

            producer.flush()

            print(f"Sent {fname} chunk {info['chunk_no']} rows={len(chunk)}")

            info["chunk_no"] += 1
            all_done = False

            time.sleep(0.1)

        except StopIteration:
            print(f"{fname} DONE")
            producer.send(info["topic"], {
                "__type": "END_OF_FILE",
                "source_file": fname
            })
            producer.flush()

            info["done"] = True

    if all_done:
        break

producer.close()
print("ALL FILES DONE")