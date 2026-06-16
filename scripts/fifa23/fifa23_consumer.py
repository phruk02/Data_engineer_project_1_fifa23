import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

TOPIC_TABLE_MAP = {
    "raw_male_coaches": "raw_male_coaches",
    "raw_male_players": "raw_male_players",
    "raw_male_players_legacy": "raw_male_players_legacy",
    "raw_male_teams": "raw_male_teams",
}

consumer = KafkaConsumer(
    *TOPIC_TABLE_MAP.keys(),
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fifa23_raw_consumer_v8",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

engine = create_engine(
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
)

buffers = {topic: [] for topic in TOPIC_TABLE_MAP}
finished_topics = set()

BATCH_SIZE = 1000


def flush_buffer(topic):
    if len(buffers[topic]) == 0:
        return

    table_name = TOPIC_TABLE_MAP[topic]
    df = pd.DataFrame(buffers[topic])

    df.to_sql(
        table_name,
        engine,
        schema="fifa23_raw",
        if_exists="append",
        index=False,
        method="multi",
    )

    print(f"Inserted {len(df)} rows into {table_name}")
    buffers[topic].clear()


try:
    for msg in consumer:
        topic = msg.topic
        row = msg.value
    
        if row.get("__type") == "END_OF_FILE":
            print(f"Received END_OF_FILE from {topic}")
          
            flush_buffer(topic)

            finished_topics.add(topic)

            if finished_topics == set(TOPIC_TABLE_MAP.keys()):
                print("All topics finished. Stopping consumer.")
                break

            continue

        buffers[topic].append(row)

        if len(buffers[topic]) >= BATCH_SIZE:
            flush_buffer(topic)

finally:
    for topic in TOPIC_TABLE_MAP:
        flush_buffer(topic)

    consumer.close()
