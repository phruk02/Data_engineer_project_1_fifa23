import json
import psycopg2
from kafka import KafkaConsumer


def main():
    consumer = KafkaConsumer(
        "weather_raw",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="weather-group"
    )

    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="postgres",
        user="postgres",
        password="1234"
    )

    cur = conn.cursor()

    for message in consumer:
        data = message.value

        temperature = data["temperature"]
        windspeed = data["windspeed"]
        timestamp = data["timestamp"]

        cur.execute("""
            INSERT INTO weather_data (temperature, windspeed, timestamp)
            VALUES (%s, %s, %s)
        """, (temperature, windspeed, timestamp))

        conn.commit()

        print("✅ Inserted from Kafka:", data)


if __name__ == "__main__":
    main()