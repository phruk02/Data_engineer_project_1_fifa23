import json
import requests
from kafka import KafkaProducer


def main():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 13.75,
        "longitude": 100.50,
        "current_weather": True
    }

    response = requests.get(url, params=params)
    data = response.json()
    weather = data["current_weather"]

    event = {
        "temperature": weather["temperature"],
        "windspeed": weather["windspeed"],
        "timestamp": weather["time"]
    }

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    producer.send("weather_raw", event)
    producer.flush()
    producer.close()

    print("✅ Sent to Kafka:", event)


if __name__ == "__main__":
    main()