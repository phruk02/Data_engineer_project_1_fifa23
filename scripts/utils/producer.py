import requests
import psycopg2


def main():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 13.75,
        "longitude": 100.50,
        "current_weather": True
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    weather = data["current_weather"]

    temperature = weather["temperature"]
    windspeed = weather["windspeed"]
    timestamp = weather["time"]

    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="postgres",
        user="postgres",
        password="1234"
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            temperature FLOAT,
            windspeed FLOAT,
            timestamp TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO weather_data (temperature, windspeed, timestamp)
        VALUES (%s, %s, %s)
    """, (temperature, windspeed, timestamp))

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data inserted:", temperature, windspeed, timestamp)


if __name__ == "__main__":
    main()