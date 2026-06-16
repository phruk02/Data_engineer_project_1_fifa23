import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
engine = create_engine(DB_URL)

df = pd.read_sql(
    text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'raw_male_players_legacy'
        ORDER BY ordinal_position
    """),
    engine
)

print(df)