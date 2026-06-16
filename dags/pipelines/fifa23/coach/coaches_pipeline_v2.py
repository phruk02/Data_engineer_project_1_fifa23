from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="cleaned_male_coaches_v2",
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["fifa23", "clean", "coaches","v2"],
) as dag:

    @task
    def clean_coaches():
        engine = create_engine(DB_URL)

        sql = """
        DROP TABLE IF EXISTS fifa23_gold.cleaned_male_coaches CASCADE;

        CREATE TABLE fifa23_gold.cleaned_male_coaches (
            coach_id BIGINT NOT NULL CHECK (coach_id > 0),
            coach_url TEXT,
            short_name TEXT NOT NULL,
            long_name TEXT,
            dob DATE,
            nationality_id BIGINT CHECK (nationality_id > 0 OR nationality_id IS NULL),
            nationality_name TEXT,
            face_url TEXT,
            source_file TEXT,
            chunk_no BIGINT CHECK (chunk_no >= 0 OR chunk_no IS NULL),

            PRIMARY KEY (coach_id),
            
            CONSTRAINT fk_coach_nationality
            FOREIGN KEY (nationality_id)
            REFERENCES fifa23_gold.dim_nationality(nationality_id)
        );

        WITH cleaned AS (
            SELECT
                coach_id,
                NULLIF(TRIM(coach_url), '') AS coach_url,
                NULLIF(TRIM(short_name), '') AS short_name,
                NULLIF(TRIM(long_name), '') AS long_name,
                TO_DATE(NULLIF(TRIM(dob), ''), 'YYYY-MM-DD') AS dob,
                nationality_id,
                COALESCE(NULLIF(TRIM(nationality_name), ''), 'Unknown') AS nationality_name,
                NULLIF(TRIM(face_url), '') AS face_url,
                NULLIF(TRIM(source_file), '') AS source_file,
                chunk_no
            FROM fifa23_raw.raw_male_coaches
            WHERE coach_id IS NOT NULL
              AND coach_id > 0
              AND short_name IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY coach_id
                    ORDER BY
                        source_file,
                        chunk_no
                ) AS rn
            FROM cleaned
            WHERE short_name IS NOT NULL
        )
        INSERT INTO fifa23_gold.cleaned_male_coaches (
            coach_id,
            coach_url,
            short_name,
            long_name,
            dob,
            nationality_id,
            nationality_name,
            face_url,
            source_file,
            chunk_no
        )
        SELECT
            coach_id,
            coach_url,
            short_name,
            long_name,
            dob,
            nationality_id,
            nationality_name,
            face_url,
            source_file,
            chunk_no
        FROM ranked
        WHERE rn = 1;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    clean_coaches()