from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="gold_team_pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["fifa23", "gold", "team"],
) as dag:

    @task
    def create_tables():
        engine = create_engine(DB_URL)

        sql = """
        CREATE TABLE general_info_team (
            team_id BIGINT NOT NULL,
            team_name TEXT NOT NULL,
            fifa_version BIGINT NOT NULL,
            type VARCHAR(20) NOT NULL CHECK (type IN ('club', 'nation')),

           
            league_id BIGINT,
            league_name TEXT,
            nationality_id BIGINT,
            nationality_name TEXT,
            overall BIGINT CHECK (overall BETWEEN 1 AND 99 OR overall IS NULL),
            rival_team_id DOUBLE PRECISION,
            coach_id DOUBLE PRECISION,
            home_stadium TEXT,
            international_prestige BIGINT,
            domestic_prestige DOUBLE PRECISION,
            transfer_budget_eur DOUBLE PRECISION CHECK (transfer_budget_eur >= 0 OR transfer_budget_eur IS NULL),
            club_worth_eur DOUBLE PRECISION CHECK (club_worth_eur >= 0 OR club_worth_eur IS NULL),

            PRIMARY KEY (team_id)
        );

        CREATE TABLE stats_team (
            team_id BIGINT NOT NULL,
            team_name TEXT NOT NULL,
            fifa_version BIGINT NOT NULL,
            type VARCHAR(20) NOT NULL CHECK (type IN ('club', 'nation')),

            league_name TEXT,
            league_level DOUBLE PRECISION,
            overall BIGINT CHECK (overall BETWEEN 1 AND 99 OR overall IS NULL),
            attack BIGINT CHECK (attack BETWEEN 1 AND 99 OR attack IS NULL),
            midfield BIGINT CHECK (midfield BETWEEN 1 AND 99 OR midfield IS NULL),
            defence BIGINT CHECK (defence BETWEEN 1 AND 99 OR defence IS NULL),

            starting_xi_average_age DOUBLE PRECISION,
            whole_team_average_age DOUBLE PRECISION,

            captain_player_id BIGINT,
            short_free_kick_player_id BIGINT,
            long_free_kick_player_id BIGINT,
            left_short_free_kick_player_id BIGINT,
            right_short_free_kick_player_id BIGINT,
            penalties_player_id BIGINT,
            left_corner_player_id BIGINT,
            right_corner_player_id BIGINT,

            PRIMARY KEY (team_id)
        );

        CREATE TABLE play_style_team (
            team_id BIGINT NOT NULL,
            team_name TEXT NOT NULL,
            fifa_version BIGINT NOT NULL,
            type VARCHAR(20) NOT NULL CHECK (type IN ('club', 'nation')),

            league_name TEXT,

            def_style TEXT,
            def_team_width BIGINT,
            def_team_depth BIGINT,

            off_build_up_play TEXT,
            off_chance_creation TEXT,
            off_team_width BIGINT,
            off_players_in_box BIGINT,
            off_corners BIGINT,
            off_free_kicks BIGINT,

            PRIMARY KEY (team_id)
        );

        CREATE TABLE version_and_debug_team (
            team_id BIGINT NOT NULL,
            team_name TEXT NOT NULL,
            fifa_version BIGINT NOT NULL,

            team_url TEXT,
            fifa_update BIGINT,
            fifa_update_date DATE,
            source_file TEXT,
            chunk_no BIGINT,

            PRIMARY KEY (team_id) 
        );

        CREATE VIEW latest_fifa23 AS
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY team_id
                    ORDER BY
                        fifa_update DESC NULLS LAST,
                        fifa_update_date DESC NULLS LAST
                ) AS rn
            FROM silver_male_teams
            WHERE fifa_version = 23
        ) x
        WHERE rn = 1;
        """

        with engine.begin() as conn:
            conn.execute(text("""DROP VIEW IF EXISTS latest_fifa23 CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS general_info_team CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS stats_team CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS play_style_team CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS version_and_debug_team CASCADE;"""))
            conn.execute(text(sql))

    @task
    def insert_general_info_team():
        engine = create_engine(DB_URL)

        sql = """
                
        INSERT INTO general_info_team (
            team_id,
            team_name,
            fifa_version,
            type,
            league_id,
            league_name,
            nationality_id,
            nationality_name,
            overall,
            rival_team_id,
            coach_id,
            home_stadium,
            international_prestige,
            domestic_prestige,
            transfer_budget_eur,
            club_worth_eur
        )
        SELECT
            team_id,
            team_name,
            fifa_version,
            CASE
                WHEN league_name = 'Friendly International' THEN 'nation'
                ELSE 'club'
            END AS type,
            league_id,
            league_name,
            nationality_id,
            nationality_name,
            overall,
            rival_team_id,
            coach_id,
            home_stadium,
            international_prestige,
            domestic_prestige,
            transfer_budget_eur,
            club_worth_eur
        FROM latest_fifa23;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    @task
    def insert_stats_team():
        engine = create_engine(DB_URL)

        sql = """
                
        INSERT INTO stats_team (
            team_id,
            team_name,
            fifa_version,
            type,
            league_name,
            league_level,
            overall,
            attack,
            midfield,
            defence,
            starting_xi_average_age,
            whole_team_average_age,
            captain_player_id,
            short_free_kick_player_id,
            long_free_kick_player_id,
            left_short_free_kick_player_id,
            right_short_free_kick_player_id,
            penalties_player_id,
            left_corner_player_id,
            right_corner_player_id
        )
        SELECT
            team_id,
            team_name,
            fifa_version,
            CASE
                WHEN league_name = 'Friendly International' THEN 'nation'
                ELSE 'club'
            END AS type,
            league_name,
            league_level,
            overall,
            attack,
            midfield,
            defence,
            starting_xi_average_age,
            whole_team_average_age,
            captain_player_id,
            short_free_kick_player_id,
            long_free_kick_player_id,
            left_short_free_kick_player_id,
            right_short_free_kick_player_id,
            penalties_player_id,
            left_corner_player_id,
            right_corner_player_id
        FROM latest_fifa23;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    @task
    def insert_play_style_team():
        engine = create_engine(DB_URL)

        sql = """
                
        INSERT INTO play_style_team (
            team_id,
            team_name,
            fifa_version,
            type,
            league_name,
            def_style,
            def_team_width,
            def_team_depth,
            off_build_up_play,
            off_chance_creation,
            off_team_width,
            off_players_in_box,
            off_corners,
            off_free_kicks
        )
        SELECT
            team_id,
            team_name,
            fifa_version,
            CASE
                WHEN league_name = 'Friendly International' THEN 'nation'
                ELSE 'club'
            END AS type,
            league_name,
            def_style,
            def_team_width,
            def_team_depth,
            off_build_up_play,
            off_chance_creation,
            off_team_width,
            off_players_in_box,
            off_corners,
            off_free_kicks
        FROM latest_fifa23;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    @task
    def insert_version_and_debug_team():
        engine = create_engine(DB_URL)

        sql = """
                
        INSERT INTO version_and_debug_team (
            team_id,
            team_name,
            fifa_version,
            team_url,
            fifa_update,
            fifa_update_date,
            source_file,
            chunk_no
        )
        SELECT
            team_id,
            team_name,
            fifa_version,
            team_url,
            fifa_update,
            fifa_update_date,
            source_file,
            chunk_no
        FROM latest_fifa23;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    create_tables() >> [
        insert_general_info_team(),
        insert_stats_team(),
        insert_play_style_team(),
        insert_version_and_debug_team(),
    ]