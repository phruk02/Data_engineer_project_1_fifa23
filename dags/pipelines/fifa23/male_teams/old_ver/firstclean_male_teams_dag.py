from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="first_clean_team",
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["fifa23", "silver", "team"],
) as dag:

    @task
    def create_silver_team():
        engine = create_engine(DB_URL)

        sql = """
        
        CREATE TABLE silver_male_teams (
            team_id BIGINT NOT NULL,
            team_url TEXT,
            fifa_version BIGINT NOT NULL,
            fifa_update BIGINT NOT NULL,
            fifa_update_date DATE NOT NULL,

            team_name TEXT NOT NULL,
            league_id BIGINT,
            league_name TEXT,
            league_level DOUBLE PRECISION,
            nationality_id BIGINT,
            nationality_name TEXT,

            overall BIGINT CHECK (overall BETWEEN 1 AND 99 OR overall IS NULL),
            attack BIGINT CHECK (attack BETWEEN 1 AND 99 OR attack IS NULL),
            midfield BIGINT CHECK (midfield BETWEEN 1 AND 99 OR midfield IS NULL),
            defence BIGINT CHECK (defence BETWEEN 1 AND 99 OR defence IS NULL),

            coach_id DOUBLE PRECISION,
            home_stadium TEXT,
            rival_team_id BIGINT,

            international_prestige BIGINT,
            domestic_prestige DOUBLE PRECISION,
            transfer_budget_eur DOUBLE PRECISION,
            club_worth_eur DOUBLE PRECISION CHECK (club_worth_eur >= 0 OR club_worth_eur IS NULL),

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

            def_style TEXT,
            def_team_width BIGINT,
            def_team_depth BIGINT,

            off_build_up_play TEXT,
            off_chance_creation TEXT,
            off_team_width BIGINT,
            off_players_in_box BIGINT,
            off_corners BIGINT,
            off_free_kicks BIGINT,

            source_file TEXT,
            chunk_no BIGINT,

            PRIMARY KEY (
                team_id,
                fifa_version,
                fifa_update,
                fifa_update_date
            )
        );
        """

       
        insert_sql = """
        WITH cleaned AS (
            SELECT
                team_id,
                NULLIF(TRIM(team_url), '') AS team_url,
                fifa_version,
                fifa_update,
                TO_DATE(NULLIF(TRIM(fifa_update_date), ''), 'YYYY-MM-DD') AS fifa_update_date,

                NULLIF(TRIM(team_name), '') AS team_name,
                league_id,
                COALESCE(NULLIF(TRIM(league_name), ''), 'Unknown') AS league_name,
                league_level,
                nationality_id,
                COALESCE(NULLIF(TRIM(nationality_name), ''), 'Unknown') AS nationality_name,

                overall,
                attack,
                midfield,
                defence,

                coach_id,
                NULLIF(TRIM(home_stadium), '') AS home_stadium,
                rival_team::BIGINT AS rival_team_id,

                international_prestige,
                domestic_prestige,
                transfer_budget_eur,

                CASE
                    WHEN club_worth_eur < 0 THEN NULL
                    ELSE club_worth_eur
                END AS club_worth_eur,

                starting_xi_average_age,
                whole_team_average_age,

                captain::BIGINT AS captain_player_id,
                short_free_kick::BIGINT AS short_free_kick_player_id,
                long_free_kick::BIGINT AS long_free_kick_player_id,
                left_short_free_kick::BIGINT AS left_short_free_kick_player_id,
                right_short_free_kick::BIGINT AS right_short_free_kick_player_id,
                NULLIF(penalties::TEXT, 'NaN')::DOUBLE PRECISION::BIGINT AS penalties_player_id,
                left_corner::BIGINT AS left_corner_player_id,
                NULLIF(right_corner::TEXT, 'NaN')::DOUBLE PRECISION::BIGINT AS right_corner_player_id,

                NULLIF(TRIM(def_style), '') AS def_style,
                def_team_width,
                def_team_depth,

                NULLIF(TRIM(off_build_up_play), '') AS off_build_up_play,
                NULLIF(TRIM(off_chance_creation), '') AS off_chance_creation,
                off_team_width,
                off_players_in_box,
                off_corners,
                off_free_kicks,

                NULLIF(TRIM(source_file), '') AS source_file,
                chunk_no

            FROM raw_male_teams
            WHERE team_id IS NOT NULL
              AND fifa_version IS NOT NULL
              AND fifa_update IS NOT NULL
              AND NULLIF(TRIM(fifa_update_date), '') IS NOT NULL
              AND team_name IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        team_id,
                        fifa_version,
                        fifa_update,
                        fifa_update_date
                    ORDER BY
                        source_file,
                        chunk_no
                ) AS rn
            FROM cleaned
            WHERE team_name IS NOT NULL
              AND overall BETWEEN 1 AND 99
        )
        INSERT INTO silver_male_teams
        SELECT
            team_id,
            team_url,
            fifa_version,
            fifa_update,
            fifa_update_date,

            team_name,
            league_id,
            league_name,
            league_level,
            nationality_id,
            nationality_name,

            overall,
            attack,
            midfield,
            defence,

            coach_id,
            home_stadium,
            rival_team_id,

            international_prestige,
            domestic_prestige,
            transfer_budget_eur,
            club_worth_eur,

            starting_xi_average_age,
            whole_team_average_age,

            captain_player_id,
            short_free_kick_player_id,
            long_free_kick_player_id,
            left_short_free_kick_player_id,
            right_short_free_kick_player_id,
            penalties_player_id,
            left_corner_player_id,
            right_corner_player_id,

            def_style,
            def_team_width,
            def_team_depth,

            off_build_up_play,
            off_chance_creation,
            off_team_width,
            off_players_in_box,
            off_corners,
            off_free_kicks,

            source_file,
            chunk_no
        FROM ranked
        WHERE rn = 1;
        """

        with engine.begin() as conn:
            conn.execute(text("""DROP TABLE IF EXISTS silver_male_teams CASCADE;"""))
            conn.execute(text(sql))
            conn.execute(text(insert_sql))

    create_silver_team()