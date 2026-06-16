from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine,text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
     dag_id="firstclean_fifa23_players_legacy",schedule=None,
     start_date=datetime(2026,6,6),catchup=False,
     tags=['fifa23_legacy','silver']
     )  as dag:

    @task
    def create_first_clean_base():
        engine = create_engine(DB_URL)
        
        sql ="""
        
        CREATE TABLE silver_players_legacy AS

        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id, fifa_version
                    ORDER BY
                        fifa_update_date DESC NULLS LAST,
                        fifa_update DESC NULLS LAST,
                        chunk_no DESC NULLS LAST
                ) AS rn
            FROM raw_male_players_legacy
            WHERE player_id IS NOT NULL
            AND short_name IS NOT NULL
            AND overall BETWEEN 1 AND 99
            AND potential BETWEEN 1 AND 99
            AND age BETWEEN 15 AND 50
            AND height_cm BETWEEN 100 AND 250
            AND weight_kg BETWEEN 40 AND 200
        )

        SELECT
            player_id,
            fifa_version,
            fifa_update,
            fifa_update_date,
            player_url,
            player_face_url,
            TRIM(short_name) AS short_name,
            TRIM(long_name) AS long_name,
            TRIM(player_positions) AS player_positions,
            overall,
            potential,
            age,
            TO_DATE(dob,'YYYY-MM-DD') AS dob,
            height_cm,
            weight_kg,
            league_id,
            COALESCE(TRIM(league_name), 'Unknown') AS league_name,
            league_level,
            club_team_id,
            COALESCE(TRIM(club_name), 'Free Agent') AS club_name,
            TRIM(club_position) AS club_position,
            club_jersey_number,
            TRIM(club_loaned_from) AS club_loaned_from,
            club_joined_date,
            club_contract_valid_until_year::INT AS club_contract_valid_until_year,
            nationality_id,
            TRIM(nationality_name) AS nationality_name,
            nation_team_id,
            nation_position,
            nation_jersey_number,
            TRIM(preferred_foot) AS preferred_foot,
            weak_foot,
            skill_moves,
            international_reputation,
            TRIM(work_rate) AS work_rate,
            TRIM(body_type) AS body_type,
            real_face,
            value_eur,
            wage_eur,
            release_clause_eur,
            player_tags,
            player_traits,
            pace,
            shooting,
            passing,
            dribbling,
            defending,
            physic,
            attacking_crossing,
            attacking_finishing,
            attacking_heading_accuracy,
            attacking_short_passing,
            attacking_volleys,
            skill_dribbling,
            skill_curve,
            skill_fk_accuracy,
            skill_long_passing,
            skill_ball_control,
            movement_acceleration,
            movement_sprint_speed,
            movement_agility,
            movement_reactions,
            movement_balance,
            power_shot_power,
            power_jumping,
            power_stamina,
            power_strength,
            power_long_shots,
            mentality_aggression,
            mentality_interceptions,
            mentality_positioning,
            mentality_vision,
            mentality_penalties,
            mentality_composure,
            defending_marking_awareness,
            defending_standing_tackle,
            defending_sliding_tackle,
            goalkeeping_diving,
            goalkeeping_handling,
            goalkeeping_kicking,
            goalkeeping_positioning,
            goalkeeping_reflexes,
            goalkeeping_speed,
            ls, st, rs,
            lw, lf, cf, rf, rw,
            lam, cam, ram,
            lm, lcm, cm, rcm, rm,
            lwb, ldm, cdm, rdm, rwb,
            lb, lcb, cb, rcb, rb,
            gk,
            source_file,
            chunk_no
        FROM ranked
        WHERE rn = 1;

        """
        with engine.begin() as conn:
            conn.execute(text(""" DROP TABLE IF EXISTS silver_players_legacy CASCADE;"""))
            conn.execute(text(sql))

    create_first_clean_base() 

