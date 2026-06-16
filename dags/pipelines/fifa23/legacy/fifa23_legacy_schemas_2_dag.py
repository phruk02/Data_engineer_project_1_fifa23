from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="fifa23_players_legacy_pipeline_2",
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["fifa23_legacy", "gold"],
) as dag:
        
    @task
    def create_tables_2():
        engine = create_engine(DB_URL)

        sql1 = """
            
            CREATE TABLE players_national_legacy(
    
            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK (
            fifa_version BETWEEN 6 AND 30),
            short_name VARCHAR(100) NOT NULL,

            nationality_id BIGINT CHECK (nationality_id > 0
            OR nationality_id IS NULL),
            nationality_name VARCHAR(100),
            nation_team_id BIGINT CHECK (nation_team_id > 0
            OR nation_team_id IS NULL),

            nation_position VARCHAR(20),
            nation_jersey_number INT CHECK (
            nation_jersey_number BETWEEN 1 AND 99 OR
            nation_jersey_number IS NULL),

            PRIMARY KEY (player_id, fifa_version)
            );

        """

        sql2 = """
         
            CREATE TABLE players_main_stats_legacy (

            
            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK (fifa_version BETWEEN 6 AND 30),
            short_name VARCHAR(100) NOT NULL,

            preferred_foot VARCHAR(10) CHECK (preferred_foot IN ('Left', 'Right')
            OR preferred_foot IS NULL),

            weak_foot INT CHECK (weak_foot BETWEEN 1 AND 5
            OR weak_foot IS NULL),
            skill_moves INT CHECK (skill_moves BETWEEN 1 AND 5
            OR skill_moves IS NULL),
        
            work_rate VARCHAR(30),
            body_type VARCHAR(50),

            player_tags TEXT,
            player_traits TEXT,

            pace INT CHECK (pace BETWEEN 1 AND 99
            OR pace IS NULL),
            shooting INT CHECK (shooting BETWEEN 1 AND 99
            OR shooting IS NULL),
            passing INT CHECK (passing BETWEEN 1 AND 99
            OR passing IS NULL),
            dribbling INT CHECK (dribbling BETWEEN 1 AND 99
            OR dribbling IS NULL),
            defending INT CHECK (defending BETWEEN 1 AND 99
            OR defending IS NULL),
            physic INT CHECK (physic BETWEEN 1 AND 99
            OR physic IS NULL),

            PRIMARY KEY (player_id, fifa_version)
            );    
        
        """

        sql3 = """
        
            CREATE TABLE players_aux_stats_legacy (

            
            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK (
            fifa_version BETWEEN 6 AND 30),

            short_name VARCHAR(100) NOT NULL,
            
            attacking_crossing INT CHECK (
            attacking_crossing BETWEEN 1 AND 99
            OR attacking_crossing IS NULL),
            
            attacking_finishing INT CHECK (
            attacking_finishing BETWEEN 1 AND 99
            OR attacking_finishing IS NULL),
            
            attacking_heading_accuracy INT CHECK (
            attacking_heading_accuracy BETWEEN 1 AND 99
            OR attacking_heading_accuracy IS NULL),

            attacking_short_passing INT CHECK (
            attacking_short_passing BETWEEN 1 AND 99
            OR attacking_short_passing IS NULL),

            attacking_volleys INT CHECK (
            attacking_volleys BETWEEN 1 AND 99
            OR attacking_volleys IS NULL),

            skill_dribbling INT CHECK (
            skill_dribbling BETWEEN 1 AND 99
            OR skill_dribbling IS NULL),

            skill_curve INT CHECK (
            skill_curve BETWEEN 1 AND 99
            OR skill_curve IS NULL),

            skill_fk_accuracy INT CHECK (
            skill_fk_accuracy BETWEEN 1 AND 99
            OR skill_fk_accuracy IS NULL),

            skill_long_passing INT CHECK (
            skill_long_passing BETWEEN 1 AND 99
            OR skill_long_passing IS NULL),

            skill_ball_control INT CHECK (
            skill_ball_control BETWEEN 1 AND 99
            OR skill_ball_control IS NULL),

            movement_acceleration INT CHECK (
            movement_acceleration BETWEEN 1 AND 99
            OR movement_acceleration IS NULL),

            movement_sprint_speed INT CHECK (
            movement_sprint_speed BETWEEN 1 AND 99
            OR movement_sprint_speed IS NULL),

            movement_agility INT CHECK (
            movement_agility BETWEEN 1 AND 99
            OR movement_agility IS NULL),

            movement_reactions INT CHECK (
            movement_reactions BETWEEN 1 AND 99
            OR movement_reactions IS NULL),

            movement_balance INT CHECK (
            movement_balance BETWEEN 1 AND 99
            OR movement_balance IS NULL),

            power_shot_power INT CHECK (
            power_shot_power BETWEEN 1 AND 99
            OR power_shot_power IS NULL),

            power_jumping INT CHECK (
            power_jumping BETWEEN 1 AND 99
            OR power_jumping IS NULL),

            power_stamina INT CHECK (
            power_stamina BETWEEN 1 AND 99
            OR power_stamina IS NULL),

            power_strength INT CHECK (
            power_strength BETWEEN 1 AND 99
            OR power_strength IS NULL),

            mentality_interceptions INT CHECK (
            mentality_interceptions BETWEEN 1 AND 99
            OR mentality_interceptions IS NULL),

            mentality_positioning INT CHECK (
            mentality_positioning BETWEEN 1 AND 99
            OR mentality_positioning IS NULL),

            mentality_vision INT CHECK (
            mentality_vision BETWEEN 1 AND 99
            OR mentality_vision IS NULL),

            mentality_penalties INT CHECK (
            mentality_penalties BETWEEN 1 AND 99
            OR mentality_penalties IS NULL),

            mentality_composure INT CHECK (
            mentality_composure BETWEEN 1 AND 99
            OR mentality_composure IS NULL),

            defending_marking_awareness INT CHECK (
            defending_marking_awareness BETWEEN 1 AND 99
            OR defending_marking_awareness IS NULL),

            defending_standing_tackle INT CHECK (
            defending_standing_tackle BETWEEN 1 AND 99
            OR defending_standing_tackle IS NULL),

            defending_sliding_tackle INT CHECK (
            defending_sliding_tackle BETWEEN 1 AND 99
            OR defending_sliding_tackle IS NULL),

            goalkeeping_diving INT CHECK (
            goalkeeping_diving BETWEEN 1 AND 99
            OR goalkeeping_diving IS NULL),

            goalkeeping_handling INT CHECK (
            goalkeeping_handling BETWEEN 1 AND 99
            OR goalkeeping_handling IS NULL),

            goalkeeping_kicking INT CHECK (
            goalkeeping_kicking BETWEEN 1 AND 99
            OR goalkeeping_kicking IS NULL),

            goalkeeping_positioning INT CHECK (
            goalkeeping_positioning BETWEEN 1 AND 99
            OR goalkeeping_positioning IS NULL),

            goalkeeping_reflexes INT CHECK (
            goalkeeping_reflexes BETWEEN 1 AND 99
            OR goalkeeping_reflexes IS NULL),

            goalkeeping_speed INT CHECK (
            goalkeeping_speed BETWEEN 1 AND 99
            OR goalkeeping_speed IS NULL),

            PRIMARY KEY (player_id, fifa_version)
            );

        """
        
        sql4 = """
            CREATE TABLE players_legacy_fifaver (

            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK (fifa_version BETWEEN 6 AND 30),
            fifa_update INT NOT NULL,
            fifa_update_date DATE,
            
            short_name VARCHAR(100) NOT NULL,
            overall INT CHECK (
            overall BETWEEN 1 AND 99
            OR overall IS NULL),

            potential INT CHECK (
            potential BETWEEN 1 AND 99
            OR potential IS NULL),

            body_type VARCHAR(50),
            real_face VARCHAR(10),
            
            PRIMARY KEY (player_id,fifa_version)    
            
            );

        """

        sql5 = """
            CREATE TABLE players_legacy_for_debug (
        
            player_id BIGINT NOT NULL CHECK (
            player_id > 0),

            fifa_version INT NOT NULL CHECK (
            fifa_version BETWEEN 6 AND 30),

            source_file VARCHAR(255) NOT NULL,

            chunk_no INT NOT NULL CHECK (
            chunk_no >= 0),

            PRIMARY KEY (player_id,fifa_version)
            
            );
            
            
            
        """

        with engine.begin() as conn:
            conn.execute(text("""DROP TABLE IF EXISTS players_national_legacy CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS players_main_stats_legacy CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS players_aux_stats_legacy CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS players_legacy_fifaver CASCADE;"""))
            conn.execute(text("""DROP TABLE IF EXISTS players_legacy_for_debug CASCADE;"""))
            conn.execute(text(sql1))
            conn.execute(text(sql2))
            conn.execute(text(sql3))
            conn.execute(text(sql4))
            conn.execute(text(sql5))
     
    @task
    def insert_into_players_national_legacy():
        engine = create_engine(DB_URL)

        sql = """

        INSERT INTO players_national_legacy(
            player_id,
            fifa_version,
            short_name,
            nationality_id,
            nationality_name,
            nation_team_id,
            nation_position,
            nation_jersey_number
        )
        SELECT
            player_id,
            fifa_version,
            TRIM(short_name),
            nationality_id,
            TRIM(nationality_name),
            nation_team_id,
            TRIM(nation_position),
            nation_jersey_number

        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30
        AND nationality_id IS NOT NULL
        AND nation_team_id IS NOT NULL;

            
                    

            """
        with engine.begin() as conn:
            conn.execute(text(sql))
     
    @task
    def insert_into_players_main_stats_legacy():
        engine = create_engine(DB_URL)

        sql = """

        INSERT INTO players_main_stats_legacy(
            player_id,
            fifa_version,
            short_name,
            preferred_foot,
            weak_foot,
            skill_moves,
            work_rate,
            body_type,
            player_tags,
            player_traits,
            pace,
            shooting,
            passing,
            dribbling,
            defending,
            physic
        )
        SELECT
            player_id,
            fifa_version,
            TRIM(short_name),
            TRIM(preferred_foot),
            weak_foot,
            skill_moves,
            TRIM(work_rate),
            TRIM(body_type),
            COALESCE('[' || REPLACE(player_tags,'#','') || ']', '[]') as player_tags,
            COALESCE('[' || player_traits || ']', '[]') as player_traits,
            pace,
            shooting,
            passing,
            dribbling,
            defending,
            physic
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30;
               
            """
        with engine.begin() as conn:
            conn.execute(text(sql))

    
    @task
    def insert_into_players_aux_stats_legacy():
        engine = create_engine(DB_URL)

        sql = """
        INSERT INTO players_aux_stats_legacy(
            player_id,
            fifa_version,
            short_name,

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
            goalkeeping_speed
        )

        SELECT
            player_id,
            fifa_version,
            TRIM(short_name),

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
            goalkeeping_speed

    FROM silver_players_legacy
    WHERE player_id IS NOT NULL
    AND fifa_version BETWEEN 6 AND 30;
                  
            """
        with engine.begin() as conn:
            conn.execute(text(sql))
    
    

    @task
    def insert_into_players_legacy_fifaver():
        engine = create_engine(DB_URL)

        sql = """

        INSERT INTO players_legacy_fifaver (
            player_id,
            fifa_version,
            fifa_update,
            fifa_update_date,
            short_name,
            overall,
            potential,
            body_type,
            real_face
        )
        SELECT
            player_id,
            fifa_version,
            fifa_update,
            TO_DATE(fifa_update_date,'YYYY-MM-DD') AS fifa_update_date,
            TRIM(short_name),
            overall,
            potential,
            TRIM(body_type),
            TRIM(real_face)
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30;
               
            """
        with engine.begin() as conn:
            conn.execute(text(sql))

    
    @task
    def insert_into_players_legacy_for_debug():
        engine = create_engine(DB_URL)

        sql = """

        INSERT INTO players_legacy_for_debug(
            player_id,
            fifa_version,
            source_file,
            chunk_no
        )
        SELECT
            player_id,
            fifa_version,
            TRIM(source_file),
            chunk_no
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30;
               
            """
        with engine.begin() as conn:
            conn.execute(text(sql))

    create_tables_2() >> [
    insert_into_players_national_legacy(),
    insert_into_players_main_stats_legacy(),
    insert_into_players_aux_stats_legacy(),
    insert_into_players_legacy_fifaver(),
    insert_into_players_legacy_for_debug()
    ]