from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="fifa23_players_legacy_pipeline",
    schedule=None,
    start_date=datetime(2026, 6, 6),
    catchup=False,
    tags=["fifa23_legacy", "gold"],
) as dag:
       
    @task
    def create_tables():
        engine = create_engine(DB_URL)

        sql1 = """
     
        CREATE TABLE general_info_players_legacy (
                
                
            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK 
            ( fifa_version BETWEEN 6 AND 30 ),
            player_url VARCHAR(300),
            age BIGINT NOT NULL CHECK (age BETWEEN 15 AND 50),
            short_name VARCHAR(100) NOT NULL CHECK (LENGTH(short_name) >= 1),
            long_name VARCHAR(150),
            
            overall INT CHECK (overall BETWEEN 1 AND 99),
            potential INT CHECK (potential BETWEEN 1 AND 99),
            player_positions VARCHAR(50),
            preferred_foot VARCHAR(10) CHECK (preferred_foot IN ('Right', 'Left')),
            
            value_eur BIGINT CHECK (value_eur >= 0 AND value_eur <= 500000000),
            
            club_team_id BIGINT CHECK (club_team_id > 0),
            club_name VARCHAR(100),
            club_jersey_number INT CHECK (
            club_jersey_number BETWEEN 1 AND 99
            OR club_jersey_number IS NULL),
                
            nationality_id BIGINT CHECK (nationality_id > 0),
            nation_team_id BIGINT CHECK (nation_team_id > 0),
            nationality_name VARCHAR(100),
            nation_jersey_number INT CHECK (
            nation_jersey_number BETWEEN 1 AND 99
            OR nation_jersey_number IS NULL),
            
            player_face_url VARCHAR(500),
            
            PRIMARY KEY (player_id, fifa_version)
        );

        """
        
        sql2 = """       

        CREATE TABLE players_market_val_legacy ( 
            
            player_id BIGINT NOT NULL CHECK ( player_id > 0 ), 
            fifa_version INT NOT NULL CHECK ( fifa_version BETWEEN 6 AND 30 ), 
            short_name VARCHAR(100) NOT NULL, 
            
            overall INT CHECK ( overall BETWEEN 1 AND 99 ), 
            potential INT CHECK ( potential BETWEEN 1 AND 99 ), 
            age INT CHECK ( age BETWEEN 15 AND 50 ), 
            
            value_eur BIGINT CHECK ( value_eur >= 0 ),
            wage_eur BIGINT CHECK ( wage_eur >= 0 ), 
            club_team_id BiGINT,
            club_name TEXT,
            club_joined_date DATE,
            club_contract_valid_until_year INT CHECK 
            ( club_contract_valid_until_year BETWEEN 2000 AND 2100 OR 
            club_contract_valid_until_year IS NULL ), 
            
            league_name VARCHAR(100), 
            nationality_name VARCHAR(100), 
            
            international_reputation INT CHECK ( international_reputation BETWEEN 1 AND 5 OR
            international_reputation IS NULL ), 
            
            release_clause_eur BIGINT CHECK ( release_clause_eur >= 0 OR release_clause_eur IS NULL ),
            
            PRIMARY KEY (player_id, fifa_version) );
            
        """

        sql3 = """    
        
        CREATE TABLE players_physics_legacy (player_id BIGINT NOT NULL CHECK (
        
            player_id > 0),
            fifa_version INT NOT NULL CHECK (fifa_version BETWEEN 6 AND 30),
            short_name VARCHAR(100) NOT NULL,
            
            age INT CHECK (age BETWEEN 15 AND 50),
            
            height_cm INT CHECK (height_cm BETWEEN 140 AND 230 OR height_cm IS NULL),
            weight_kg INT CHECK (weight_kg BETWEEN 40 AND 130 OR weight_kg IS NULL),

            preferred_foot VARCHAR(10) CHECK (preferred_foot IN ('Left', 'Right')
            OR preferred_foot IS NULL
            ),

            PRIMARY KEY (player_id, fifa_version));
        """

        sql4 = """       
        CREATE TABLE players_club_legacy (
            
            player_id BIGINT NOT NULL CHECK (player_id > 0),
            fifa_version INT NOT NULL CHECK (fifa_version BETWEEN 6 AND 30),
            short_name VARCHAR(100) NOT NULL,

            club_team_id BIGINT CHECK (club_team_id > 0
            OR club_team_id IS NULL),
            club_name VARCHAR(100),
            club_position VARCHAR(20),
            club_jersey_number INT CHECK (club_jersey_number BETWEEN 1 AND 99
            OR club_jersey_number IS NULL),
            club_loaned_from VARCHAR(100),
            club_joined_date DATE,

            club_contract_valid_until_year INT CHECK (
            club_contract_valid_until_year BETWEEN 2000 AND 2100
            OR club_contract_valid_until_year IS NULL),

            league_id BIGINT CHECK (league_id > 0
            OR league_id IS NULL),
            league_name VARCHAR(100),
            league_level INT CHECK (league_level BETWEEN 1 AND 10
            OR league_level IS NULL),

            PRIMARY KEY (player_id, fifa_version)
        );    
                
            """
        with engine.begin() as conn:
            conn.execute(text("""DROP TABLE IF EXISTS general_info_players_legacy CASCADE;""" ))
            conn.execute(text("""DROP TABLE IF EXISTS players_market_val_legacy CASCADE;""" ))
            conn.execute(text("""DROP TABLE IF EXISTS players_physics_legacy CASCADE;""" ))
            conn.execute(text("""DROP TABLE IF EXISTS players_club_legacy CASCADE;""" ))
            conn.execute(text(sql1))
            conn.execute(text(sql2))
            conn.execute(text(sql3))
            conn.execute(text(sql4))

    @task
    def insert_into_general_legacy():
        engine = create_engine(DB_URL)

        sql = """
        INSERT INTO general_info_players_legacy (
            player_id,
            fifa_version,
            player_url,
            age,
            short_name,
            long_name,
            overall,
            potential,
            player_positions,
            preferred_foot,
            value_eur,
            club_team_id,
            club_name,
            club_jersey_number,
            nationality_id,
            nation_team_id,
            nationality_name,
            nation_jersey_number,
            player_face_url
        )
        SELECT
            player_id,
            fifa_version,
            player_url,
            age,
            short_name,
            long_name,
            overall,
            potential,
            player_positions,
            preferred_foot,
            value_eur,
            club_team_id,
            club_name,
            club_jersey_number,
            nationality_id,
            nation_team_id,
            TRIM(nationality_name),
            nation_jersey_number,
            player_face_url
        FROM silver_players_legacy;
        """

        with engine.begin() as conn:
            conn.execute(text(sql))

    @task
    def insert_players_market_val_legacy():
        engine = create_engine(DB_URL)

        sql = """         
        INSERT INTO players_market_val_legacy(
            player_id,
            fifa_version,
            short_name,
            overall,
            potential,
            age,
            value_eur,
            wage_eur,
            club_team_id,
            club_name,
            club_joined_date,
            club_contract_valid_until_year,
            league_name,
            nationality_name,
            international_reputation,
            release_clause_eur
        )
        SELECT 
            player_id,
            fifa_version,
            TRIM(short_name),
            overall,
            potential,
            age,
            value_eur,
            wage_eur,
            club_team_id,
            club_name,
            TO_DATE(club_joined_date, 'YYYY-MM-DD'),
            club_contract_valid_until_year,
            TRIM(league_name),
            TRIM(nationality_name),
            international_reputation,
            release_clause_eur
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30
        AND overall BETWEEN 1 AND 99
        AND potential BETWEEN 1 AND 99
        AND age BETWEEN 15 AND 50;

            """
        with engine.begin() as conn:
            conn.execute(text(sql))
            
    @task
    def insert_players_physics_legacy():
        engine = create_engine(DB_URL)

        sql = """ 
       INSERT INTO players_physics_legacy (
            player_id,
            fifa_version,
            short_name,
            age,
            height_cm,
            weight_kg,
            preferred_foot
        )
        SELECT
            player_id,
            fifa_version,
            TRIM(short_name),
            age,
            height_cm,
            weight_kg,
            TRIM(preferred_foot)
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30
        AND age BETWEEN 15 AND 50;
              
              """
        with engine.begin() as conn:
            conn.execute(text(sql))

    @task
    def insert_players_club_legacy():
        engine = create_engine(DB_URL)

        sql = """
        INSERT INTO players_club_legacy (
            player_id,
            fifa_version,
            short_name,
            club_team_id,
            club_name,
            club_position,
            club_jersey_number,
            club_loaned_from,
            club_joined_date,
            club_contract_valid_until_year,
            league_id,
            league_name,
            league_level
        )
        SELECT
            player_id,
            fifa_version,
            TRIM(short_name),
            club_team_id,
            TRIM(club_name),
            TRIM(club_position),
            club_jersey_number,
            TRIM(club_loaned_from),
            TO_DATE(club_joined_date, 'YYYY-MM-DD'),
            club_contract_valid_until_year,
            league_id,
            TRIM(league_name),
            league_level
        FROM silver_players_legacy
        WHERE player_id IS NOT NULL
        AND fifa_version BETWEEN 6 AND 30
        AND club_team_id IS NOT NULL
        AND club_name IS NOT NULL;

             """
        with engine.begin() as conn:
            conn.execute(text(sql))

    create_tables() >>  [
    insert_into_general_legacy(),
    insert_players_market_val_legacy(),
    insert_players_physics_legacy(),
    insert_players_club_legacy()
    ]