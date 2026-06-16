**###FIFA23 Data Engineering Pipeline**



**##Overview**



This project demonstrates an end-to-end Data Engineering pipeline built using Apache Airflow, Apache Kafka, PostgreSQL, and Docker.



The pipeline ingests FIFA23 datasets, performs data cleaning and transformation, and loads curated datasets into a PostgreSQL Data Warehouse using a multi-layer architecture.



**##Pipeline Flow**



Read FIFA23 CSV files

Airflow triggers ingestion DAGs

Producer publishes records to Kafka topics

Consumer receives messages and loads data into PostgreSQL

Silver transformation layer cleans and validates data

Gold layer creates analytical tables



**##Project Structure**



dags/

scripts/

sql/

docker-compose.yml

requirements.txt

README.md



**##Tools:**



**Python**

**Apache Airflow**

**Apache Kafka**

**PostgreSQL**

**Docker**

**Pandas**

**SQL**



\##Description :


In this project, I decided to use Docker to ensure a portable development environment. All required services are in the docker-compose.yml file, allowing the project to run without compatibility issues across different machines. Docker is also required because Apache Airflow and PostgreSQL are deployed as containers and communicate within the same Docker network.



There are 4 csv files which I got from Kaggle.

raw\_male\_players.csv

raw\_male\_teams.csv

raw\_male\_coaches.csv

raw\_male\_players\_legacy.csv



The datasets are split into chunks and streamed through Apache Kafka before being loaded into PostgreSQL. Kafka Producer and Consumer scripts are located in airflow/scripts.



The data is then cleaned and transformed into a multi-layer data warehouse architecture consisting of Raw, Silver, and Gold schemas.



!\[Architecture](docs/sql\_schemas.png)



&#x20;All transformations are implemented as Apache Airflow DAGs and can be triggered manually through the Airflow UI.

!\[Airflow DAG Example](docs/airflow\_dag.png)



The latest pipeline versions are marked with the v2 suffix and include additional data integrity constraints such as Foreign Key relationships. Pipeline files are located in airflow/dags/pipelines/fifa23/.















