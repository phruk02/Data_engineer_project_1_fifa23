from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ฟังก์ชันที่ task จะรัน
def hello_world():
    print("Hello from Airflow!")

def second_task():
    print("This is task 2")


with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )

    task2 = PythonOperator(
        task_id="task_2",
        python_callable=second_task
    )


    task1 >> task2