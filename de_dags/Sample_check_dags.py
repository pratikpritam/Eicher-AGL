from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("to Check dag integration with kconnect")

with DAG(
    dag_id="samplewa_dag",
    start_date=datetime(2025, 11, 21),
    schedule="@daily",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )
