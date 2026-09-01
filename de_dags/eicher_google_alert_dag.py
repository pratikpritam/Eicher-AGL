from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import sys
import os
from datetime import datetime, timedelta
from airflow.models import Variable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, "configs", "eicher_google_alert_config.yaml")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from de_integration.eicher_google_alert_adaptor import GoogleAlertAdaptor

SECRETS = Variable.get("google_alert_secrets", deserialize_json=True)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "Prateek Pandey",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 11),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}
dag = DAG(
    "eicher_google_alert_dag",
    default_args=default_args,
    schedule_interval="0 */6 * * *",   # Every 6 hours      ="30 22 * * *",   #At 10:30 PM IST
    catchup=False,
    tags=["Google Alert", "News", "Web Scraping"],
)

adaptor = GoogleAlertAdaptor(CONFIG_PATH, SECRETS)
logging.info("GoogleAlertAdaptor Initialized with config and secrets. Now creating tasks...")

with dag:
    start_task = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("Starting Google Alert Extraction DAG..."),
    )
    extract_tasks = []
    for obj in adaptor.objects:
        object_name = list(obj.keys())[0]
        if not obj[object_name]["active"]:
            print(f"Skipping inactive object: {object_name}.")
            continue
        print(f"Processing active object: {object_name}.")
        task = PythonOperator(
            task_id=f"extract_{object_name}",
            python_callable=lambda object_name=object_name: adaptor.get_dataframe(object_name),
        )
        extract_tasks.append(task)
        start_task >> task
    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("Completed Google Alert DAG."),
    )
    for task in extract_tasks:
        task >> end_task