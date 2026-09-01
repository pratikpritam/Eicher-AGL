import logging
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, "configs", "eicher_veetrack_config.yaml")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from de_integration.eicher_veetrack_adaptor import VeeTrackAdaptor

SECRETS = Variable.get("veetrack_secrets", deserialize_json=True)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "Prateek Pandey",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}
dag = DAG(
    "eicher_veetrack_dag",
    default_args=default_args,
    schedule_interval="0 20 * * *",             #IST
    catchup=False,
    tags=["Eicher", "VeeTrack", "Gmail", "DataIngestion"]
)

adaptor = VeeTrackAdaptor(CONFIG_PATH, SECRETS)
print("Adaptor Initialized with config and secrets. Now creating tasks...")
with dag:
    start_task = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("Starting VeeTrack DAG..."),
    )
    for obj in adaptor.objects:
        object_name = list(obj.keys())[0]
        if obj[object_name]["active"]:
            print(f"Processing active object: {object_name}.")
            _extract = PythonOperator(
                task_id=f"extract_{object_name}",
                python_callable=lambda: adaptor.get_dataframe(object_name)
            )
        else:
            print(f"Skipping inactive object: {object_name}.")
            continue

    logging.info(f"Created task for {object_name} extraction.")
    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("Completed VeeTrack DAG."),
    )

    start_task >> _extract >> end_task
