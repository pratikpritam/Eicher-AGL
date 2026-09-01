import logging
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, "configs", "eicher_konnect_config.yaml")

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from de_integration.eicher_konnect_adaptor import KonnectAdaptor
print(f"All Imports done. Project root: {PROJECT_ROOT}")
SECRETS = Variable.get("konnect_secrets", deserialize_json=True)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "Prateek Pandey",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "eicher_konnect_insights_dag",
    default_args=default_args,
    schedule_interval="0 2 * * *",             #IST
    catchup=False,
    tags=["Eicher", "KONNECT"]
)

adaptor = KonnectAdaptor(CONFIG_PATH, SECRETS)
logging.info("Adaptor Initialized with config and secrets. Now creating tasks...")
with dag:
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("Pipeline Started")
    )

    extract_groups = PythonOperator(
        task_id="extract_groups",
        python_callable=lambda: adaptor.get_dataframe("groups")
    )
    logging.info("Created task for [groups] extraction.")

    child_tasks = []
    for obj in adaptor.objects:
        object_name = list(obj.keys())[0]
        if not obj[object_name]["active"] or object_name == "groups":
            print(f"Skipping inactive object: {object_name}.")
            continue

        task = PythonOperator(
            task_id=f"extract_{object_name}",
            python_callable=lambda object_name=object_name: adaptor.get_dataframe(object_name)
        )
        child_tasks.append(task)
        print(f"Created task for {object_name} extraction.")

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("Pipeline Completed.")
    )

    start >> extract_groups
    print("Set dependency: start -----> extract_groups")
    for child in child_tasks:
        print(f"Setting dependency: extract_groups -----> {child.task_id}")
        extract_groups >> child >> end
