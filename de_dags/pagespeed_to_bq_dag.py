import yaml
import logging
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, "configs", "eicher_pagespeed_config.yaml")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
from de_integration.pagespeed_to_bigquery_adaptor import PageSpeedAdaptor
print(f"All Imports done. Project root: {PROJECT_ROOT}")

SECRETS = Variable.get("pagespeed_secrets", deserialize_json=True)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "Prateek Pandey",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}
dag = DAG(
    "eicher_pagespeed_dag",
    default_args=default_args,
    schedule_interval="30 2 * * *",             #IST
    catchup=False,
    tags=["Eicher", "PageSpeed", "DataIngestion"]
)

adaptor = PageSpeedAdaptor(CONFIG_PATH, SECRETS)
print("Adaptor Initialized with config and secrets. Now creating tasks...")
with dag:
    start_task = PythonOperator(
        task_id="start",
        python_callable=lambda: logging.info("Starting PageSpeed DAG..."),
    )
    extract_pagespeed = PythonOperator(
        task_id="extract_pagespeed",
        python_callable=lambda: adaptor.get_dataframe("pagespeed")
    )
    print("Created task for pagespeed extraction.")
    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: logging.info("Completed PageSpeed DAG."),
    )

    start_task >> extract_pagespeed >> end_task
