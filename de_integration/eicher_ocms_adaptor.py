import logging
import pandas as pd
import yaml
from typing import Dict, Any
from google.cloud import bigquery

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class OCMSAdaptor:

    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as cp:
            self.config = yaml.safe_load(cp)
        self.objects = self.config["ocms"]["objects"]
        self.secrets = secrets
        self.wrangler = DataWrangler(secrets)

        self.project_id = secrets['PROJECT_ID']
        self.connection_id = secrets['CONNECTION_ID']
        self.bq_client = bigquery.Client(project=self.project_id)

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is not active or missing in config.")
        return obj

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        print(f"Starting extraction for {object_name} under get_dataframe()")
        obj_cfg = self._get_obj_cfg(object_name)

        src_table_name = obj_cfg["table"]
        if not src_table_name:
            raise ValueError(f"OCMS Missing 'table' name for object '{object_name}'")

        _extraction_query = f"""
        SELECT *
        FROM EXTERNAL_QUERY(
            '{self.project_id}.{self.connection_id}',
            'SELECT * FROM {src_table_name}'
        )
        """
        logging.info(f"Running query for object: {object_name} <<------ (table: {src_table_name})")

        _df = self.bq_client.query(_extraction_query).to_dataframe()
        logging.info(f"Retrieved {len(_df)} rows from {src_table_name}")

        if _df.empty:
            logging.warning(f"⚠️No data fetched for {object_name} from\n{_extraction_query}")
            return _df
        logging.info(f"✅ Extracted {len(_df)} rows for {object_name}.---> Loading to save_batch..")
        self.wrangler.save_batch(_df, object_name, "ocms", primary_key=obj_cfg["primary_key"])
        logging.info(f"Completed save_batch() of OCMS for {object_name}.")
        return pd.DataFrame()