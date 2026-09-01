import re
import logging
import yaml
import pandas as pd
from io import BytesIO
from datetime import datetime
from typing import Dict, Any, List
from google.cloud import storage
from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class CleverTapAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.objects = self.config["clevertap"]["objects"]
        self.gcs_cfg = self.config["clevertap"]["gcs"]
        self.bucket_name = secrets['GCS_BUCKET']
        self.prefix = self.gcs_cfg["prefix"]
        self.skip_leading_rows = self.gcs_cfg.get("skip_leading_rows", 1)
        self.load_strategy = self.gcs_cfg.get("load_strategy")

        self.storage_client = storage.Client()
        self.wrangler = DataWrangler(secrets)

    @staticmethod
    def _to_snake_case(col: str) -> str:
        col = col.strip()
        col = col.replace("%", " pct ")
        col = col.replace("&", " and ")
        col = col.replace("/", "_")
        col = col.replace("-", "_")
        col = col.replace("(", "")
        col = col.replace(")", "")
        col = col.replace(":", "")
        col = col.replace(".", "")
        col = col.replace("'", "")
        col = col.replace("’", "")
        col = re.sub(r"[^\w]+", "_", col)
        col = re.sub(r"_+", "_", col)
        return col.lower().strip("_")
    
    @staticmethod
    def _make_unique_columns(columns: List[str]) -> List[str]:
        counts = {}
        output = []
        for col in columns:
            if col not in counts:
                counts[col] = 0
                output.append(col)
            else:
                counts[col] += 1
                output.append(f"{col}_{counts[col]+1}")
        return output
    
    def _list_report_files(self):
        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = list(bucket.list_blobs(prefix=self.prefix))
        blobs = [
            blob for blob in blobs
            if blob.name.endswith(".csv")
            and "campaign_report_datalake_eicher_" in blob.name
        ]
        if not blobs:
            logging.warning("No Campaign Report files found in GCS.")
            return []
        blobs = sorted(blobs, key=lambda x: x.updated)
        logging.info(f"Found {len(blobs)} campaign report files.")
        return blobs
    
    def _read_blob_to_dataframe(self, blob):
        logging.info(f"Reading : gs://{self.bucket_name}/{blob.name}")
        with blob.open("rb") as f:
            df = pd.read_csv(
                f,
                sep=",",
                quotechar='"',
                encoding="utf-8",
                engine="python",
                dtype=str,
                keep_default_na=False
            )
        df.columns = [self._to_snake_case(c) for c in df.columns]
        df.columns = self._make_unique_columns(df.columns.tolist())
        df["source_file_name"] = blob.name.split("/")[-1]
        df["file_updated_time"] = blob.updated
        # df["ingestion_timestamp"] = datetime.utcnow()
        logging.info(f"Rows Read : {len(df)}")
        return df
    
    def extract_campaign_reports(self):
        blobs = self._list_report_files()
        if not blobs:
            return pd.DataFrame()
        # if self.load_strategy == "latest":
        #     blobs = [blobs[-1]]
        dfs = []
        for blob in blobs:
            try:
                df = self._read_blob_to_dataframe(blob)
                dfs.append(df)
            except Exception as ex:
                logging.exception(f"Failed reading {blob.name}. Error : {ex}")
        if not dfs:
            return pd.DataFrame()
        final_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"Total Records : {len(final_df)}")
        return final_df

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive or missing in config")
        return obj

    def get_dataframe(self, object_name):
        logging.info(f"Starting extraction for : {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        if object_name == "campaign_reports_gcs_raw":
            report_df = self.extract_campaign_reports()
            if report_df.empty:
                logging.info("No Campaign Report Found.")
                return report_df
            logging.info(f"Extracted {len(report_df)} rows.")
            self.wrangler.save_batch(report_df, object_name, "clevertap", primary_key=obj_cfg["primary_key"])
            logging.info(f"Completed save_batch for {object_name}")
            return pd.DataFrame()
    
        raise ValueError(f"Unsupported object : {object_name}")