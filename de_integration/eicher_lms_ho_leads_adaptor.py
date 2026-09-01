import requests
import yaml
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class LMSHOLeadsAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.base_url = self.config["lms_ho_leads"]["base_url"]
        self.headers = {
            "Authorization": secrets["API_AUTH_TOKEN"],
            "Content-Type": "application/json"
        }
        self.objects = self.config["lms_ho_leads"]["objects"]
        self.wrangler = DataWrangler(secrets)

    @staticmethod
    def _to_snake_case(col: str) -> str:
        return col.strip().lower()

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive or missing in config")
        logging.info(f"Object {object_name} found and fetched relevant data from Config.")
        return obj

    def _generate_date_range(self):
        backfill_cfg = self.config["lms_ho_leads"].get("backfill", {})
        if backfill_cfg.get("enabled", False):
            start_date = backfill_cfg.get("start_date")
            if not start_date:
                raise ValueError("Backfill enabled but start_date not provided")
            if backfill_cfg.get("end_date"):
                end_date = backfill_cfg["end_date"]
            else:
                end_date = datetime.now().date().strftime("%Y-%m-%d")
            logging.info(f"🔁 Running BACKFILL from {start_date} → {end_date}")
            return start_date, end_date
        # Default daily incremental
        today = datetime.now().date()
        yesterday = today - timedelta(days=3)       #back 3 days to avoid missing of any Records
        logging.info(f"📅 Running DAILY incremental from : {yesterday} → {today}")
        return (yesterday.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))

    def _fetch_api_data(self) -> Dict[str, Any]:
        start_date, end_date = self._generate_date_range()
        logging.info(f"For API Call Payload 'start_date' is :{start_date} and 'end_date' would be: {end_date}")
        payload = {
            "start_request_date": start_date,
            "end_request_date": end_date
        }
        response = requests.post(
            self.base_url,
            headers=self.headers,
            json=payload,
            timeout=60
        )
        if response.status_code != 200:
            raise Exception(f"API Failed: {response.text}")
        data = response.json()
        if not data.get("status"):
            raise Exception("API returned status=false")
        logging.info("fetched Data from API and now will bifuracte records based on Mapping")
        return data

    def _normalize(self, records: List[Dict]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df.columns = [self._to_snake_case(c) for c in df.columns]
        logging.info("Records converted to DF along with Standardizations of Columns.")
        return df

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"Starting extraction for '{object_name}'")
        obj_cfg = self._get_obj_cfg(object_name)
        api_data = self._fetch_api_data()
        api_key = obj_cfg["api_key"]
        if not api_key:
            raise ValueError(f"Unsupported object_name: {object_name}")
        records = api_data.get(api_key, [])
        df = self._normalize(records)
        if df.empty:
            logging.info(f"No records found for {object_name}")
            return df
        # df = df.astype(str)
        self.wrangler.save_batch(df, object_name, "LMS_HO", primary_key=obj_cfg["primary_key"])
        logging.info(f"Completed save_batch for {object_name}")
        return pd.DataFrame()