import logging
import requests
import yaml
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class EicherCDRAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.objects = self.config["cdr"]["objects"]
        self.headers = {
            "Content-Type": "application/json"
        }
        self.wrangler = DataWrangler(secrets)

    def _get_date_ranges(self):
        backfill_cfg = self.config["cdr"].get("backfill", {})
        today = datetime.now().date()
        if backfill_cfg.get("enabled", False):
            start_date = datetime.strptime(backfill_cfg["start_date"], "%Y-%m-%d").date()
            end_date = today - timedelta(days=2)
            # end_date = datetime.strptime(backfill_cfg["end_date"], "%Y-%m-%d").date()
            date_ranges = []
            current = start_date
            while current <= end_date:
                chunk_end = min(current + timedelta(days=2), end_date)
                since = f"{current} 00:00:00"
                until = f"{chunk_end} 23:59:59"
                date_ranges.append((since, until))
                current = chunk_end + timedelta(days=1)
            logging.info(f"BACKFILL MODE ACTIVATED--→ {len(date_ranges)} chunks (3-day each) --> from DateRange of {date_ranges}.")
            return date_ranges
        crawl_date = today - timedelta(days=2)          #back 2 days to avoid missing of any Records
        start = f"{crawl_date} 00:00:00"
        end = f"{crawl_date} 23:59:59"
        # end = f"{crawl_date} 01:59:59"
        logging.info(f"📅Backfill Disabled🚫🚫-→Running DAILY incremental '{start}' to '{end}'.")
        return [(start, end)]

    def fetch_api(self, url: str, start, end) -> pd.DataFrame:
        payload = {
            "startDate": start,
            "endDate": end,
            "campaignName": "Lead_Management_Desk"
        }
        logging.info(f"Fetching CDR API for {payload['startDate']} ---> {payload['endDate']}")
        response = requests.get(
            url,
            headers=self.headers,
            json=payload,
            timeout=300
        )
        response.raise_for_status()
        result = response.json()
        if not result.get("status", False):
            logging.warning("API returned Status= False. SO NO DATA AVAILABLE.")
            return pd.DataFrame()
        records = result.get("data", [])
        if not records:
            logging.info("There is Records Params but NO DATA AVAILABLE ---> returned Back.")
            return pd.DataFrame()

        df = pd.json_normalize(records)
        logging.info(f"Fetched {len(df)} rows.")
        return df

    def _sleep(self, seconds: int = 10):
        logging.info(f"😴😴Sleeping for {seconds}s to respect API rate limits...")
        time.sleep(seconds)

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"🚫❌Object '{object_name}' is not active or missing in config🚫❌")
        return obj

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"Starting extraction for {object_name} under get_dataframe()")
        obj_cfg = self._get_obj_cfg(object_name)
        date_ranges = self._get_date_ranges()
        total_rows = 0
        url = obj_cfg["url"]
        api_call_count = 0
        final_dfs = []
        for start, end in date_ranges:
            api_call_count += 1
            if api_call_count % 10 == 0:
                self._sleep()
            print(f"Crawling data from date-range '{start}' ---> '{end}'")
            try:
                df = self.fetch_api(url, start, end)
                if df.empty:
                    continue
                final_dfs.append(df)
                total_rows += len(df)
                print(f"Running Total Rows fetched: {total_rows}")
            except Exception as ex:
                logging.exception(f"Failed for date-range '{start}' ---> '{end}' due to: {ex}")
        if final_dfs:
            final_df = pd.concat(final_dfs, ignore_index=True)
            self.wrangler.save_batch(final_df, object_name=object_name, api_name="cdr", primary_key=obj_cfg["primary_key"])
            logging.info(f"Completed CDR ingestion via save_batch.")
            return final_df
        logging.info("Completed CDR ingestion. No data fetched.")
        return pd.DataFrame()