import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
import requests
import yaml
from requests.auth import HTTPBasicAuth
from de_integration.data_wrangler import DataWrangler

logging.basicConfig(level=logging.INFO)

class EicherSAPAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.sap_cfg = self.config["sap"]
        self.objects = self.sap_cfg["objects"]
        self.username = secrets["SAP_USERNAME"]
        self.password = secrets["SAP_PASSWORD"]

        self.timeout = self.sap_cfg.get("timeout_seconds", 120)
        self.retry_attempts = self.sap_cfg.get("retry_attempts", 3)
        # self.sleep_after_every_n_calls = self.sap_cfg.get("sleep_after_every_n_calls",5)
        self.sleep_duration_seconds = self.sap_cfg.get("sleep_duration_seconds",10)
        self.window_days = self.sap_cfg.get("window_days", 10)
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({"Accept": "application/json"})

        self.wrangler = DataWrangler(secrets)

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj:
            raise ValueError(f"Object '{object_name}' not found in config.")
        if not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive.")
        return obj

    def _build_payloads(self, date_mode: str) -> List[Dict]:
        backfill_cfg = self.sap_cfg.get("backfill", {})
        today = datetime.now().date()
        if backfill_cfg.get("enabled", False):
            start_date = datetime.strptime(backfill_cfg["start_date"], "%Y-%m-%d").date()
            # end_date = datetime.strptime(backfill_cfg["end_date"], "%Y-%m-%d").date()
            end_date = today - timedelta(days=10)
            logging.info(f"🔁 BACKFILL ACTIVE ---→ '{start_date}' to '{end_date}'")
        else:
            start_date = today - timedelta(days=10)
            end_date = start_date
            logging.info(f"📅 Backfill Disabled🚫🚫-→Running DAILY Incremental for-→ {start_date}")

        if date_mode == "datetime":
            return self._generate_datetime_ranges(start_date, end_date)
        elif date_mode == "datetime_millis":
            return self._generate_datetime_millis_ranges(start_date, end_date)
        elif date_mode == "month":
            return self._generate_month_ranges(start_date, end_date)
        elif date_mode == "financial_period":
            return self._generate_financial_period_ranges(start_date, end_date)
        raise ValueError(f"Unsupported date_mode: {date_mode}")

    def _generate_datetime_ranges(self, start_date, end_date) -> List[Dict]:
        payloads = []
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=self.window_days - 1), end_date)
            payloads.append({
                "start": f"{current}T00:00:00",
                "end": f"{chunk_end}T23:59:59"
            })
            current = chunk_end + timedelta(days=1)
        return payloads

    def _generate_datetime_millis_ranges(self,start_date,end_date) -> List[Dict]:
        payloads = []
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=self.window_days - 1),end_date)
            payloads.append({
                "start": f"{current}T00:00:00.000",
                "end": f"{chunk_end}T23:59:59.000"
            })
            current = chunk_end + timedelta(days=1)
        return payloads

    def _generate_month_ranges(self,start_date,end_date) -> List[Dict]:
        payloads = []
        current = start_date.replace(day=1)
        while current <= end_date:
            month_val = current.strftime("%m.%Y")
            payloads.append({
                "month": month_val,
                "month_to": month_val
            })
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return payloads

    def _generate_financial_period_ranges(self,start_date,end_date) -> List[Dict]:
        payloads = []
        current = start_date.replace(day=1)
        while current <= end_date:
            fiscal_period = self._get_fiscal_period(current)
            payloads.append({
                "fiscal_period": fiscal_period,
                "fiscal_period_to": fiscal_period
            })
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return payloads

    def _get_fiscal_period(self, dt) -> str:
        fiscal_map = {
            4:"001", 5:"002", 6:"003", 7:"004", 8:"005",
            9:"006", 10:"007", 11:"008", 12:"009",
            1:"010", 2:"011", 3:"012"
        }
        period = fiscal_map[dt.month]
        fiscal_year = dt.year
        if dt.month in [1, 2, 3]:
            fiscal_year -= 1
        return f"{period}.{fiscal_year}"

    def _call_api(self, url: str) -> pd.DataFrame:
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self.session.get(
                    url,
                    timeout=self.timeout
                )
                response.raise_for_status()
                data = response.json()
                records = (data.get("d") or {}).get("results", [])
                df = pd.DataFrame(records, dtype=str)
                if not df.empty:
                    df.columns = [col.lower() for col in df.columns]
                logging.info(f"✅Records fetched from API: {len(df)}")
                return df
            except requests.exceptions.RequestException as e:
                logging.warning(f"⚠️ Attempt {attempt}/{self.retry_attempts} failed ---> Due to \n{e}")
                time.sleep(45)
        raise Exception(f"❌ SAP API failed after [{self.retry_attempts}] retries.")

    def _sleep(self):
        logging.info(f"😴 Sleeping for {self.sleep_duration_seconds}s to respect SAP limits...")
        time.sleep(self.sleep_duration_seconds)

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"🚀 Starting extraction for object: {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        date_mode = obj_cfg.get("date_mode", "datetime")
        payloads = self._build_payloads(date_mode)
        logging.info(f"Payload Bifurcations --> Total [{len(payloads)}] Chunks to Crawl. Payloads as '{payloads}'")
        base_url = obj_cfg["base_url"]
        api_call_count = 0
        dfs = []
        for payload in payloads:
            api_call_count += 1
            if api_call_count % 5 == 0:
                self._sleep()
            logging.info(f"Crawling Data in range of '{payload}'.")
            url = base_url.format(**payload)
            logging.info(f"🔹API CALL ##{api_call_count} for URL: '{url}'")
            df = self._call_api(url)
            if not df.empty:
                dfs.append(df)
            else:
                print("No Record Found for given Date Range")
        if not dfs:
            logging.warning(f"⚠️ No data fetched for object: {object_name}")
            return pd.DataFrame()
        final_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"✅ Total Records Fetched: {len(final_df)}")
        self.wrangler.save_batch(final_df, object_name, "sap", primary_key=obj_cfg["primary_key"])
        logging.info(f"✅ save_batch completed for object: {object_name}")
        return pd.DataFrame()