import logging
import requests
import pandas as pd
import yaml
from typing import Dict, Any

from de_integration.data_wrangler import DataWrangler

logging.basicConfig(level=logging.INFO)


class CMSAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            full_cfg = yaml.safe_load(f)

        self.config = full_cfg["cms"]
        self.objects = self.config["objects"]
        self.secrets = secrets
        self.wrangler = DataWrangler(secrets)
        self.headers = {
            "Cookie": secrets.get("PHPSESSID", "")
        }
        self.st_url = self.config["st_url"]

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is 'NOT ACTIVE' or missing in config")
        return obj

    def _fetch_data(self, url: str, object_name: str, obj_cfg: Dict[str, Any]) -> pd.DataFrame:
        logging.info(f"[CMS] Fetching data for {object_name} from {url}")

        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if object_name == "dealer_raw":
            key = obj_cfg.get("response_key", "dealer")
            records = data.get(key, [])
            return pd.DataFrame(records)
        if "states" in data:
            states = data.get("states", [])
            return states
        if "cities" in data:
            return pd.DataFrame({"cities": data.get("cities", [])})
        return pd.DataFrame(data)

    def extract_address_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if "address" not in df.columns.str.lower():
            print("No Address Columns Found Here.")
            return df
        df["address_text"] = (
            df["address"]
            .str.replace(r"<[^>]+>", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        df["full_address"] = df["address_text"].str.extract(
            r'Address\s+(.*)', expand=False
        )
        df["pin"] = df["full_address"].str.extract(
            r'(\b\d{6}\b)', expand=False
        )

        df["state"] = df["full_address"].str.extract(
            r',\s*([A-Za-z ]+)-?\s*\d{6}$', expand=False
        )
        df["city"] = df["full_address"].str.extract(
            r',\s*([^,]+),\s*[A-Za-z ]+-?\s*\d{6}$', expand=False
        )
        for col in ["full_address", "city", "state", "pin"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        return df

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"[CMS] Starting extraction for {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)

        if object_name == "dealer_raw":
            df = self._fetch_data(
                obj_cfg["url"],
                object_name,
                obj_cfg
            )
            df = self.extract_address_fields(df)
            logging.info(f"[CMS] Totally {len(df)} records fetched for {object_name}")
        elif object_name == "cities_raw":
            states = self._fetch_data(
                self.st_url,
                "states_raw",
                obj_cfg
            )
            print(f"Totally {len(states)} States fetched from CMS")
            if not states:
                logging.warning("[CMS] No states returned. Skipping cities extraction.")
                return pd.DataFrame()
            city_rows = []
            for state in states:
                city_url = f"{obj_cfg['url']}{state}"
                city_df = self._fetch_data(
                    city_url,
                    object_name,
                    obj_cfg
                )
                print(f'Totally {len(city_df)} city fetched for the [{state}].')
                if city_df.empty:
                    continue
                for city in city_df["cities"].dropna().tolist():
                    city_rows.append({
                        "states": state,
                        "cities": city
                    })
            df = pd.DataFrame(city_rows)
        else:
            raise ValueError(f"Unsupported CMS object: {object_name}")
        if df.empty:
            logging.warning(f"[CMS] No data fetched for {object_name}")
            return df
        logging.info(f"Total {len(df)} records fetched from CMS-API & sent to Wrangler")

        self.wrangler.save_batch(
            df=df,
            object_name=object_name,
            api_name="cms",
            primary_key=obj_cfg.get("primary_key")
        )
        logging.info(f"[CMS] Completed save batch ingestion for {object_name}")
        # return pd.DataFrame()