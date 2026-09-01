import re
import json
import yaml
import logging
import feedparser
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
from urllib.parse import urlparse
from de_integration.data_wrangler import DataWrangler

logging.basicConfig(level=logging.INFO)

class GoogleAlertAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.objects = self.config["google"]["objects"]
        self.alert_urls = self.config["google"]["rss_urls"]
        self.wrangler = DataWrangler(secrets)

    @staticmethod
    def _to_snake_case(col: str) -> str:
        col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
        col = re.sub(r"[^\w]+", "_", col)
        return col.lower().strip("_")

    def _normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [self._to_snake_case(c) for c in df.columns]
        return df.fillna("")

    def _parse_struct_time(self, time_struct):
        if not time_struct:
            return None
        return datetime(
            time_struct.tm_year,
            time_struct.tm_mon,
            time_struct.tm_mday,
            time_struct.tm_hour,
            time_struct.tm_min,
            time_struct.tm_sec,
        )

    def _extract_domain(self, url: str):
        if not url:
            return None
        try:
            return urlparse(url).netloc
        except Exception:
            return None

    def _extract_keyword(self, feed_title: str):
        if not feed_title:
            return None
        return feed_title.replace("Google Alert -", "").strip()

    def _extract_alerts_raw(self) -> pd.DataFrame:
        logging.info(f"🔹 Extracting Google Alerts RSS feeds on Today_UTC: {datetime.today()}")
        rows = []
        for rss_url in self.alert_urls:
            logging.info(f"Fetching RSS URL: {rss_url}")
            feed = feedparser.parse(rss_url)
            feed_meta = feed.get("feed", {})
            headers = feed.get("headers", {})
            feed_id = feed_meta.get("id")
            feed_title = feed_meta.get("title")
            feed_link = feed_meta.get("link")
            feed_updated = self._parse_struct_time(
                feed_meta.get("updated_parsed")
            )
            logging.info(f"Processing feed: {feed_title} with {len(feed.entries)} entries")
            for entry in feed.entries:
                row = {
                    "feed_id": feed_id,
                    "feed_title": feed_title,
                    "feed_link": feed_link,
                    "feed_updated_at": feed_updated,
                    "alert_keyword": self._extract_keyword(feed_title),
                    "entry_id": entry.get("id"),
                    "guid_is_link": entry.get("guidislink"),
                    "link": entry.get("link"),
                    "title": entry.get("title"),
                    "published": entry.get("published"),
                    "updated": entry.get("updated"),
                    "summary": entry.get("summary"),
                    "author": entry.get("author"),
                    "content_html": (
                        entry.get("content")[0].get("value")
                        if isinstance(entry.get("content"), list)
                        and len(entry.get("content")) > 0
                        else None
                    ),
                    "source_domain": self._extract_domain(entry.get("link")),
                    "feed_href": feed.get("href"),
                    "feed_encoding": feed.get("encoding"),
                    "feed_version": feed.get("version"),
                    "feed_headers_json": json.dumps(headers),
                    "source_system": "google_alert",
                    "raw_payload": json.dumps(entry, default=str)
                }
                rows.append(row)
            logging.info(f"All Records Appended for URL: {rss_url}")
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        return self._normalize_df(df)

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"🚫❌Object '{object_name}' is not active or missing in config🚫❌")
        return obj

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"🚀 Starting extraction for: {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        if object_name == "alert_raw":
            df = self._extract_alerts_raw()
            if df.empty:
                logging.warning(f"No Google Alerts records found  on Today_UTC: {datetime.today()}.")
                return df
            self.wrangler.save_batch(df, object_name, primary_key=obj_cfg["primary_key"], api_name="google")
            logging.info(f"✅ Completed save_batch for: {object_name}.")
            return pd.DataFrame()
        raise ValueError(f"Unsupported object_name: {object_name}")
