import yaml
import requests
import pandas as pd
from typing import Dict, Any
import logging
from de_integration.data_wrangler import DataWrangler


class PageSpeedAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.objects = self.config["pagespeed_insight"]["objects"]
        self.secrets = secrets
        self.wrangler = DataWrangler(secrets)
        self.api_key = secrets.get("PAGESPEED_API_KEY")

    def get_label(self, score: float, metric: str) -> str:
        """Assign performance labels based on metric thresholds."""
        thresholds = {
            "FCP": [(1.8, "Good"), (3.0, "Needs Improvement"), (float("inf"), "Poor")],
            "LCP": [(2.5, "Good"), (4.0, "Needs Improvement"), (float("inf"), "Poor")],
            "TBT": [(200, "Good"), (600, "Needs Improvement"), (float("inf"), "Poor")],
        }
        for limit, label in thresholds.get(metric, []):
            if score <= limit:
                return label
        return "Unknown"

    def extract_pagespeed_data(self, target_url: str, api_url: str) -> Dict[str, Any]:
        params = {
            "url": target_url,
            "category": ["PERFORMANCE", "ACCESSIBILITY", "BEST_PRACTICES", "SEO"],
            "key": self.api_key,
            "strategy": "mobile"
        }

        response = requests.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()["lighthouseResult"]

        get_audit = lambda key: data["audits"].get(key, {})
        to_clean = lambda val: val.replace("\xa0", " ") if isinstance(val, str) else val

        result = {
            "url": target_url,
            "performance_score": data["categories"]["performance"]["score"] * 100,
            "accessibility_score": data["categories"]["accessibility"]["score"] * 100,
            "best_practices_score": data["categories"]["best-practices"]["score"] * 100,
            "seo_score": data["categories"]["seo"]["score"] * 100,

            "fcp_score": get_audit("first-contentful-paint").get("score", 0) * 100,
            "fcp_display_value": to_clean(get_audit("first-contentful-paint").get("displayValue", "")),
            "fcp_numeric_value": get_audit("first-contentful-paint").get("numericValue", 0),

            "lcp_score": get_audit("largest-contentful-paint").get("score", 0) * 100,
            "lcp_display_value": to_clean(get_audit("largest-contentful-paint").get("displayValue", "")),
            "lcp_numeric_value": get_audit("largest-contentful-paint").get("numericValue", 0),

            "speed_index": get_audit("speed-index").get("score", 0) * 100,
            "si_display_value": to_clean(get_audit("speed-index").get("displayValue", "")),
            "si_numeric_value": get_audit("speed-index").get("numericValue", 0),

            "total_blocking_time": get_audit("total-blocking-time").get("score", 0) * 100,
            "tbt_display_value": to_clean(get_audit("total-blocking-time").get("displayValue", "")),
            "tbt_numeric_value": get_audit("total-blocking-time").get("numericValue", 0),

            "interactive": get_audit("interactive").get("score", 0) * 100,
            "interactive_display_value": to_clean(get_audit("interactive").get("displayValue", "")),
            "interactive_numeric_value": get_audit("interactive").get("numericValue", 0),

            "fcp_performance_label": self.get_label(self._safe_float(get_audit("first-contentful-paint").get("displayValue")), "FCP"),
            "lcp_performance_label": self.get_label(self._safe_float(get_audit("largest-contentful-paint").get("displayValue")), "LCP"),
            "tbt_performance_label": self.get_label(self._safe_float(get_audit("total-blocking-time").get("displayValue")), "TBT")
        }
        return result

    def _safe_float(self, value: str) -> float:
        try:
            if not value:
                return 0.0
            return float(value.split()[0])
        except Exception:
            return 0.0
    
    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is not active or missing in config.")
        return obj

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        print(f"Starting extraction for {object_name} under get_dataframe()")
        obj_cfg = self._get_obj_cfg(object_name)
        try:
            all_data = []
            for target_url in obj_cfg["target_urls"]:
                print(f"Running for the URL as : {target_url}")
                try:
                    result = self.extract_pagespeed_data(target_url, obj_cfg["api_url"])
                    all_data.append(result)
                    logging.info(f"[PageSpeed] Successfully fetched data for: {target_url}")
                except Exception as e:
                    logging.error(f"[PageSpeed] Error fetching data for {target_url}: {e}")

            if not all_data:
                logging.warning("[PageSpeed] No data fetched.")
                return pd.DataFrame()

            _df = pd.DataFrame(all_data)
            logging.info(f"[PageSpeed] Extracted {len(_df)} records")
            
            self.wrangler.save_batch(_df, object_name, "pagespeed_insights", primary_key=None)
            logging.info(f"Completed save_batch() for {object_name}.")
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"PageSpeed Error in get_dataframe(): {e}")
            return pd.DataFrame()