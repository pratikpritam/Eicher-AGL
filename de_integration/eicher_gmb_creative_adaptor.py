import re
import yaml
import logging
import pandas as pd
from typing import Dict, Any, List
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class GMBCreativeAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.connection_string = secrets["CONNECTION_STRING"]
        self.database = secrets["DATABASE"]
        self.objects = self.config["msil_gmb"]["objects"]
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.database]
        self.wrangler = DataWrangler(secrets)

    @staticmethod
    def _to_snake_case(col: str) -> str:
        col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
        col = re.sub(r"[^\w]+", "_", col)
        return col.lower().strip("_")
    @staticmethod
    def _normalize_objectid(val):
        if isinstance(val, ObjectId):
            return str(val)
        return val
    @staticmethod
    def _to_objectid_safe(val):
        try:
            return ObjectId(str(val))
        except (InvalidId, TypeError):
            return None
    def _flatten_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        flat: Dict[str, Any] = {}
        def recurse(obj, parent_key=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key = f"{parent_key}_{k}" if parent_key else k
                    recurse(v, key)
            else:
                col = self._to_snake_case(parent_key)
                val = self._normalize_objectid(obj)
                if isinstance(val, list):
                    val = ",".join(map(str, val))
                flat[col] = val
        recurse(doc)
        return flat

    def _normalize_docs(self, docs: List[Dict[str, Any]]) -> pd.DataFrame:
        logging.info("Normalizing, Flattening, cols_standardizations from Docs before creating DF")
        if not docs:
            return pd.DataFrame()
        rows = [self._flatten_document(d) for d in docs]
        df = pd.DataFrame(rows)
        df.columns = [self._to_snake_case(c) for c in df.columns]
        return df

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive or missing in config")
        return obj

    def extract_creative_raw(self, collection) -> pd.DataFrame:
        logging.info("🔹Extracting creative_raw --> eichercreativehashurls from MongoDB")
        docs = list(collection.find())
        logging.info(f"Total creative documents fetched: {len(docs)}")
        if not docs:
            return pd.DataFrame()
        df = self._normalize_docs(docs)
        return df

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"🚀 Starting extraction for object: {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        collection = self.db[obj_cfg['collection']]
        # logging.info(f"Collections found as: {collection}")
        if object_name == "creative_raw":
            creative_df = self.extract_creative_raw(collection)
            logging.info(f"✅Extracted {len(creative_df)} records for {object_name}---> Loading to save_batch..")
            self.wrangler.save_batch(creative_df, object_name, "msil_gmb", primary_key=obj_cfg["primary_key"])
            logging.info(f"✅Completed save_batch for: {object_name}")
            return pd.DataFrame()
        raise ValueError(f"Unsupported object_name: {object_name}")