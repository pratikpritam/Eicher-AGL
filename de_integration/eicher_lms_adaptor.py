import re
import yaml
import logging
import pandas as pd
from typing import Dict, Any, List
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from cryptography.fernet import Fernet
from de_integration.data_wrangler import DataWrangler

logging.basicConfig(level=logging.INFO)


class LMSAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.connection_string = secrets["CONNECTION_STRING"]
        self.database = secrets["DATABASE"]
        self.objects = self.config["lms"]["objects"]
        self.pii_columns = set(self.config["lms"]["pii_columns"])
        self.encryption_key = secrets["ENCRYPTION_KEY"]
        self.cipher = Fernet(self.encryption_key)
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

    def _encrypt_pii(self, val: Any) -> Any:
        if val is None or val == "":
            return val
        if isinstance(val, list):
            return ",".join([
                self.cipher.encrypt(str(v).encode()).decode()
                for v in val
            ])
        return self.cipher.encrypt(str(val).encode()).decode()

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
                if col in self.pii_columns:
                    val = self._encrypt_pii(val)
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
        return df.fillna("")

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive or missing in config")
        return obj

    def extract_enquries_raw(self, collection, obj_cfg) -> pd.DataFrame:
        logging.info("🔹Extracting enquries_raw from MongoDB")
        docs = list(collection.find())
        logging.info(f"Total enquries documents fetched: {len(docs)}")
        if not docs:
            return pd.DataFrame()
        df = self._normalize_docs(docs)
        if "lead_type" in df.columns:
            logging.info(f"⛓️‍💥Lead_Type Bifurcations: --> {df['lead_type'].value_counts().to_dict()}")
        return df

    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"🚀 Starting extraction for object: {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        collection = self.db[obj_cfg['collection']]
        if object_name == "enquries_raw":
            enq_df = self.extract_enquries_raw(collection, obj_cfg)
            enq_df = enq_df.astype(str)
            logging.info(f"✅Extracted {len(enq_df)} records for {object_name}---> Loading to save_batch..")
            self.wrangler.save_batch(enq_df, object_name, "lms", primary_key=obj_cfg["primary_key"])
            logging.info(f"✅Completed save_batch for: {object_name}")
            return pd.DataFrame()
        raise ValueError(f"Unsupported object_name: {object_name}")