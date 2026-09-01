import re
import json
import yaml
import time
import logging
import pandas as pd

from typing import Dict, Any, List, Tuple
from pymongo import MongoClient
from bson import ObjectId
from bson.errors import InvalidId
from pandas import json_normalize
from cryptography.fernet import Fernet

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)


class GeoBizzAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.secrets = secrets
        self.connection_string = secrets["CONNECTION_STRING"]
        self.database = secrets["DATABASE"]
        self.account_numbers = self.config["geobizz"]["_accounts"]
        self.objects = self.config["geobizz"]["objects"]
        self.encryption_key = secrets.get("ENCRYPTION_KEY")
        self.cipher = Fernet(self.encryption_key.encode()) if self.encryption_key else None
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
    def to_objectid_safe(val):
        try:
            return ObjectId(str(val))
        except (InvalidId, TypeError):
            return None
    def _flatten_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        flat = {}
        def recurse(obj, parent_key=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    key = f"{parent_key}_{k}" if parent_key else k
                    recurse(v, key)
            else:
                flat[self._to_snake_case(parent_key)] = self._normalize_objectid(obj)
        recurse(doc)
        return flat

    def _normalize_docs(self, docs: List[Dict[str, Any]]) -> pd.DataFrame:
        rows = [self._flatten_document(d) for d in docs]
        df = pd.DataFrame(rows)
        df.columns = [self._to_snake_case(c) for c in df.columns]
        return df.fillna("")
    
    def _encrypt_pii_columns(self, object_name: str, df: pd.DataFrame, obj_cfg: Dict[str, Any]) -> pd.DataFrame:
        logging.info(f"🔐 Starting PII encryption for object: {object_name}")
        if not self.cipher:
            logging.warning("⚠️ Encryption key not found. Skipping PII encryption.")
            return df
        pii_cols = obj_cfg.get("pii_columns", [])
        if not pii_cols:
            logging.warning(f"⚠️No PII columns defined in config for object. Skipping encryption.")
            return df
        for col in pii_cols:
            if col in df.columns:
                logging.info(f"🔐 Encrypting PII column: {col}")
                df[col] = df[col].astype(str).apply(
                    lambda x: self.cipher.encrypt(x.encode()).decode() if x else x
                )
            else:
                logging.warning(f"⚠️ PII column '{col}' not found in dataframe.")
        return df

    def extract_groups_raw(self, collection) -> Tuple[List[ObjectId], pd.DataFrame]:
        logging.info("🔹Extracting groups_raw for group_ids & grp_df")
        # query = {"accountNumber": {"$in": self.account_numbers}}
        query = {"accountName": {"$regex": "eicher", "$options": "i"}}
        docs = list(collection.find(query))
        if not docs:
            return [], pd.DataFrame()
        # group_ids = [d["_id"] for d in docs]
        grp_df = self._normalize_docs(docs)
        group_ids = grp_df["id"].unique().tolist()
        return group_ids, grp_df

    def extract_business_raw(self, collection, group_ids: List[ObjectId]) -> pd.DataFrame:
        logging.info("🔹Extracting business_raw with Help of group_ids")
        group_object_ids = [ObjectId(x) if not isinstance(x, ObjectId) else x for x in group_ids]
        query = {"groupId": {"$in": group_object_ids}}
        logging.info(f"FYR in Businss Raw --> Query is: {query}")
        docs = list(collection.find(query))
        logging.info(f"totally Bisiness_Raw docs are: {len(docs)}")
        if not docs:
            return pd.DataFrame()
        _df =self._normalize_docs(docs)
        business_ids = _df["id"].unique().tolist()
        return business_ids, _df
    
    def extract_businessreviews_raw(self, collection, group_ids: List) -> Tuple[List[str], pd.DataFrame]:
        logging.info("🔹 Extracting business_reviews_raw with Help of group_ids")
        query = {"groupId": {"$in": group_ids}}
        logging.info(f"FYR Biz_Revww --> Query is: {query}")
        docs = list(collection.find(query))
        if not docs:
            return [], pd.DataFrame()
        df = self._normalize_docs(docs)
        logging.info(f"bizz_rew columns: {df.columns}")
        return df

    def extract_reviews_raw(self, collection, business_ids: List[ObjectId]) -> pd.DataFrame:
        logging.info("🔹 Extracting Reviews with Help of Business_ids ")
        business_ids = [ObjectId(x) if not isinstance(x, ObjectId) else x for x in business_ids]
        query = {"businessId": {"$in": business_ids}}
        logging.info(f"FYR in Review --> Query is: {query}")
        docs = list(collection.find(query))
        logging.info(f"Total Len of Review-Docs: {len(docs)}")
        if not docs:
            return pd.DataFrame()
        return self._normalize_docs(docs)

    def extract_metrics_raw(self, collection, group_ids: List[str]) -> pd.DataFrame:
        logging.info("🔹 Extracting metrics with Help of group_ids")
        group_object_ids = [ObjectId(x) if not isinstance(x, ObjectId) else x for x in group_ids]
        query = {"groupId": {"$in": group_object_ids}}
        docs = list(collection.find(query))
        logging.info(f"FYR Metrics --> Query is: {query}")
        if not docs:
            return pd.DataFrame()
        logging.info(f"Total Len of metrics-Docs: {len(docs)}")
        return self._normalize_docs(docs)
    
    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"🚫❌Object '{object_name}' is not active or missing in config🚫❌")
        return obj
    
    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"🚀Starting extraction for: {object_name}")
        obj_cfg = self._get_obj_cfg(object_name)
        if not obj_cfg or not obj_cfg.get("active", False):
            raise ValueError(f"Object '{object_name}' is inactive or missing in config")
        groups_col = self.db["groups"]

        group_ids, groups_df = self.extract_groups_raw(groups_col)
        if object_name == "groups_raw":
            self.wrangler.save_batch(groups_df, object_name, "geobizz", primary_key=obj_cfg["primary_key"])
            logging.info(f"✅ Completed save_batch for: {object_name}")
            return pd.DataFrame()

        if object_name == "business_reviews_raw":
            breview_df = self.extract_businessreviews_raw(self.db["businessreviews"], group_ids)
            self.wrangler.save_batch(breview_df, object_name, "geobizz", primary_key=obj_cfg["primary_key"])
            logging.info(f"✅Completed save_batch for: {object_name}")
            return pd.DataFrame()

        if object_name in ['businesses_raw', "reviews_raw"]:
            business_ids, business_df = self.extract_business_raw(self.db["businesses"], group_ids)
            if object_name == "businesses_raw":
                business_df = business_df.astype(str)
                business_df = self._encrypt_pii_columns(object_name, business_df, obj_cfg)
                self.wrangler.save_batch(business_df, object_name, "geobizz", primary_key=obj_cfg["primary_key"])
                logging.info(f"✅ Completed save_batch for: {object_name}")
                return pd.DataFrame()            
            logging.info(f"GrpIds as: {group_ids} and business_ids of lenth-{len(business_ids)}")
            if object_name == "reviews_raw":
                reviews_df = self.extract_reviews_raw(self.db["reviews"], business_ids)
                reviews_df = self._encrypt_pii_columns(object_name, reviews_df, obj_cfg)
                self.wrangler.save_batch(reviews_df, object_name, "geobizz", primary_key=obj_cfg["primary_key"])
                logging.info(f"✅ Completed save_batch for: {object_name}")
                return pd.DataFrame()

        # METRICS
        if object_name == "metrics_raw":
            metrics_df = self.extract_metrics_raw(self.db["metrics"], group_ids)
            self.wrangler.save_batch(metrics_df, object_name, "geobizz", primary_key=obj_cfg["primary_key"])
            logging.info(f"✅ Completed save_batch for: {object_name}")
            return metrics_df

        raise ValueError(f"Unsupported object_name: {object_name}")