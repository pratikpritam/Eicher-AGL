import logging
import re
import requests
import pandas as pd
import yaml
import time
from typing import Dict, Any
from datetime import datetime, timedelta

from de_integration.data_wrangler import DataWrangler
logging.basicConfig(level=logging.INFO)

class KonnectAdaptor:
    def __init__(self, config_path: str, secrets: Dict[str, str]):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.objects = self.config["konnect_insight"]["objects"]
        self.grp_url = self.config["konnect_insight"]["grp_url"]
        self.secrets = secrets
        self.wrangler = DataWrangler(secrets)

    @staticmethod
    def _to_snake_case(col: str) -> str:
        col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
        col = re.sub(r"[^\w]+", "_", col)
        return col.lower().strip("_")

    def _extract_groups(self, obj_cfg: Dict[str, Any]) -> pd.DataFrame:
        _df = self._fetch_data(self.grp_url)
        if _df.empty:
            logging.error("No🚫❌groups returned from API.")
            return 
        group_ids_list = _df["GroupId"].tolist()
        logging.info(f"Stored {len(group_ids_list)} group IDs for downstream use.")
        return _df, group_ids_list

    def _extract_with_groups(self, object_name: str, obj_cfg: Dict[str, Any]):
        _df, group_ids_list = self._extract_groups(obj_cfg)
        for gid in group_ids_list:
            logging.info(f"Extracting {object_name} for group ID: {gid}")
            df = self._fetch_data(obj_cfg["url"], {"grpid": gid})
            if df.empty:
                logging.error(f"No data 🚫❌ for {object_name} with group {gid}")
                continue
            if "groupid" not in df.columns.str.lower():
                    df["groupid"] = gid
            self.wrangler.save_batch(df, object_name, "konnect", primary_key=obj_cfg["primary_key"])
            logging.info(f"Completed save_batch() for {object_name} with group ID: {gid}")
            # self._sleep()
        return group_ids_list

    def _get_date_ranges(self):
        backfill_cfg = self.config["konnect_insight"].get("backfill", {})
        today = datetime.now().date()
        if backfill_cfg.get("enabled", False):
            start_date = datetime.strptime(backfill_cfg["start_date"], "%Y-%m-%d").date()
            end_date = today - timedelta(days=2)
            # end_date = datetime.strptime(backfill_cfg["end_date"], "%Y-%m-%d").date()
            date_ranges = []
            current = start_date
            while current <= end_date:
                chunk_end = min(current + timedelta(days=9), end_date)
                since = f"{current} 00:00:00"
                until = f"{chunk_end} 23:59:59"
                date_ranges.append((since, until))
                current = chunk_end + timedelta(days=1)
            logging.info(f"BACKFILL MODE ACTIVATED--→ {len(date_ranges)} chunks (10-day each) --> from DateRange of {date_ranges}.")
            return date_ranges
        start = today - timedelta(days=5)          #back 2 days to avoid missing of any Records
        end = today - timedelta(days=2)
        logging.info(f"📅Backfill Disabled🚫🚫-→Running DAILY incremental '{start}' to '{end}'.")
        return [(f"{start} 00:00:00", f"{end} 23:59:59")]

    def _extract_social_messages(self, object_name: str, obj_cfg: dict, media: str):
        _df, group_ids_list = self._extract_groups(obj_cfg)
        date_ranges = self._get_date_ranges()
        api_call_count = 0
        for gid in group_ids_list:
            logging.info(f"Extracting ProfileIds associated with groupID: {gid}")
            prf_df = self._fetch_data(self.config["konnect_insight"]["prf_url"], {"grpid": gid})
            logging.info(f"Grouped-Bifurcation of profiles based on media: {media}")
            
            profiles = prf_df.groupby("Media")["ProfileId"].apply(list).to_dict()
            if media not in profiles:
                logging.warning(f"🚫❌No profiles found for media: {media}🚫❌")
                continue
            profile_ids = profiles[media]
            logging.info(f"📻[{object_name}]📲 Found {len(profile_ids)} profileIds for media={media}")
            for pid in profile_ids:
                final_df = []
                for since, until in date_ranges:
                    api_call_count += 1
                    if api_call_count % 5 == 0:
                        self._sleep()
                    logging.info(f"Processing window: {since} → {until}")
                    logging.info(f"Fetching {object_name} for profileId={pid} from {since} to {until}")
                    url = (
                        obj_cfg["url"]
                        .replace("profile_id", str(pid))
                        .replace("_since", str(since))
                        .replace("_until", str(until))
                    )
                    df = self._fetch_data(url, {"grpid": gid})
                    if df.empty:
                        logging.warning(f"🚫❌No data for profile {pid} from '{since}' to '{until}'.")
                        continue
                    df["profileid"] = pid
                    df["media"] = media
                    df["groupid"] = gid
                    final_df.append(df)
                    logging.info(f"Datafrme found {len(df)} records-----> Now appended to Final DF.")
                if not final_df:
                    logging.warning(f"🚫❌No SocialMessages data found for media={media}")
                    continue
                merged_df = pd.concat(final_df, ignore_index=True)
                logging.info(f"[{object_name}] Final merged DF rows for {pid} =====>> {len(merged_df)}")
                self.wrangler.save_batch(merged_df, object_name, "konnect", primary_key=obj_cfg["primary_key"])
        return profile_ids

    def _extract_social_insights(self, object_name: str, obj_cfg: dict):
        _df, group_ids_list = self._extract_groups(obj_cfg)
        date_ranges = self._get_date_ranges()
        api_call_count = 0
        for gid in group_ids_list:
            logging.info(f"Extracting ProfileIds associated with groupID: {gid}")
            prf_df = self._fetch_data(self.config["konnect_insight"]["prf_url"], {"grpid": gid})
            profile_ids = prf_df["ProfileId"].tolist()
            logging.info(f"📻[{object_name}]📲 Found {len(profile_ids)} profileIds.")
            for pid in profile_ids:
                final_df = []
                for since, until in date_ranges:
                    api_call_count += 1
                    if api_call_count % 5 == 0:
                        self._sleep()
                    logging.info(f"Processing window: {since} → {until}")
                    logging.info(f"Fetching {object_name} for profileId={pid} from {since} to {until}")
                    url = (obj_cfg["url"].replace("profile_id", str(pid)).replace("_since", str(since)).replace("_until", str(until)))
                    df = self._fetch_data(url, {"grpid": gid})
                    if df.empty:
                        logging.warning(f"🚫❌No data Available for profile '{pid}' from '{since}' to '{until}'.")
                        continue
                    if object_name == "messages_raw":
                        df["profile_id"] = pid
                    else:
                        df["profileid"] = pid
                    df["groupid"] = gid
                    final_df.append(df)
                    logging.info(f"Datafrme found {len(df)} records-----> Now appended to Final DF.")
                if not final_df:
                    logging.warning(f"🚫❌No {object_name} data found for given date range {date_ranges}.")
                    continue
                merged_df = pd.concat(final_df, ignore_index=True)
                if object_name == "social_insights_raw":
                    merged_df.columns = [self._to_snake_case(col) for col in merged_df.columns]
                logging.info(f"[{object_name}] Final merged DF rows for {pid} =====>> {len(merged_df)}")
                self.wrangler.save_batch(merged_df, object_name, "konnect", primary_key=obj_cfg["primary_key"])
        return merged_df

    def _get_obj_cfg(self, object_name: str) -> Dict[str, Any]:
        obj = next((o[object_name] for o in self.objects if object_name in o), None)
        if not obj or not obj.get("active", False):
            raise ValueError(f"🚫❌Object '{object_name}' is not active or missing in config🚫❌")
        return obj

    def _fetch_data(self, url: str, extra: Dict[str, Any] = None) -> pd.DataFrame:
        extra = extra or {}
        url = (
            url.replace("account__token", self.secrets["ACCOUNT_TOKEN"])
               .replace("user__token", self.secrets["USER_TOKEN"])
        )
        for k, v in extra.items():
            url = url.replace(str(k), str(v))
        logging.info(f"Fetching from URL: '{url}'")
        try:
            resp = requests.get(url)
            if resp.status_code != 200:
                logging.warning(f"❌ HTTP Error '{resp.status_code}' for above URL.")
                return pd.DataFrame()
            data = resp.json()
        except requests.RequestException as e:
            logging.warning(f"❌❌❌Request failed due to: {e}")
            return pd.DataFrame()
        except ValueError:
            logging.warning(f"❌ Invalid JSON response from '{url}'")
            return pd.DataFrame()
        if "error" in data:
            logging.error(f"🚫API having 'Error' as: {data['error']}")
            return pd.DataFrame()
        docs = resp.json().get("docs", [])
        if not docs:
            logging.error(f"🚫No docs found in response after all checked.")
            return pd.DataFrame()
        if isinstance(docs, list) and len(docs) == 1:
            first_doc = docs[0]
            if isinstance(first_doc, dict) and "StatusId" in first_doc:
                logging.error(f"🚫API returned error in docs for above URL as: {first_doc}")
                return pd.DataFrame()
        df = pd.DataFrame(docs)
        # df.columns = [self._to_snake_case(col) for col in df.columns]
        return df

    def _sleep(self, seconds: int = 20):
        logging.info(f"😴😴Sleeping for {seconds}s to respect API rate limits...")
        time.sleep(seconds)
    
    def get_dataframe(self, object_name: str) -> pd.DataFrame:
        logging.info(f"Starting extraction for {object_name} under get_dataframe()")
        obj_cfg = self._get_obj_cfg(object_name)

        if object_name == "groups":
            _df, group_ids_list = self._extract_groups(obj_cfg)
            self.wrangler.save_batch(_df, "groups", "konnect", primary_key=obj_cfg["primary_key"])
            logging.info(f"Completed save_batch() for groups with {len(group_ids_list)} group IDs.")
            # self._sleep()
            return pd.DataFrame()

        if object_name in ["profiles", "topics", "clusters", "classifications", "severity", "commenter_type", "commenter_level", "conversation_type", "additional_info_fields", "active_users_in_queue"]:
            logging.info(f"Object {object_name} requires group IDs for extraction.")
            group_ids_list =self._extract_with_groups(object_name, obj_cfg)
            logging.info(f"Completed extraction for {object_name} with group IDs: {group_ids_list}")
            return pd.DataFrame()
        
        if object_name in ["youtube_raw", "facebook_raw", "instagram_raw", "linkedin_raw", "X_raw"]:
            logging.info(f"Object {object_name} requires groupID & ProfileID for extraction.")
            SOCIAL_MEDIA_MAP = {"youtube_raw": "Youtube", "instagram_raw": "Instagram", "facebook_raw": "Facebook", "linkedin_raw": "LinkedIn", "X_raw": "Twitter"}
            media = SOCIAL_MEDIA_MAP[object_name]
            profile_ids = self._extract_social_messages(object_name, obj_cfg, media)
            logging.info(f"Completed extraction of {object_name} with profile IDs: {profile_ids}")
            return pd.DataFrame()

        if object_name in ["social_insights_raw", "messages_raw"]:
            logging.info(f"Object {object_name} requires groupID & ProfileID for extraction.")
            social_insight_df = self._extract_social_insights(object_name, obj_cfg)
            logging.info(f"Completed extraction of {object_name}.")
            return pd.DataFrame()