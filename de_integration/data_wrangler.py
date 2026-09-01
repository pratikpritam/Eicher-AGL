import logging
import pandas as pd
from google.cloud import storage, bigquery
from datetime import datetime
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)

class DataWrangler:
    def __init__(self, secrets: dict):
        self.secrets = secrets
        self.project_id = secrets['PROJECT_ID']
        self.dataset = secrets['BQ_DATASET']
        self.bucket_name = secrets['GCS_BUCKET']
        self.gcs_client = storage.Client()
        self.bq_client = bigquery.Client(project=self.project_id)
    
    def slowly_changing_schema_check(self, df: pd.DataFrame, table_id: str):
        try:
            table = self.bq_client.get_table(table_id)
            bq_columns = {field.name for field in table.schema}
            df_columns = set(df.columns)

            new_columns = df_columns - bq_columns
            if new_columns:
                logging.warning(
                    f"[SCHEMA DRIFT] New columns detected for {table_id}: {sorted(new_columns)}"
                )
        except Exception as e:
            logging.warning(f"Could not fetch Existing Schema for {table_id}: {e}")


    def save_batch(self, df: pd.DataFrame, object_name: str, api_name: str, primary_key: str = None):
        table_name = f"eicher_{api_name}_{object_name}"
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"

        df.columns = df.columns.map(str).str.lower()
        df = df.sort_index(axis=1)

        if primary_key:
            before = len(df)
            if isinstance(primary_key, list):
                df = df.dropna(subset=primary_key)
                missing_cols = [col for col in primary_key if col not in df.columns]
                if missing_cols:
                    logging.warning(f"{object_name} Missing columns for composite key: {missing_cols}")
                else:
                    df["_composite_key"] = df[primary_key].astype(str).agg("_".join, axis=1)
                    df = df.drop_duplicates(subset="_composite_key")
                    logging.info(
                        f"[{object_name}] Dropped {before - len(df)} in-batch duplicates based on composite key {primary_key}."
                    )
                    try:
                        key_expr = " || '_' || ".join(primary_key)
                        query = f"SELECT DISTINCT {key_expr} AS composite_key FROM `{self.project_id}.{self.dataset}.{table_name}`"
                        existing_keys = {row["composite_key"] for row in self.bq_client.query(query).result()}
                        before = len(df)
                        df = df[~df["_composite_key"].isin(existing_keys)]
                        logging.info(
                            f"[{object_name}] Removed {before - len(df)} records bcz It's already existing in BigQuery (composite key)."
                        )
                    except Exception as e:
                        logging.warning(f"[{object_name}] Could not fetch existing composite keys: {e}")

                    df = df.drop(columns="_composite_key", errors="ignore")
            elif isinstance(primary_key, str) and primary_key in df.columns:
                df = df.dropna(subset=[primary_key])
                df[primary_key] = df[primary_key].astype(str).str.strip().str.lower()
                before = len(df)
                df = df.drop_duplicates(subset=[primary_key])
                logging.info(
                    f"{object_name} Dropped {before - len(df)} in-batch duplicates based on '{primary_key}'."
                )
                try:
                    query = f"""SELECT DISTINCT LOWER(TRIM({primary_key})) AS key FROM `{table_id}`"""
                    existing_keys = {row["key"] for row in self.bq_client.query(query).result()}
                    before = len(df)
                    df = df[~df[primary_key].isin(existing_keys)]
                    logging.info(
                        f"{object_name} Removed {before - len(df)} records because they already exist in BigQuery."
                    )
                except Exception as e:
                    logging.warning(f"{object_name} Could not fetch existing keys: {e}")

        if df.empty:
            logging.info(f"No data to save for {object_name}. Skipping further GCS & BQ Load.")
            return
        df = df.astype(str)
        df["ingestion_timestamp"] = datetime.now(ZoneInfo("Asia/Kolkata"))
        df["bq_ingested_by"] = "Eicher-Airflowa"
        self.slowly_changing_schema_check(df, table_id)

        # --- Upload to GCS ---
        logging.info(f"Uploading {len(df)} records to GCS for table {object_name}.")
        gcs_path = f"{api_name}/raw_layer/{object_name}/{object_name}_{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')}.parquet"
        bucket = self.gcs_client.bucket(self.bucket_name)
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(df.to_parquet(index=False), content_type="application/octet-stream")
        logging.info(f"Saved {len(df)} rows to GCS at {gcs_path}")

        # --- Load to BigQuery ---
        logging.info(f"Loading data into BigQuery table {table_id}.")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            autodetect=True
        )
        job = self.bq_client.load_table_from_uri(
            f"gs://{self.bucket_name}/{gcs_path}",
            table_id,
            job_config=job_config
        )
        job.result()
        logging.info(f"Loaded {len(df)} rows into BigQuery table {table_id}")