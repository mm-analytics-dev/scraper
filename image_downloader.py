# -*- coding: utf-8 -*-
import os, re, io, sys, time, math
from datetime import datetime, timezone, date
from urllib.parse import urlparse

import requests
import pandas as pd

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter, QueryJobConfig
from google.cloud import storage

# ---------- ENV ----------
PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "")
DATASET      = os.getenv("BQ_DATASET", "realestate_v2")
LOCATION     = os.getenv("BQ_LOCATION", "EU")
BUCKET_NAME  = os.getenv("GCS_BUCKET", "")

LOOKBACK_DAYS     = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS      = int(os.getenv("IMAGES_MAX_LISTINGS", "10"))
MAX_PER_LISTING   = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))
BATCH_DATE_STR    = os.getenv("BATCH_DATE")  # optional YYYYMMDD
TABLE_RECREATE    = os.getenv("IMAGES_TABLE_RECREATE", "false").lower() == "true"

# ---------- HTTP ----------
REQ_TIMEOUT = (10, 30)  # connect, read
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
})

# ---------- BQ / GCS ----------
bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
gcs = storage.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)

# ---------- Helpers ----------
def yyyymmdd_today_utc() -> str:
    if BATCH_DATE_STR:
        return BATCH_DATE_STR
    return datetime.utcnow().strftime("%Y%m%d")

def safe_int(x):
    try:
        return None if pd.isna(x) else int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

def ext_from_response(url: str, resp: requests.Response) -> str:
    # prefer Content-Type
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "image/webp" in ct: return ".webp"
    if "image/jpeg" in ct or "image/jpg" in ct: return ".jpg"
    if "image/png" in ct: return ".png"
    if "image/avif" in ct: return ".avif"
    if "image/gif" in ct: return ".gif"
    # fallback to URL
    path = urlparse(url).path.lower()
    for suf in (".webp",".jpg",".jpeg",".png",".avif",".gif",".jfif"):
        if path.endswith(suf):
            return ".jpg" if suf == ".jpeg" else suf
    return ".jpg"

def make_folder(seq_global, listing_id):
    seq = f"{int(seq_global):06d}" if seq_global is not None else "000000"
    lid = (listing_id or "NA").replace("/", "_")
    return f"{seq}_{lid}"

def gcs_blob_exists(path: str) -> bool:
    return bucket.blob(path).exists()

def ensure_images_table():
    """(re)create images table with the correct schema when TABLE_RECREATE=true."""
    table_id = f"{PROJECT_ID}.{DATASET}.images"
    schema = [
        bigquery.SchemaField("pk", "STRING"),
        bigquery.SchemaField("listing_id", "STRING"),
        bigquery.SchemaField("seq_global", "INT64"),
        bigquery.SchemaField("url", "STRING"),          # URL detailu inzerátu
        bigquery.SchemaField("image_url", "STRING"),    # zdrojová URL
        bigquery.SchemaField("image_rank", "INT64"),
        bigquery.SchemaField("gcs_path", "STRING"),
        bigquery.SchemaField("batch_date", "DATE"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
        bigquery.SchemaField("http_status", "INT64"),
        bigquery.SchemaField("bytes", "INT64"),
        bigquery.SchemaField("content_type", "STRING"),
        bigquery.SchemaField("error", "STRING"),
    ]
    if TABLE_RECREATE:
        try:
            bq.delete_table(table_id, not_found_ok=True)
            print(f"[BQ] Dropped table {table_id}")
        except Exception as e:
            print(f"[BQ] Drop error (ignored): {e}")
        table = bigquery.Table(table_id, schema=schema)
        table = bq.create_table(table)
        print(f"[BQ] Created table {table_id} with schema.")
    else:
        # nothing; we append later (and let it be created by pandas_gbq if missing)
        pass

def query_df(sql: str, params: list[ScalarQueryParameter]) -> pd.DataFrame:
    job = bq.query(sql, job_config=QueryJobConfig(query_parameters=params))
    return job.result().to_dataframe(create_bqstorage_client=False)

# ---------- SQL: fetch candidates ----------
SQL_CANDIDATES = f"""
DECLARE lookback_days INT64 DEFAULT @lookback_days;
DECLARE max_listings  INT64 DEFAULT @max_listings;
DECLARE max_per       INT64 DEFAULT @max_per;

WITH base AS (
  SELECT
    pk,
    listing_id,
    CAST(seq_global AS INT64) AS seq_global,
    url AS listing_url,
    image_url,
    CAST(image_rank AS INT64) AS image_rank,
    batch_ts,
    DATE(batch_ts) AS batch_date
  FROM `{PROJECT_ID}.{DATASET}.images_urls_daily`
  WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
),
dedup AS (
  -- 1) posledná verzia rovnakej image_url v rámci pk
  -- 2) poistka na duplicity v tom istom ranku
  SELECT * EXCEPT(rn1, rn2)
  FROM (
    SELECT
      b.*,
      ROW_NUMBER() OVER (PARTITION BY pk, image_url ORDER BY batch_ts DESC) AS rn1,
      ROW_NUMBER() OVER (PARTITION BY pk, image_rank ORDER BY batch_ts DESC) AS rn2
    FROM base b
  )
  WHERE rn1 = 1 AND rn2 = 1
),
pk_list AS (
  SELECT pk
  FROM (
    SELECT pk, MAX(batch_ts) AS last_ts
    FROM dedup
    GROUP BY pk
    ORDER BY last_ts DESC
    LIMIT max_listings
  )
),
limited AS (
  SELECT d.*
  FROM dedup d
  JOIN pk_list p USING (pk)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY image_rank) <= max_per
)
SELECT
  pk, listing_id, seq_global, listing_url, image_url, image_rank, batch_date
FROM limited
ORDER BY pk, image_rank
"""

def fetch_candidates() -> pd.DataFrame:
    params = [
        ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
        ScalarQueryParameter("max_listings",  "INT64", MAX_LISTINGS),
        ScalarQueryParameter("max_per",       "INT64", MAX_PER_LISTING),
    ]
    return query_df(SQL_CANDIDATES, params)

# ---------- Download & upload ----------
def download_and_upload(row, base_prefix: str):
    url = row["image_url"]
    pk = row["pk"]
    listing_id = row["listing_id"]
    seq_global = safe_int(row["seq_global"])
    rank = safe_int(row["image_rank"]) or 0

    folder = make_folder(seq_global, listing_id)
    # Request first (to decide extension & content-type)
    try:
        resp = SESSION.get(url, timeout=REQ_TIMEOUT, stream=True)
        status = resp.status_code
    except Exception as e:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "http_status": None,
            "bytes": None,
            "content_type": None,
            "error": f"request_error: {e}",
        }

    if status != 200:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "http_status": int(status),
            "bytes": None,
            "content_type": resp.headers.get("Content-Type"),
            "error": f"http_{status}",
        }

    ext = ext_from_response(url, resp)
    filename = f"{rank:03d}{ext}"
    gcs_path = f"{base_prefix}/{folder}/{filename}"

    # Skip if already uploaded
    if gcs_blob_exists(gcs_path):
        return {
            **row,
            "gcs_path": gcs_path,
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "http_status": 200,
            "bytes": None,
            "content_type": resp.headers.get("Content-Type"),
            "error": None,
        }

    # Read bytes
    try:
        content = resp.content
        size = len(content)
        ctype = resp.headers.get("Content-Type")
    finally:
        resp.close()

    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=ctype)
        print(f"[OK] gs://{BUCKET_NAME}/{gcs_path}", flush=True)
        return {
            **row,
            "gcs_path": gcs_path,
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "http_status": 200,
            "bytes": int(size),
            "content_type": ctype,
            "error": None,
        }
    except Exception as e:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "http_status": 200,
            "bytes": int(size),
            "content_type": ctype,
            "error": f"gcs_upload_error: {e}",
        }

# ---------- BQ writer ----------
def write_results(df_out: pd.DataFrame):
    # enforce dtypes
    if df_out.empty:
        return
    df = df_out.copy()
    df["seq_global"]   = pd.to_numeric(df["seq_global"], errors="coerce").astype("Int64")
    df["image_rank"]   = pd.to_numeric(df["image_rank"], errors="coerce").astype("Int64")
    df["http_status"]  = pd.to_numeric(df["http_status"], errors="coerce").astype("Int64")
    df["bytes"]        = pd.to_numeric(df["bytes"], errors="coerce").astype("Int64")
    df["downloaded_at"]= pd.to_datetime(df["downloaded_at"], utc=True)
    df["batch_date"]   = pd.to_datetime(df["batch_date"], errors="coerce").dt.date

    # pandas_gbq append
    import pandas_gbq
    schema = [
        {"name":"pk","type":"STRING"},
        {"name":"listing_id","type":"STRING"},
        {"name":"seq_global","type":"INT64"},
        {"name":"url","type":"STRING"},
        {"name":"image_url","type":"STRING"},
        {"name":"image_rank","type":"INT64"},
        {"name":"gcs_path","type":"STRING"},
        {"name":"batch_date","type":"DATE"},
        {"name":"downloaded_at","type":"TIMESTAMP"},
        {"name":"http_status","type":"INT64"},
        {"name":"bytes","type":"INT64"},
        {"name":"content_type","type":"STRING"},
        {"name":"error","type":"STRING"},
    ]
    pandas_gbq.to_gbq(
        df,
        f"{DATASET}.images",
        project_id=PROJECT_ID,
        if_exists="append",
        table_schema=schema,
        location=LOCATION,
        progress_bar=False,
    )

# ---------- MAIN ----------
def main():
    if not (PROJECT_ID and DATASET and LOCATION and BUCKET_NAME):
        raise RuntimeError("Chýba GCP_PROJECT_ID / BQ_DATASET / BQ_LOCATION / GCS_BUCKET env.")

    ensure_images_table()

    # fetch work
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] Žiadni kandidáti na sťahovanie (v lookback okne).")
        return

    # cieľový prefix
    batch_yyyymmdd = yyyymmdd_today_utc()
    base_prefix = f"images_v2/{batch_yyyymmdd}"

    # dedup v rámci behu (pre istotu)
    seen_paths = set()
    results = []

    for _, r in cand.iterrows():
        row = {
            "pk": r.get("pk"),
            "listing_id": r.get("listing_id"),
            "seq_global": r.get("seq_global"),
            "url": r.get("listing_url"),
            "image_url": r.get("image_url"),
            "image_rank": r.get("image_rank"),
            "batch_date": r.get("batch_date") or date.today(),
        }
        # Predikcia GCS path (ak by sme vedeli ext až po headere, skipneme iba keď už reálne existuje)
        out = download_and_upload(row, base_prefix)
        # ešte vnútrobehový dedup (keď by kandidáty obsahovali duplicitný rank)
        key = (out.get("pk"), out.get("image_rank"), out.get("gcs_path"))
        if key in seen_paths:
            continue
        seen_paths.add(key)
        results.append(out)

    df_out = pd.DataFrame(results, columns=[
        "pk","listing_id","seq_global","url","image_url","image_rank",
        "gcs_path","batch_date","downloaded_at","http_status","bytes","content_type","error"
    ])
    write_results(df_out)

if __name__ == "__main__":
    main()
