# -*- coding: utf-8 -*-
"""
Images downloader (v2) -> GCS + BigQuery log

Čo robí:
- Z BQ tab. {BQ_DATASET}.images_urls_daily si vyberie kandidátov za posledné N dní
  (IMAGES_LOOKBACK_DAYS), zoberie najviac M inzerátov (IMAGES_MAX_LISTINGS) a
  max K fotiek na inzerát (IMAGES_MAX_PER_LISTING).
- Stiahne obrázky, uloží do GCS do prefixu: images_v2/YYYYMMDD/{FOLDER}/{NNN.ext}
  (YYYYMMDD = dnešný UTC alebo BATCH_DATE=YYYYMMDD).
- Zapíše výsledky do BQ tab. {BQ_DATASET}.images (append), so schémou:
  pk, listing_id, seq_global, url, image_url, image_rank, gcs_path,
  http_status, bytes, content_type, downloaded_at(TIMESTAMP UTC), error,
  batch_date(DATE), batch_ts(TIMESTAMP UTC).

Poznámky:
- Dedup: v SQL (ROW_NUMBER PARTITION BY pk,image_url) + v kóde podľa gcs_path.
- Ak blob v GCS už existuje, neuploaduje sa znovu (ale riadok sa zaloguje ako OK).
- Ak potrebuješ „reset“ schémy tabulky images, nastav IMAGES_TABLE_RECREATE=true
  (jednorazovo). Skript tabulku pred zápisom zmaže a založí sa nanovo pri loade.

ENV (s rozumnými defaultami):
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=3
  IMAGES_MAX_LISTINGS=15
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD (voliteľné)
  IMAGES_TABLE=images (voliteľné, default 'images')
  IMAGES_TABLE_RECREATE=true|false (default false)
"""

import os
import re
import math
import json
import time
import random
from datetime import datetime, timezone, date
from typing import Dict, Any, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.cloud import bigquery
from google.cloud import storage


# --------- ENV ---------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET = os.getenv("BQ_DATASET", "realestate_v2").strip()
LOCATION = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME = os.getenv("GCS_BUCKET", "").strip()

LOOKBACK_DAYS = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS = int(os.getenv("IMAGES_MAX_LISTINGS", "15"))
MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
IMAGES_TABLE = os.getenv("IMAGES_TABLE", "images").strip()
IMAGES_TABLE_RECREATE = os.getenv("IMAGES_TABLE_RECREATE", "false").lower() == "true"

# --------- Helpers ---------
def now_utc() -> pd.Timestamp:
    # Pandas-safe UTC timestamp
    return pd.Timestamp.now(tz=timezone.utc)

def today_yyyymmdd_utc() -> str:
    if BATCH_DATE_ENV:
        # Validate format
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = today_yyyymmdd_utc()
BASE_PREFIX = f"images_v2/{BATCH_YYYYMMDD}"

# --------- HTTP session with retries ---------
SESSION = requests.Session()
_retry = Retry(
    total=4, connect=3, read=3,
    backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"])
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]
def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
        "Accept-Language": "sk-SK,sk;q=0.9,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra: h.update(extra)
    return h

# --------- GCS / BQ clients ---------
bq = bigquery.Client(project=PROJECT_ID, location=LOCATION) if PROJECT_ID else bigquery.Client(location=LOCATION)
gcs = storage.Client(project=PROJECT_ID) if PROJECT_ID else storage.Client()
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

# --------- SQL (parametrized, bez výrazov v LIMIT) ---------
SQL_CANDIDATES = f"""
WITH recent AS (
  SELECT
    pk,
    listing_id,
    seq_global,
    url,
    image_url,
    image_rank,
    batch_ts
  FROM `{PROJECT_ID}.{DATASET}.images_urls_daily`
  WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY pk, image_url ORDER BY image_rank) AS dup_rn
  FROM recent
),
ranked AS (
  SELECT
    pk, listing_id, seq_global, url, image_url, image_rank,
    ROW_NUMBER() OVER (PARTITION BY pk ORDER BY image_rank) AS rn
  FROM dedup
  WHERE dup_rn = 1
),
chosen AS (
  SELECT pk
  FROM ranked
  GROUP BY pk
  ORDER BY MIN(seq_global)
  LIMIT @max_listings
)
SELECT
  r.pk, r.listing_id, r.seq_global, r.url, r.image_url, r.image_rank
FROM ranked r
JOIN chosen c USING(pk)
WHERE r.rn <= @max_per
ORDER BY r.seq_global, r.pk, r.rn
"""

def query_df(sql: str, params: List[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=params
    )
    job = bq.query(sql, job_config=job_cfg, location=LOCATION)
    return job.result().to_dataframe(create_bqstorage_client=False)

def fetch_candidates() -> pd.DataFrame:
    params = [
        bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
        bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
        bigquery.ScalarQueryParameter("max_per", "INT64", MAX_PER_LISTING),
    ]
    return query_df(SQL_CANDIDATES, params)

# --------- Path / naming helpers ---------
def slugify(s: str) -> str:
    if not s: return "NA"
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "na"

def make_folder(seq_global: Optional[int], listing_id: Optional[str]) -> str:
    if seq_global is not None and not (pd.isna(seq_global) or seq_global == ""):
        seq = int(seq_global)
        if listing_id:
            return f"{seq:06d}_{slugify(str(listing_id))}"
        return f"{seq:06d}"
    # fallback
    return slugify(listing_id) if listing_id else "na"

def ext_from_response(url: str, resp: requests.Response) -> str:
    # priority: content-type -> url
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/webp" in ctype: return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype: return ".jpg"
    if "image/png" in ctype: return ".png"
    if "image/avif" in ctype: return ".avif"
    if "image/gif" in ctype: return ".gif"
    # url fallback
    u = url.lower().split("?")[0]
    for suf in (".webp", ".jpg", ".jpeg", ".png", ".avif", ".gif"):
        if u.endswith(suf): return suf if suf != ".jpeg" else ".jpg"
    return ".webp"

def safe_int(x):
    try:
        if pd.isna(x): return None
        return int(x)
    except Exception:
        return None

def gcs_blob_exists(path: str) -> bool:
    if not bucket: return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

# --------- Download + upload ---------
def download_and_upload(row: Dict[str, Any], base_prefix: str) -> Dict[str, Any]:
    url = row.get("image_url")
    if not url:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": now_utc(),
            "http_status": None,
            "bytes": None,
            "content_type": None,
            "error": "no_url",
        }

    pk = row.get("pk")
    listing_id = row.get("listing_id")
    seq_global = safe_int(row.get("seq_global"))
    rank = safe_int(row.get("image_rank")) or 0

    folder = make_folder(seq_global, listing_id)

    # GET
    try:
        resp = SESSION.get(url, timeout=30, headers=rand_headers(), stream=True)
        status = resp.status_code
    except Exception as e:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": now_utc(),
            "http_status": None,
            "bytes": None,
            "content_type": None,
            "error": f"request_error: {e}",
        }

    if status != 200:
        try_ctype = resp.headers.get("Content-Type")
        try:
            resp.close()
        except Exception:
            pass
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": now_utc(),
            "http_status": int(status),
            "bytes": None,
            "content_type": try_ctype,
            "error": f"http_{status}",
        }

    ext = ext_from_response(url, resp)
    filename = f"{rank:03d}{ext}"
    gcs_path = f"{base_prefix}/{folder}/{filename}"

    # skip upload if already in bucket
    if gcs_blob_exists(gcs_path):
        try_ctype = resp.headers.get("Content-Type")
        try:
            _ = resp.content  # drain so connection is reusable, then close
        except Exception:
            pass
        finally:
            resp.close()
        return {
            **row,
            "gcs_path": gcs_path,
            "downloaded_at": now_utc(),
            "http_status": 200,
            "bytes": None,
            "content_type": try_ctype,
            "error": None,
        }

    # read content & upload
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
            "downloaded_at": now_utc(),
            "http_status": 200,
            "bytes": int(size),
            "content_type": ctype,
            "error": None,
        }
    except Exception as e:
        return {
            **row,
            "gcs_path": None,
            "downloaded_at": now_utc(),
            "http_status": 200,
            "bytes": int(size),
            "content_type": ctype,
            "error": f"gcs_upload_error: {e}",
        }

# --------- Write results to BQ ---------
def write_results(rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] Nothing to write.")
        return

    df = pd.DataFrame(rows)

    # unify dtypes
    if "downloaded_at" in df.columns:
        df["downloaded_at"] = pd.to_datetime(df["downloaded_at"], utc=True)
    for col in ("http_status", "bytes", "image_rank", "seq_global"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # required text cols
    for col in ("pk", "listing_id", "url", "image_url", "gcs_path", "content_type", "error"):
        if col in df.columns:
            df[col] = df[col].astype("string").where(df[col].notna(), None)

    # add batch metadata
    df["batch_date"] = pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d", utc=True).dt.date
    df["batch_ts"] = now_utc()

    # BQ load
    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_TABLE}" if PROJECT_ID else f"{DATASET}.{IMAGES_TABLE}"

    # Optional: recreate table once if requested
    if IMAGES_TABLE_RECREATE:
        try:
            bq.delete_table(table_id, not_found_ok=True)
            print(f"[INFO] Dropped table {table_id} (IMAGES_TABLE_RECREATE).")
        except Exception as e:
            print(f"[WARN] Drop table failed (ignored): {e}")

    schema = [
        bigquery.SchemaField("pk", "STRING"),
        bigquery.SchemaField("listing_id", "STRING"),
        bigquery.SchemaField("seq_global", "INT64"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("image_rank", "INT64"),
        bigquery.SchemaField("gcs_path", "STRING"),
        bigquery.SchemaField("http_status", "INT64"),
        bigquery.SchemaField("bytes", "INT64"),
        bigquery.SchemaField("content_type", "STRING"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
        bigquery.SchemaField("error", "STRING"),
        bigquery.SchemaField("batch_date", "DATE"),
        bigquery.SchemaField("batch_ts", "TIMESTAMP"),
    ]

    job_cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )

    job = bq.load_table_from_dataframe(df, table_id, job_config=job_cfg, location=LOCATION)
    job.result()  # wait
    print(f"[BQ] Loaded {len(df)} rows into {table_id}.")


# --------- Main ---------
def main():
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required.")
    if not BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET is required.")

    # fetch
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No candidates (images_urls_daily) for given window/limits.")
        return

    # Pre-run dedup: v rámci výsledku (pk, image_url) & z toho vypočítať uniq rank poradie
    cand = (
        cand
        .sort_values(["pk", "image_rank"], kind="stable")
        .drop_duplicates(subset=["pk", "image_url"], keep="first")
    )

    # per-run dedup na gcs_path
    seen_paths = set()
    out_rows: List[Dict[str, Any]] = []

    for _, r in cand.iterrows():
        row = {
            "pk": r.get("pk"),
            "listing_id": r.get("listing_id"),
            "seq_global": r.get("seq_global"),
            "url": r.get("url"),
            "image_url": r.get("image_url"),
            "image_rank": r.get("image_rank"),
        }

        # deterministický gcs_path aby sme vedeli dopredu, či by sa duplicitne spracovalo
        folder = make_folder(safe_int(row["seq_global"]), row["listing_id"])
        filename = f"{safe_int(row['image_rank']) or 0:03d}.webp"  # dočasne (skutočná ext sa ustáli po GET)
        planned_path = f"{BASE_PREFIX}/{folder}/{filename}"
        if planned_path in seen_paths:
            # preskoč (duplicitná rada)
            continue
        seen_paths.add(planned_path)

        out = download_and_upload(row, BASE_PREFIX)
        out_rows.append(out)
        # jemná pauza, nech to nie je úplný DDoS
        time.sleep(random.uniform(0.05, 0.15))

    write_results(out_rows)


if __name__ == "__main__":
    main()
