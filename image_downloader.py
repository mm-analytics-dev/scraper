# -*- coding: utf-8 -*-
"""
Images downloader (v2) -> GCS + BigQuery (PK -> GCS URL)

Čo robí:
- Z BQ tab. {BQ_DATASET}.images_urls_daily vyberie kandidátov (za IMAGES_LOOKBACK_DAYS),
  zoberie max IMAGES_MAX_LISTINGS inzerátov a max IMAGES_MAX_PER_LISTING fotiek na inzerát.
- Stiahne obrázky do GCS prefixu: images_v2/YYYYMMDD/{FOLDER}/{NNN.ext}
  (YYYYMMDD = dnešný UTC alebo BATCH_DATE=YYYYMMDD).
- Zapíše do BQ tabuľky {BQ_DATASET}.{IMAGES_PK_GCS_TABLE} už len:
    pk STRING, gcs_url STRING, downloaded_at TIMESTAMP, batch_date DATE
  (t.j. jedna URL na GCS na riadok; rovnaký pk sa môže opakovať pre viac fotiek).
- "Nikdy duplicitne": dedup podľa existencie v GCS + pred zápisom do BQ vynechá už existujúce (pk,gcs_url).

ENV:
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=3
  IMAGES_MAX_LISTINGS=15
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD (optional)
  IMAGES_PK_GCS_TABLE=images_pk_gcs (optional; default)
"""

import os
import re
import time
import json
import math
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

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
IMAGES_PK_GCS_TABLE = os.getenv("IMAGES_PK_GCS_TABLE", "images_pk_gcs").strip()

# --------- Batch date / prefixes ---------
def today_yyyymmdd_utc() -> str:
    if BATCH_DATE_ENV:
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = today_yyyymmdd_utc()
BASE_PREFIX = f"images_v2/{BATCH_YYYYMMDD}"

def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)

# --------- HTTP session with retries ---------
SESSION = requests.Session()
_retry = Retry(
    total=4, connect=3, read=3, backoff_factor=1.2,
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
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID or None)
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

# --------- SQL pre kandidátov ---------
SQL_CANDIDATES = """
WITH recent AS (
  SELECT
    pk,
    listing_id,
    seq_global,
    url,
    image_url,
    image_rank,
    batch_ts
  FROM `{{PROJECT_ID}}.{{DATASET}}.images_urls_daily`
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
""".replace("{{PROJECT_ID}}", PROJECT_ID).replace("{{DATASET}}", DATASET)

def query_df(sql: str, params: List[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_cfg, location=LOCATION or None)
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
    if not s: return "na"
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "na"

def safe_int(x):
    try:
        if pd.isna(x): return None
        return int(x)
    except Exception:
        return None

def make_folder(seq_global: Optional[int], listing_id: Optional[str]) -> str:
    if seq_global is not None:
        if listing_id:
            return f"{seq_global:06d}_{slugify(str(listing_id))}"
        return f"{seq_global:06d}"
    return slugify(listing_id) if listing_id else "na"

def ext_from_response(url: str, resp: requests.Response) -> str:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/webp" in ctype: return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype: return ".jpg"
    if "image/png" in ctype: return ".png"
    if "image/avif" in ctype: return ".avif"
    if "image/gif" in ctype: return ".gif"
    u = url.lower().split("?", 1)[0]
    for suf in (".webp", ".jpg", ".jpeg", ".png", ".avif", ".gif"):
        if u.endswith(suf): return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def gcs_blob_exists(path: str) -> bool:
    if not bucket: return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

# --------- Download + upload ---------
def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text)
    """
    folder = make_folder(seq_global, listing_id)

    try:
        resp = SESSION.get(image_url, timeout=30, headers=rand_headers(), stream=True)
        status = int(resp.status_code)
    except Exception as e:
        return ("", None, f"request_error:{e}")

    if status != 200:
        ctype = resp.headers.get("Content-Type")
        try:
            resp.close()
        except Exception:
            pass
        return ("", status, f"http_{status} ({ctype})")

    ext = ext_from_response(image_url, resp)
    filename = f"{rank:03d}{ext}"
    gcs_path = f"{BASE_PREFIX}/{folder}/{filename}"

    # Duplicitný upload = preskočiť
    if gcs_blob_exists(gcs_path):
        try:
            _ = resp.content
        except Exception:
            pass
        finally:
            resp.close()
        return (gcs_path, 200, None)

    # Upload
    try:
        content = resp.content
        ctype = resp.headers.get("Content-Type")
    finally:
        resp.close()

    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=ctype)
        print(f"[OK] gs://{BUCKET_NAME}/{gcs_path}", flush=True)
        return (gcs_path, 200, None)
    except Exception as e:
        return ("", 200, f"gcs_upload_error:{e}")

# --------- BQ: ensure table + read existing pairs + insert new ---------
def ensure_pk_gcs_table(table_id: str):
    schema = [
        bigquery.SchemaField("pk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_date", "DATE", mode="NULLABLE"),
    ]
    try:
        bq.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        bq.create_table(table)
        print(f"[BQ] Created table {table_id}.")

def fetch_existing_pairs(table_id: str, pks: List[str]) -> Set[Tuple[str, str]]:
    """
    Bezpečne a rýchlo: WHERE pk IN UNNEST(@pks)
    """
    if not pks:
        return set()
    sql = f"""
    SELECT pk, gcs_url
    FROM `{table_id}`
    WHERE pk IN UNNEST(@pks)
    """
    params = [
        bigquery.ArrayQueryParameter("pks", "STRING", pks),
    ]
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    try:
        df = bq.query(sql, job_config=job_cfg, location=LOCATION or None).result().to_dataframe()
        return set((str(r["pk"]), str(r["gcs_url"])) for _, r in df.iterrows())
    except Exception as e:
        print(f"[WARN] fetch_existing_pairs failed (ignored): {e}")
        return set()

def insert_pk_gcs_rows(table_id: str, rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] Nothing to insert into pk->gcs table.")
        return
    df = pd.DataFrame(rows)
    df["downloaded_at"] = pd.to_datetime(df["downloaded_at"], utc=True)
    df["batch_date"] = pd.to_datetime(df["batch_date"]).dt.date
    job_cfg = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_cfg, location=LOCATION or None)
    job.result()
    print(f"[BQ] Inserted {len(df)} rows into {table_id}.")

# --------- Main ---------
def main():
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required.")
    if not BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET is required.")

    # 1) Načítaj kandidátov z images_urls_daily
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No candidates from images_urls_daily (check lookback/limits).")
        return

    # Stabilné poradie + dedup (pk, image_url) v rámci tohto behu
    cand = (
        cand
        .sort_values(["pk", "image_rank"], kind="stable")
        .drop_duplicates(subset=["pk", "image_url"], keep="first")
    )

    # 2) Sťahuj + uploaduj
    out_records: List[Dict[str, Any]] = []
    for _, r in cand.iterrows():
        pk = str(r.get("pk"))
        listing_id = r.get("listing_id")
        seq_global = safe_int(r.get("seq_global"))
        image_url = r.get("image_url")
        rank = safe_int(r.get("image_rank")) or 0

        if not image_url or not pk:
            continue

        gcs_path, http_status, err = download_to_gcs(image_url, seq_global, listing_id, rank)
        if gcs_path:
            out_records.append({
                "pk": pk,
                "gcs_url": f"gs://{BUCKET_NAME}/{gcs_path}",
                "downloaded_at": now_utc(),
                "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
            })
        else:
            print(f"[WARN] skip {pk} rank {rank}: {err or http_status}", flush=True)

        time.sleep(random.uniform(0.05, 0.15))

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    # 3) Dedup pred zápisom do BQ (nikdy duplicitne)
    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_PK_GCS_TABLE}"
    ensure_pk_gcs_table(table_id)

    pks = sorted({rec["pk"] for rec in out_records})
    existing = fetch_existing_pairs(table_id, pks)

    final_rows = [
        rec for rec in out_records
        if (rec["pk"], rec["gcs_url"]) not in existing
    ]

    # 4) Zapíš len nové páry (pk, gcs_url)
    insert_pk_gcs_rows(table_id, final_rows)

if __name__ == "__main__":
    main()
