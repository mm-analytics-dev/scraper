# -*- coding: utf-8 -*-
"""
Image downloader job (v2)
- Source: BigQuery table with {listing_id, url[, seq_global]}
- Target: GCS bucket (prefix images_v2/YYYYMMDD/<listing_id>/)
- Log:   BigQuery table <dataset>.images_downloaded_daily

Env vars (set in GitHub Actions):
  GOOGLE_APPLICATION_CREDENTIALS  -> path to SA json
  GCP_PROJECT_ID                  -> GCP project
  BQ_DATASET                      -> fallback dataset name
  BQ_DATASET_V2                   -> preferred dataset v2 (if set)
  BQ_LOCATION                     -> e.g. EU
  GCS_BUCKET                      -> destination bucket

Optional envs:
  MAX_IMAGES                      -> int limit for testing
  HTTP_TIMEOUT                    -> default 25
  MIN_BYTES                       -> default 3_000  (skip tiny pixels)
  MAX_BYTES                       -> default 15_000_000 (15MB)

Expected source table names (first that exists will be used):
  images_urls_daily
  images_urls_v2
  image_urls_daily
  image_urls_v2
"""

import os
import re
import sys
import time
import json
import random
import logging
import hashlib
from datetime import datetime, date

import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound, Conflict

# ------------- Config & utils -------------
PROJECT   = os.environ.get("GCP_PROJECT_ID") or ""
DATASET   = os.environ.get("BQ_DATASET_V2") or os.environ.get("BQ_DATASET") or "realestate_v2"
LOCATION  = os.environ.get("BQ_LOCATION") or "EU"
BUCKET    = os.environ.get("GCS_BUCKET") or ""
RUN_ID    = os.environ.get("GITHUB_RUN_ID") or datetime.utcnow().strftime("%Y%m%d%H%M%S")
TODAY     = date.today().strftime("%Y%m%d")

HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "25"))
MIN_BYTES    = int(os.environ.get("MIN_BYTES", "3000"))
MAX_BYTES    = int(os.environ.get("MAX_BYTES", str(15_000_000)))
MAX_IMAGES   = int(os.environ.get("MAX_IMAGES", "0"))  # 0 = no limit

SRC_TABLE_CANDIDATES = [
    "images_urls_daily",
    "images_urls_v2",
    "image_urls_daily",
    "image_urls_v2",
]

# minimal logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("img-dl")

IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif", ".jfif")
BAD_HINTS = ("pixel", "beacon", "ads", "analytics")
ACCEPT_HDR = "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"

def guess_ext(url: str, content_type: str | None) -> str:
    u = (url or "").lower()
    for suf in IMG_SUFFIX_ALLOW:
        if u.endswith(suf):
            return suf
    if not content_type:
        return ".jpg"
    ct = content_type.lower()
    if "jpeg" in ct:
        return ".jpg"
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    if "avif" in ct:
        return ".avif"
    if "gif" in ct:
        return ".gif"
    return ".jpg"

def is_probably_bad(url: str) -> bool:
    u = (url or "").lower()
    if any(b in u for b in BAD_HINTS):
        return True
    return False

def pick_source_table(bq: bigquery.Client) -> str:
    for name in SRC_TABLE_CANDIDATES:
        tbl = f"{PROJECT}.{DATASET}.{name}"
        try:
            bq.get_table(tbl)
            log.info(f"Using source table: {tbl}")
            return name
        except NotFound:
            continue
    raise RuntimeError(
        f"No source table found in {PROJECT}.{DATASET}. "
        f"Tried: {', '.join(SRC_TABLE_CANDIDATES)}"
    )

def ensure_log_table(bq: bigquery.Client, table_id: str):
    schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("uploaded_at", "TIMESTAMP"),
        bigquery.SchemaField("listing_id", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("gcs_path", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("http_status", "INT64"),
        bigquery.SchemaField("bytes", "INT64"),
        bigquery.SchemaField("content_type", "STRING"),
        bigquery.SchemaField("sha1", "STRING"),
    ]
    table_ref = bigquery.Table(table_id, schema=schema)
    try:
        bq.get_table(table_id)
    except NotFound:
        log.info(f"Creating log table {table_id}")
        bq.create_table(table_ref)
    except Exception as e:
        raise

# ------------- Main downloader -------------
def main():
    if not PROJECT or not BUCKET:
        log.error("Missing GCP_PROJECT_ID or GCS_BUCKET env.")
        sys.exit(2)

    bq = bigquery.Client(project=PROJECT, location=LOCATION)
    gcs = storage.Client(project=PROJECT)
    bucket = gcs.bucket(BUCKET)

    src_table = pick_source_table(bq)

    sql = f"""
    SELECT
      CAST(listing_id AS STRING) AS listing_id,
      CAST(url AS STRING)         AS url,
      CAST(seq_global AS INT64)   AS seq_global
    FROM `{PROJECT}.{DATASET}.{src_table}`
    WHERE url IS NOT NULL AND url != ''
    """
    df = bq.query(sql).result().to_dataframe(create_bqstorage_client=True)
    if df.empty:
        log.info("No image urls found, exiting.")
        return

    # Drop obvious bads & NaNs
    df = df.dropna(subset=["listing_id", "url"])
    df = df[~df["url"].str.contains(r"^data:image", na=False)]
    df = df[~df["url"].apply(is_probably_bad)]

    # Index per listing for deterministic file names
    df["img_idx"] = df.groupby("listing_id").cumcount() + 1

    if MAX_IMAGES and MAX_IMAGES > 0:
        df = df.head(MAX_IMAGES)

    # Prepare log table
    log_table = f"{PROJECT}.{DATASET}.images_downloaded_daily"
    ensure_log_table(bq, log_table)

    s = requests.Session()
    s.headers.update({
        "Accept": ACCEPT_HDR,
        "User-Agent": "Mozilla/5.0 (img-downloader/2.0; +github-actions)"
    })

    records = []
    processed = 0

    for r in df.itertuples(index=False):
        listing_id = str(r.listing_id or "NA")
        url        = str(r.url or "")
        idx        = int(getattr(r, "img_idx", 1))

        if not url or url.startswith("data:"):
            continue

        # GCS path: images_v2/YYYYMMDD/<listing_id>/<idx>.<ext>
        # Keep it stable per day; if exists -> skip
        subdir = f"images_v2/{TODAY}/{listing_id}"
        # Temporary ext guess, final after response
        ext_hint = guess_ext(url, None)
        object_name = f"{subdir}/{idx:03d}{ext_hint}"

        blob = bucket.blob(object_name)
        if blob.exists():
            log.debug(f"SKIP exists: gs://{BUCKET}/{object_name}")
            records.append({
                "run_id": RUN_ID,
                "uploaded_at": datetime.utcnow().isoformat(),
                "listing_id": listing_id,
                "url": url,
                "gcs_path": f"gs://{BUCKET}/{object_name}",
                "status": "exists",
                "http_status": 0,
                "bytes": int(blob.size or 0),
                "content_type": blob.content_type or None,
                "sha1": None,
            })
            continue

        try:
            resp = s.get(url, stream=True, timeout=HTTP_TIMEOUT)
            http_status = resp.status_code
            if http_status != 200:
                log.warning(f"HTTP {http_status} -> {url}")
                records.append({
                    "run_id": RUN_ID,
                    "uploaded_at": datetime.utcnow().isoformat(),
                    "listing_id": listing_id,
                    "url": url,
                    "gcs_path": None,
                    "status": "http_error",
                    "http_status": http_status,
                    "bytes": None,
                    "content_type": None,
                    "sha1": None,
                })
                continue

            # Read up to MAX_BYTES
            content = resp.content
            ctype = resp.headers.get("Content-Type", "")
            size = len(content)
            if size < MIN_BYTES or size > MAX_BYTES:
                log.warning(f"Size out of range ({size} B) -> {url}")
                records.append({
                    "run_id": RUN_ID,
                    "uploaded_at": datetime.utcnow().isoformat(),
                    "listing_id": listing_id,
                    "url": url,
                    "gcs_path": None,
                    "status": "bad_size",
                    "http_status": http_status,
                    "bytes": size,
                    "content_type": ctype,
                    "sha1": None,
                })
                continue

            # Correct ext by content-type if needed
            ext_final = guess_ext(url, ctype)
            if not object_name.endswith(ext_final):
                object_name = f"{subdir}/{idx:03d}{ext_final}"
                blob = bucket.blob(object_name)

            sha1 = hashlib.sha1(content).hexdigest()

            # Upload
            blob.upload_from_string(content, content_type=ctype or "application/octet-stream")
            log.info(f"UPLOADED gs://{BUCKET}/{object_name} ({size/1024:.1f} KB)")

            records.append({
                "run_id": RUN_ID,
                "uploaded_at": datetime.utcnow().isoformat(),
                "listing_id": listing_id,
                "url": url,
                "gcs_path": f"gs://{BUCKET}/{object_name}",
                "status": "uploaded",
                "http_status": http_status,
                "bytes": size,
                "content_type": ctype,
                "sha1": sha1,
            })

        except Exception as e:
            log.error(f"Download/upload error: {e} | url={url}")
            records.append({
                "run_id": RUN_ID,
                "uploaded_at": datetime.utcnow().isoformat(),
                "listing_id": listing_id,
                "url": url,
                "gcs_path": None,
                "status": "error",
                "http_status": None,
                "bytes": None,
                "content_type": None,
                "sha1": None,
            })

        processed += 1
        # gentle pacing
        time.sleep(random.uniform(0.15, 0.4))

    if not records:
        log.info("Nothing processed.")
        return

    # Write log to BQ
    out_df = pd.DataFrame.from_records(records)
    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(out_df, log_table, job_config=job_cfg)
    job.result()

    log.info(f"Finished. Processed={processed}, uploaded={sum(r['status']=='uploaded' for r in records)}, "
             f"exists={sum(r['status']=='exists' for r in records)}")

if __name__ == "__main__":
    main()

