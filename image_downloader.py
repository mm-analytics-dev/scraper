# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import uuid
import json
import random
from datetime import datetime, timezone, date
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

# ---------------- Env & constants ----------------

PROJECT_ID   = os.environ.get("GCP_PROJECT_ID") or ""
BQ_DATASET   = os.environ.get("BQ_DATASET") or ""  # napr. realestate_v2
BQ_LOCATION  = os.environ.get("BQ_LOCATION") or "EU"
GCS_BUCKET   = os.environ.get("GCS_BUCKET") or ""  # napr. realestate-images-eu
BATCH_LIMIT  = int(os.environ.get("BATCH_LIMIT", "100"))

TABLE_STAGING = f"{PROJECT_ID}.{BQ_DATASET}.image_urls_staging"
TABLE_IMAGES  = f"{PROJECT_ID}.{BQ_DATASET}.images"

TODAY = date.today()
DAYSTR = TODAY.strftime("%Y%m%d")
PREFIX_ROOT = f"images_v2/{DAYSTR}"

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".jfif", ".gif")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics")


def log(msg: str):
    print(msg, flush=True)


def fail_if_missing():
    missing = []
    if not PROJECT_ID:  missing.append("GCP_PROJECT_ID")
    if not BQ_DATASET:  missing.append("BQ_DATASET")
    if not GCS_BUCKET:  missing.append("GCS_BUCKET")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


# ---------------- HTTP session ----------------

def new_session():
    s = requests.Session()
    retry = Retry(
        total=4, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch_image(session: requests.Session, url: str) -> tuple[bytes, str] | tuple[None, str]:
    """
    Return (content, content_type) or (None, reason)
    """
    if not url or any(b in url.lower() for b in IMG_BAD_HINTS):
        return None, "bad_url"

    headers = {
        "User-Agent": random.choice(UAS),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": "https://www.nehnutelnosti.sk/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    r = session.get(url, headers=headers, timeout=25)
    ct = (r.headers.get("Content-Type") or "").lower()
    if r.status_code != 200:
        return None, f"http_{r.status_code}"
    if not ct.startswith("image/"):
        return None, f"not_image:{ct or 'unknown'}"

    content = r.content or b""
    if len(content) < 3000:
        return None, "too_small"

    return content, ct


# ---------------- BQ helpers ----------------

def ensure_tables(bq: bigquery.Client):
    """
    Vytvorí tabuľky, ak neexistujú.
    - image_urls_staging: {seq_global INT64, listing_id STRING, image_url STRING, source_url STRING, created_at TIMESTAMP}
    - images: {seq_global INT64, listing_id STRING, image_url STRING, gcs_uri STRING, content_type STRING, size_bytes INT64, downloaded_at TIMESTAMP}
    """
    # staging
    try:
        bq.get_table(TABLE_STAGING)
    except NotFound:
        schema = [
            bigquery.SchemaField("seq_global", "INTEGER"),
            bigquery.SchemaField("listing_id", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("source_url", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(TABLE_STAGING, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="created_at")
        bq.create_table(table)
        log(f"Created table {TABLE_STAGING}")

    # images
    try:
        bq.get_table(TABLE_IMAGES)
    except NotFound:
        schema = [
            bigquery.SchemaField("seq_global", "INTEGER"),
            bigquery.SchemaField("listing_id", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("gcs_uri", "STRING"),
            bigquery.SchemaField("content_type", "STRING"),
            bigquery.SchemaField("size_bytes", "INTEGER"),
            bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(TABLE_IMAGES, schema=schema)
        bq.create_table(table)
        log(f"Created table {TABLE_IMAGES}")


def fetch_batch_urls(bq: bigquery.Client, limit: int) -> list[dict]:
    """
    Vezme len URL, ktoré:
      - sú v stagingu
      - ešte nie sú v tabuľke images (podľa image_url)
    """
    sql = f"""
    SELECT
      s.seq_global,
      s.listing_id,
      s.image_url
    FROM `{TABLE_STAGING}` AS s
    LEFT JOIN `{TABLE_IMAGES}` AS i
      ON s.image_url = i.image_url
    WHERE i.image_url IS NULL
    GROUP BY s.seq_global, s.listing_id, s.image_url
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    # >>> location patrí do client.query(...), nie do QueryJobConfig
    job = bq.query(sql, job_config=job_config, location=BQ_LOCATION)
    rows = list(job.result())
    return [dict(r) for r in rows]


def insert_images_rows(bq: bigquery.Client, rows: list[dict]):
    if not rows:
        return
    errors = bq.insert_rows_json(TABLE_IMAGES, rows)
    if errors:
        log(f"[WARN] BQ insert errors: {errors}")


# ---------------- GCS helpers ----------------

CT2EXT = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/webp": ".webp",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/avif": ".avif",
}

def guess_ext(ct: str, url: str) -> str:
    ct = (ct or "").split(";")[0].strip().lower()
    if ct in CT2EXT:
        return CT2EXT[ct]
    path = urlparse(url).path.lower()
    for suf in IMG_SUFFIX_ALLOW:
        if path.endswith(suf):
            return suf
    return ".jpg"


def next_index_for_listing(sto: storage.Client, bucket_name: str, prefix: str) -> int:
    bucket = sto.bucket(bucket_name)
    blobs = list(sto.list_blobs(bucket, prefix=prefix))
    return len([b for b in blobs if b.name and re.search(r"/\d{3}\.", b.name)]) + 1


def upload_image(sto: storage.Client, bucket_name: str, object_name: str, content: bytes, content_type: str) -> str:
    bucket = sto.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(content, content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"


# ---------------- Main logic ----------------

def main():
    fail_if_missing()

    bq = bigquery.Client(project=PROJECT_ID)
    sto = storage.Client(project=PROJECT_ID)

    ensure_tables(bq)

    batch = fetch_batch_urls(bq, BATCH_LIMIT)
    if not batch:
        log("No new image URLs to download. Done.")
        return

    log(f"Downloading up to {len(batch)} images...")

    s = new_session()
    ok_rows = []
    done = 0
    skipped = 0
    failed = 0

    for row in batch:
        seq = row.get("seq_global")
        listing_id = (row.get("listing_id") or "").strip() or None
        img_url = (row.get("image_url") or "").strip()

        if not img_url:
            skipped += 1
            continue

        content, ct = fetch_image(s, img_url)
        if content is None:
            failed += 1
            log(f"[SKIP] {img_url} ({ct})")
            continue

        folder_id = listing_id if listing_id else str(seq or "NA")
        prefix_listing = f"{PREFIX_ROOT}/{folder_id}/"
        idx = next_index_for_listing(sto, GCS_BUCKET, prefix_listing)
        ext = guess_ext(ct, img_url)
        object_name = f"{prefix_listing}{idx:03d}{ext}"

        try:
            gcs_uri = upload_image(sto, GCS_BUCKET, object_name, content, ct)
        except Exception as e:
            failed += 1
            log(f"[ERR] upload {img_url} -> {e}")
            continue

        ok_rows.append({
            "seq_global": int(seq) if seq is not None else None,
            "listing_id": listing_id,
            "image_url": img_url,
            "gcs_uri": gcs_uri,
            "content_type": ct,
            "size_bytes": len(content),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        })
        done += 1
        time.sleep(0.2)

    insert_images_rows(bq, ok_rows)

    log(f"Finished. downloaded={done}, skipped={skipped}, failed={failed}")
    if done:
        log(f"Example GCS prefix: gs://{GCS_BUCKET}/{PREFIX_ROOT}/")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[FATAL] {e}")
        sys.exit(1)
