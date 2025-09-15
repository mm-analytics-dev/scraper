# -*- coding: utf-8 -*-
import os
import re
import sys
import time
import random
from datetime import datetime, timezone, date
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# ---------------- Env ----------------
PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "")
BQ_DATASET  = os.environ.get("BQ_DATASET", "")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "EU")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "")
BATCH_LIMIT = int(os.environ.get("BATCH_LIMIT", "100"))

TABLE_STAGING = f"{PROJECT_ID}.{BQ_DATASET}.image_urls_staging"
TABLE_IMAGES  = f"{PROJECT_ID}.{BQ_DATASET}.images"

TODAY   = date.today()
DAYSTR  = TODAY.strftime("%Y%m%d")
PREFIX  = f"images_v2/{DAYSTR}"

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]

IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".jfif", ".gif")
IMG_BAD_HINTS    = ("data:image/", "beacon", "pixel", "ads", "analytics")

CT2EXT = {
    "image/jpeg": ".jpg",
    "image/jpg":  ".jpg",
    "image/webp": ".webp",
    "image/png":  ".png",
    "image/gif":  ".gif",
    "image/avif": ".avif",
}

# --------------- utils ---------------
def log(msg: str): print(msg, flush=True)

def require_env():
    missing = [k for k,v in {
        "GCP_PROJECT_ID":PROJECT_ID,
        "BQ_DATASET":BQ_DATASET,
        "GCS_BUCKET":GCS_BUCKET
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")

def new_session():
    s = requests.Session()
    retry = Retry(
        total=4, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=frozenset(["GET","HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def looks_bad(url: str) -> bool:
    u = (url or "").lower()
    return (not u) or any(b in u for b in IMG_BAD_HINTS)

def ext_from_ct_or_url(ct: str, url: str) -> str:
    ct = (ct or "").split(";")[0].strip().lower()
    if ct in CT2EXT: return CT2EXT[ct]
    path = urlparse(url).path.lower()
    for suf in IMG_SUFFIX_ALLOW:
        if path.endswith(suf): return suf
    return ".jpg"

# --------------- BigQuery ---------------
def ensure_tables(bq: bigquery.Client):
    # staging
    try: bq.get_table(TABLE_STAGING)
    except NotFound:
        schema = [
            bigquery.SchemaField("seq_global","INTEGER"),
            bigquery.SchemaField("listing_id","STRING"),
            bigquery.SchemaField("image_url","STRING"),
            bigquery.SchemaField("source_url","STRING"),
            bigquery.SchemaField("created_at","TIMESTAMP"),
        ]
        t = bigquery.Table(TABLE_STAGING, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="created_at")
        bq.create_table(t); log(f"Created table {TABLE_STAGING}")

    # images
    try: bq.get_table(TABLE_IMAGES)
    except NotFound:
        schema = [
            bigquery.SchemaField("seq_global","INTEGER"),
            bigquery.SchemaField("listing_id","STRING"),
            bigquery.SchemaField("image_url","STRING"),
            bigquery.SchemaField("gcs_uri","STRING"),
            bigquery.SchemaField("content_type","STRING"),
            bigquery.SchemaField("size_bytes","INTEGER"),
            bigquery.SchemaField("downloaded_at","TIMESTAMP"),
        ]
        bq.create_table(bigquery.Table(TABLE_IMAGES, schema=schema))
        log(f"Created table {TABLE_IMAGES}")

def fetch_urls_to_download(bq: bigquery.Client, limit: int) -> list[dict]:
    sql = f"""
    SELECT s.seq_global, s.listing_id, s.image_url
    FROM `{TABLE_STAGING}` s
    LEFT JOIN `{TABLE_IMAGES}` i
      ON s.image_url = i.image_url
    WHERE i.image_url IS NULL
    GROUP BY s.seq_global, s.listing_id, s.image_url
    LIMIT @lim
    """
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("lim","INT64", limit)]
    )
    job = bq.query(sql, job_config=cfg, location=BQ_LOCATION)  # location sem
    return [dict(r) for r in job.result()]

def insert_images(bq: bigquery.Client, rows: list[dict]):
    if not rows: return
    errs = bq.insert_rows_json(TABLE_IMAGES, rows)
    if errs: log(f"[WARN] BQ insert errors: {errs}")

# --------------- GCS ---------------
def next_index_for_listing(sto: storage.Client, bucket_name: str, prefix: str) -> int:
    # spočíta existujúcich 3-ciferných súborov v priečinku
    blobs = list(sto.list_blobs(bucket_name, prefix=prefix))
    return len([b for b in blobs if b.name and re.search(r"/\d{3}\.", b.name)]) + 1

def stream_to_gcs(session: requests.Session, url: str, bucket: storage.Bucket, object_name: str) -> tuple[str, str, int] | tuple[None, str, int]:
    headers = {
        "User-Agent": random.choice(UAS),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": "https://www.nehnutelnosti.sk/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    with session.get(url, headers=headers, stream=True, timeout=30) as r:
        if r.status_code != 200:
            return None, f"http_{r.status_code}", 0
        ct = (r.headers.get("Content-Type") or "").lower()
        if not ct.startswith("image/"):
            return None, f"not_image:{ct or 'unknown'}", 0

        # malé obrázky/pixely preskoč
        try:
            cl = int(r.headers.get("Content-Length") or 0)
        except:  # noqa
            cl = 0
        if cl and cl < 3000:
            return None, "too_small", cl

        blob = bucket.blob(object_name)
        # stream zápis do GCS (bez držania v RAM)
        with blob.open("wb", content_type=ct) as f:
            for chunk in r.iter_content(256 * 1024):
                if chunk:
                    f.write(chunk)

        # veľkosť – ak content-length chýba, dočítame zo statov
        size_bytes = cl if cl else blob.size or 0
        return f"gs://{bucket.name}/{object_name}", ct, int(size_bytes)

# --------------- Main ---------------
def main():
    require_env()
    bq  = bigquery.Client(project=PROJECT_ID)
    sto = storage.Client(project=PROJECT_ID)
    ensure_tables(bq)

    batch = fetch_urls_to_download(bq, BATCH_LIMIT)
    if not batch:
        log("No new image URLs to download. Done.")
        return

    s = new_session()
    bucket = sto.bucket(GCS_BUCKET)

    done, skipped, failed = 0, 0, 0
    out_rows = []

    for row in batch:
        seq        = row.get("seq_global")
        listing_id = (row.get("listing_id") or "").strip() or None
        img_url    = (row.get("image_url") or "").strip()

        if looks_bad(img_url):
            skipped += 1
            continue

        folder = listing_id if listing_id else str(seq or "NA")
        prefix_listing = f"{PREFIX}/{folder}/"
        idx = next_index_for_listing(sto, GCS_BUCKET, prefix_listing)

        # ext z Content-Type alebo URL
        # (zistí sa až po requeste; object_name poskladáme po fetchnutí)
        # preto najprv načítame, až potom priradíme koncovku
        # => spravíme "dočasný" object_name a po získaní ct ho finalizujeme
        tmp_object_name = f"{prefix_listing}{idx:03d}.bin"

        gcs_uri, ct, size = stream_to_gcs(s, img_url, bucket, tmp_object_name)
        if not gcs_uri:
            failed += 1
            log(f"[SKIP] {img_url} ({ct})")
            # zmaž prípadné prázdne .bin
            try: bucket.blob(tmp_object_name).delete()
            except Exception: pass
            continue

        # ak bola .bin, premenuj na správnu koncovku
        final_ext = ext_from_ct_or_url(ct, img_url)
        final_object_name = f"{prefix_listing}{idx:03d}{final_ext}"
        if final_object_name != tmp_object_name:
            try:
                bucket.rename_blob(bucket.blob(tmp_object_name), new_name=final_object_name)
                gcs_uri = f"gs://{GCS_BUCKET}/{final_object_name}"
            except Exception:
                # rename zlyhal – nechajme pôvodný názov
                final_object_name = tmp_object_name

        out_rows.append({
            "seq_global": int(seq) if seq is not None else None,
            "listing_id": listing_id,
            "image_url": img_url,
            "gcs_uri": gcs_uri,
            "content_type": ct,
            "size_bytes": size,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        })
        done += 1
        time.sleep(0.15)

    insert_images(bq, out_rows)
    log(f"Finished. downloaded={done}, skipped={skipped}, failed={failed}")
    if done:
        log(f"Example prefix: gs://{GCS_BUCKET}/{PREFIX}/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[FATAL] {e}")
        sys.exit(1)
