# image_downloader.py
# Sťahovanie obrázkov pre listings z BQ -> upload do GCS -> log do BQ
# Zdroj URL: {BQ_DATASET}.images_urls_daily (posledný batch na pk)

import os
import re
import io
import sys
import time
import random
import mimetypes
from datetime import datetime, timezone, date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd

# --- Google libs
from google.cloud import bigquery
from google.cloud import storage

# ========== ENV ==========
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET    = os.getenv("BQ_DATASET", "realestate_v2")
LOCATION   = os.getenv("BQ_LOCATION", "EU")
BUCKET     = os.getenv("GCS_BUCKET", "")

LOOKBACK_DAYS        = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS         = int(os.getenv("IMAGES_MAX_LISTINGS", "10"))      # <- obmedzenie na test
MAX_IMAGES_PER_LIST  = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE = os.getenv("BATCH_DATE", "")  # YYYYMMDD
if not BATCH_DATE:
    BATCH_DATE = datetime.utcnow().strftime("%Y%m%d")

# ========== HTTP ==========
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
ACCEPT_LANGS = [
    "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
    "cs-CZ,sk;q=0.9,en-US;q=0.7,en;q=0.6",
    "en-US,en;q=0.9,sk;q=0.8,cs;q=0.7",
]

IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".jfif", ".gif")

def rand_headers(extra=None, referer=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        h["Referer"] = referer
    if extra: 
        h.update(extra)
    if random.random() < 0.3:
        h["Connection"] = "close"
    return h

def new_session():
    s = requests.Session()
    retry = Retry(
        total=4, connect=3, read=3, backoff_factor=1.2,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=frozenset(["GET","HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

# ========== BQ helpers ==========
def ensure_images_table(client: bigquery.Client):
    """Create images table if not exists."""
    table_id = f"{PROJECT_ID}.{DATASET}.images"
    schema = [
        bigquery.SchemaField("pk", "STRING"),
        bigquery.SchemaField("listing_id", "STRING"),
        bigquery.SchemaField("seq_global", "INTEGER"),
        bigquery.SchemaField("source_url", "STRING"),
        bigquery.SchemaField("image_rank", "INTEGER"),
        bigquery.SchemaField("gcs_path", "STRING"),
        bigquery.SchemaField("byte_size", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("error", "STRING"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
        bigquery.SchemaField("batch_ts", "TIMESTAMP"),
    ]
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id}")

def fetch_latest_image_urls(client: bigquery.Client) -> pd.DataFrame:
    """Fetch last batch of image URLs per pk within lookback window."""
    sql = f"""
    DECLARE lookback_days INT64 DEFAULT {LOOKBACK_DAYS};
    WITH latest AS (
      SELECT pk, MAX(batch_ts) AS max_ts
      FROM `{PROJECT_ID}.{DATASET}.images_urls_daily`
      WHERE DATE(batch_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      GROUP BY pk
    )
    SELECT i.pk, i.listing_id, i.seq_global, i.url AS listing_url,
           i.image_url, i.image_rank, i.batch_ts
    FROM `{PROJECT_ID}.{DATASET}.images_urls_daily` i
    JOIN latest l USING (pk)
    WHERE i.batch_ts = l.max_ts
    ORDER BY i.seq_global, i.image_rank
    """
    job = client.query(sql, location=LOCATION)
    df = job.result().to_dataframe(create_bqstorage_client=False)
    return df

# ========== GCS helpers ==========
def gcs_blob_exists(storage_client: storage.Client, bucket_name: str, blob_name: str) -> bool:
    b = storage_client.bucket(bucket_name).blob(blob_name)
    try:
        return b.exists()
    except Exception:
        return False

def upload_bytes_to_gcs(storage_client: storage.Client, bucket_name: str, blob_name: str, data: bytes, content_type: str | None):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(io.BytesIO(data), size=len(data), content_type=content_type)
    return f"gs://{bucket_name}/{blob_name}", len(data)

# ========== core ==========
def ext_from_url(url: str) -> str:
    low = (url or "").lower()
    for e in IMG_SUFFIX_ALLOW:
        if low.endswith(e):
            return e
    # fallback: try to guess from query or Content-Type later
    return ".webp"

def content_type_guess(url: str, headers: requests.structures.CaseInsensitiveDict) -> str | None:
    ct = headers.get("Content-Type")
    if ct:
        ct = ct.split(";")[0].strip()
        return ct
    # fallback from extension
    ex = ext_from_url(url)
    guess = mimetypes.types_map.get(ex)
    return guess

def main():
    if not PROJECT_ID or not DATASET or not BUCKET:
        print("[FATAL] Missing GCP_PROJECT_ID / BQ_DATASET / GCS_BUCKET env.")
        sys.exit(1)

    bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    st = storage.Client(project=PROJECT_ID)
    ensure_images_table(bq)

    # 1) fetch last-batch URLs
    df = fetch_latest_image_urls(bq)
    if df.empty:
        print("No URLs in images_urls_daily for the recent window. Done.")
        return

    # 2) obmedz počet inzerátov (pk)
    #    vezmeme DISTINCT pk v poradí podľa seq_global a potom filterujeme df
    distinct_pk = (df[["pk","seq_global","listing_id"]]
                   .drop_duplicates(subset=["pk"])
                   .sort_values(["seq_global","pk"]))
    if MAX_LISTINGS > 0:
        keep_pks = set(distinct_pk.head(MAX_LISTINGS)["pk"].tolist())
        df = df[df["pk"].isin(keep_pks)].copy()

    # 3) stiahnuť a nahrať
    session = new_session()
    total_saved = 0
    records_for_bq = []

    # skupiny po pk, aby sme vedeli uplatniť MAX_IMAGES_PER_LIST
    for pk, group in df.groupby("pk", sort=False):
        group = group.sort_values("image_rank")
        # meta
        seq_global = int(group["seq_global"].dropna().iloc[0]) if pd.notna(group["seq_global"].iloc[0]) else None
        listing_id = str(group["listing_id"].dropna().iloc[0]) if pd.notna(group["listing_id"].iloc[0]) else "NA"

        # gcs prefix
        prefix = f"images_v2/{BATCH_DATE}/{seq_global:06d}_{listing_id}/" if seq_global is not None else f"images_v2/{BATCH_DATE}/NA_{listing_id}/"

        downloaded_for_pk = 0
        listing_url = str(group["listing_url"].iloc[0]) if "listing_url" in group.columns else None

        for _, row in group.iterrows():
            if downloaded_for_pk >= MAX_IMAGES_PER_LIST:
                break

            img_url = row["image_url"]
            rank    = int(row["image_rank"]) if not pd.isna(row["image_rank"]) else (downloaded_for_pk + 1)

            # destination blob
            ext = ext_from_url(img_url)
            blob_name = f"{prefix}{rank:03d}{ext}"

            # skip if exists
            if gcs_blob_exists(st, BUCKET, blob_name):
                # already there → nezapisuj ešte raz do images (aby nevznikali duplicitné riadky)
                continue

            try:
                r = session.get(
                    img_url,
                    headers=rand_headers(
                        referer=listing_url,
                        extra={"Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"}
                    ),
                    timeout=25
                )
                if r.status_code != 200 or not r.content or len(r.content) < 3000:
                    print(f"[IMG] skip ({r.status_code}, {len(r.content) if r.content else 0} B) {img_url}")
                    continue

                ct = content_type_guess(img_url, r.headers)
                gcs_path, byte_size = upload_bytes_to_gcs(st, BUCKET, blob_name, r.content, ct)

                records_for_bq.append({
                    "pk": pk,
                    "listing_id": listing_id,
                    "seq_global": int(seq_global) if seq_global is not None else None,
                    "source_url": img_url,
                    "image_rank": rank,
                    "gcs_path": gcs_path,
                    "byte_size": int(byte_size),
                    "status": "ok",
                    "error": None,
                    "downloaded_at": datetime.utcnow().isoformat(),
                    "batch_ts": datetime.utcnow().isoformat(),
                })
                downloaded_for_pk += 1
                total_saved += 1
                print(f"[OK] {gcs_path}")

                # jemná pauza
                time.sleep(random.uniform(0.3, 1.0))

            except Exception as e:
                print(f"[ERR] {img_url} → {e}")
                records_for_bq.append({
                    "pk": pk,
                    "listing_id": listing_id,
                    "seq_global": int(seq_global) if seq_global is not None else None,
                    "source_url": img_url,
                    "image_rank": rank,
                    "gcs_path": None,
                    "byte_size": None,
                    "status": "error",
                    "error": str(e)[:500],
                    "downloaded_at": datetime.utcnow().isoformat(),
                    "batch_ts": datetime.utcnow().isoformat(),
                })

    # 4) zapíš do BQ.images (append)
    if records_for_bq:
        df_out = pd.DataFrame.from_records(records_for_bq)
        # pandas-gbq je najjednoduchšie; fallback na load_table_from_dataframe ak chceš
        try:
            import pandas_gbq
            pandas_gbq.to_gbq(
                df_out, f"{DATASET}.images", project_id=PROJECT_ID,
                if_exists="append", location=LOCATION, progress_bar=False
            )
        except Exception as e:
            # fallback cez native BQ
            table_id = f"{PROJECT_ID}.{DATASET}.images"
            job = bq.load_table_from_dataframe(df_out, table_id, location=LOCATION)
            job.result()

    if total_saved == 0:
        print("No new image URLs to download (after filters / existing GCS). Done.")
    else:
        print(f"Done. Uploaded {total_saved} images to gs://{BUCKET}/images_v2/{BATCH_DATE}/")

if __name__ == "__main__":
    main()
