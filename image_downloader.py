# image_downloader.py
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import random
import mimetypes
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
import pandas as pd

from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# ========= NATVRDO LIMITY (uprav tu podľa potreby) =========
IMAGES_LOOKBACK_DAYS   = 3     # koľko dní späť brať batch_ts z images_urls_daily
IMAGES_MAX_LISTINGS    = 15    # koľko inzerátov spracovať
IMAGES_MAX_PER_LISTING = 60    # max fotiek na 1 inzerát
# ===========================================================

# --------- ENV / konfigurácia GCP ----------
PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "")
DATASET      = os.getenv("BQ_DATASET", "")
LOCATION     = os.getenv("BQ_LOCATION", "EU")
BUCKET       = os.getenv("GCS_BUCKET", "")
BATCH_DATE   = os.getenv("BATCH_DATE")  # ak None -> dnešný UTC YYYYMMDD
PREFIX_ROOT  = "images_v2"              # gs://bucket/images_v2/yyyymmdd/...

if not PROJECT_ID or not DATASET or not BUCKET:
    print("[FATAL] Chýba GCP_PROJECT_ID alebo BQ_DATASET alebo GCS_BUCKET.", file=sys.stderr)
    sys.exit(1)

# --------- helpers (HTTP) ----------
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
ACCEPT_LANGS = [
    "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
    "cs-CZ,sk;q=0.9,en-US;q=0.7,en;q=0.6",
    "en-US,en;q=0.9,sk;q=0.8,cs;q=0.7",
]

def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "image/avif,image/webp,image/*;q=0.8,*/*;q=0.5",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
        "Connection": "close",
    }
    if extra:
        h.update(extra)
    return h

# --------- BQ kandidáti (dedup už v SQL) ----------
def fetch_candidates(client_bq, lookback_days, max_listings, max_per_listing) -> pd.DataFrame:
    sql = f"""
    DECLARE days INT64 DEFAULT @days;
    DECLARE maxL INT64 DEFAULT @max_listings;
    DECLARE maxP INT64 DEFAULT @max_per_listing;

    -- 1) zober posledné N dní, a vyber 1x (pk, image_url)
    WITH src AS (
      SELECT
        pk, listing_id, seq_global, url, image_url, image_rank, batch_ts,
        ROW_NUMBER() OVER (
          PARTITION BY pk, image_url
          ORDER BY batch_ts DESC, image_rank ASC
        ) AS rn
      FROM `{PROJECT_ID}.{DATASET}.images_urls_daily`
      WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL days DAY)
    ),
    uniq AS (
      SELECT pk, listing_id, seq_global, url, image_url, image_rank, batch_ts
      FROM src
      WHERE rn = 1
    ),
    -- 2) top N fotiek na inzerát podľa image_rank
    per_listing AS (
      SELECT
        u.*,
        ROW_NUMBER() OVER (PARTITION BY pk ORDER BY image_rank ASC) AS rnk
      FROM uniq u
    ),
    limited_per_listing AS (
      SELECT * FROM per_listing WHERE rnk <= maxP
    ),
    -- 3) obmedzíme počet inzerátov
    pick_listings AS (
      SELECT
        *, DENSE_RANK() OVER (ORDER BY seq_global ASC) AS listing_order
      FROM limited_per_listing
    )
    SELECT
      pk, listing_id, seq_global, url, image_url, rnk AS image_rank, batch_ts
    FROM pick_listings
    WHERE listing_order <= maxL
    ORDER BY seq_global, image_rank;
    """
    params = [
        bigquery.ScalarQueryParameter("days", "INT64", lookback_days),
        bigquery.ScalarQueryParameter("max_listings", "INT64", max_listings),
        bigquery.ScalarQueryParameter("max_per_listening", "INT64", max_per_listing),  # typo guard if used?
    ]
    # correct param name:
    params = [
        bigquery.ScalarQueryParameter("days", "INT64", lookback_days),
        bigquery.ScalarQueryParameter("max_listings", "INT64", max_listings),
        bigquery.ScalarQueryParameter("max_per_listings", "INT64", max_per_listing),
    ]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("days", "INT64", lookback_days),
            bigquery.ScalarQueryParameter("max_listings", "INT64", max_listings),
            bigquery.ScalarQueryParameter("max_per_listings", "INT64", max_per_listing),
        ]
    )
    job = client_bq.query(sql, job_config=job_config)
    return job.result().to_dataframe(create_bqstorage_client=False)

# --------- util: bezpečný názov priečinka ---------
SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_\-]+")

def safe_slug(s: str, default="NA") -> str:
    if not s:
        return default
    s = str(s)
    s = s.strip().replace(" ", "-")
    s = SAFE_CHARS_RE.sub("-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or default

# --------- util: odhad prípony podľa Content-Type/URL ---------
CT_TO_EXT = {
    "image/jpeg": ".jpg",
    "image/jpg":  ".jpg",
    "image/png":  ".png",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "image/gif":  ".gif",
}

def choose_ext(img_url: str, content_type: str | None) -> str:
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in CT_TO_EXT:
            return CT_TO_EXT[ct]
    # fallback z URL
    path = urlparse(img_url).path.lower()
    for e in (".jpg",".jpeg",".png",".webp",".avif",".jfif",".gif"):
        if path.endswith(e):
            return ".jpg" if e == ".jpeg" else e
    return ".webp"

# --------- BQ: vytvor/zarovnaj schému tabuľky images ----------
CANON_SCHEMA = [
    bigquery.SchemaField("pk",           "STRING"),
    bigquery.SchemaField("listing_id",   "STRING"),
    bigquery.SchemaField("seq_global",   "INT64"),
    bigquery.SchemaField("image_url",    "STRING"),
    bigquery.SchemaField("image_rank",   "INT64"),
    bigquery.SchemaField("gcs_path",     "STRING"),
    bigquery.SchemaField("content_type", "STRING"),
    bigquery.SchemaField("size_bytes",   "INT64"),
    bigquery.SchemaField("downloaded_at","TIMESTAMP"),
    # POZN: 'url' (detail URL) sem zámerne nedávame kvôli kompatibilite s existujúcou tabuľkou.
]

def ensure_table_and_align_df(client_bq: bigquery.Client, df: pd.DataFrame, table_id: str) -> pd.DataFrame:
    """Ak tabuľka neexistuje -> vytvor s CANON_SCHEMA.
       Ak existuje -> necháme pôvodnú schému a DF obmedzíme na prienik stĺpcov."""
    try:
        tbl = client_bq.get_table(table_id)
        existing_cols = [f.name for f in tbl.schema]
        # odfiltruj stĺpce, ktoré v tabuľke nie sú (napr. 'url')
        keep = [c for c in df.columns if c in existing_cols]
        df2 = df[keep].copy()
        return df2
    except NotFound:
        # vytvor novú tabuľku s kanonickou schémou
        dataset_id = ".".join(table_id.split(".")[:2])
        # dataset musí existovať; ak nie, vyhoďu sa zrozumiteľné chyby z API
        table = bigquery.Table(table_id, schema=CANON_SCHEMA)
        table.location = LOCATION
        client_bq.create_table(table)
        # DF obmedz na kanonické stĺpce
        keep = [f.name for f in CANON_SCHEMA if f.name in df.columns]
        df2 = df[keep].copy()
        return df2

# --------- MAIN ----------
def main():
    session = requests.Session()

    client_bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    client_gcs = storage.Client(project=PROJECT_ID)
    bucket = client_gcs.bucket(BUCKET)

    batch_date = BATCH_DATE or datetime.utcnow().strftime("%Y%m%d")
    prefix = f"{PREFIX_ROOT}/{batch_date}"

    # 1) kandidáti z BQ (dedup už v SQL)
    df = fetch_candidates(
        client_bq,
        IMAGES_LOOKBACK_DAYS,
        IMAGES_MAX_LISTINGS,
        IMAGES_MAX_PER_LISTING
    )

    if df.empty:
        print("No candidates found. Done.")
        return

    # 2) poistka proti duplicitám; re-rank v rámci pk 1..N
    df = df.drop_duplicates(subset=["pk", "image_url"], keep="first")
    df = df.sort_values(["seq_global", "pk", "image_rank"])
    df["image_rank"] = df.groupby("pk").cumcount() + 1

    rows_out = []  # audit pre BQ

    for _, row in df.iterrows():
        pk          = str(row.get("pk") or "")
        listing_id  = safe_slug(row.get("listing_id"), default="NA")
        seq_global  = int(row.get("seq_global") or 0)
        image_url   = str(row.get("image_url") or "")
        image_rank  = int(row.get("image_rank") or 0)
        detail_url  = row.get("url")  # môže byť None a tabuľka ho nemusí mať

        folder = f"{prefix}/{seq_global:06d}_{listing_id}"
        # najprv si odhadni ext iba podľa URL (ak Content-Type nepovie inak)
        ext_guess = choose_ext(image_url, None)
        target_name = f"{folder}/{image_rank:03d}{ext_guess}"

        blob = bucket.blob(target_name)
        if blob.exists(client_gcs):
            print(f"[SKIP exists] gs://{BUCKET}/{target_name}")
            # aj SKIP chceme zalogovať, ale bez veľkosti (size) -> ponechaj size_bytes=None
            rows_out.append({
                "pk": pk,
                "listing_id": listing_id,
                "seq_global": seq_global,
                "image_url": image_url,
                "image_rank": image_rank,
                "gcs_path": f"gs://{BUCKET}/{target_name}",
                "content_type": None,
                "size_bytes": None,
                "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
                "url": detail_url,  # voliteľný; pri load-e sa odfiltruje, ak v tabuľke nie je
            })
            continue

        # stiahni
        try:
            resp = session.get(
                image_url,
                headers=rand_headers(),
                timeout=25,
                stream=True,
            )
        except Exception as e:
            print(f"[ERR GET] {image_url} -> {e}")
            continue

        if resp.status_code != 200:
            print(f"[SKIP HTTP {resp.status_code}] {image_url}")
            continue

        content = resp.content or b""
        if len(content) < 3000:
            print(f"[SKIP tiny] {image_url}")
            continue

        # upresni príponu podľa Content-Type
        ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        final_ext = choose_ext(image_url, ctype)
        if final_ext != ext_guess:
            # zmeň cieľový názov, aby zodpovedal skutočnému typu
            target_name = f"{folder}/{image_rank:03d}{final_ext}"
            blob = bucket.blob(target_name)

        # upload
        try:
            blob.upload_from_string(content, content_type=ctype or "application/octet-stream")
            print(f"[OK] gs://{BUCKET}/{target_name}")
        except Exception as e:
            print(f"[ERR UPLOAD] gs://{BUCKET}/{target_name} -> {e}")
            continue

        size_bytes = len(content)
        rows_out.append({
            "pk": pk,
            "listing_id": listing_id,
            "seq_global": seq_global,
            "image_url": image_url,
            "image_rank": image_rank,
            "gcs_path": f"gs://{BUCKET}/{target_name}",
            "content_type": ctype or None,
            "size_bytes": int(size_bytes),
            "downloaded_at": pd.Timestamp.utcnow(tz="UTC"),
            "url": detail_url,  # voliteľný; pri load-e sa odfiltruje, ak v tabuľke nie je
        })

        # šetrná pauza (trošku random)
        time.sleep(random.uniform(0.05, 0.15))

    # 3) audit -> BQ.images
    if not rows_out:
        print("Nothing to write to BigQuery. Done.")
        return

    df_out = pd.DataFrame(rows_out)

    # typy (hlavne downloaded_at -> datetime64[ns, UTC])
    if "downloaded_at" in df_out.columns:
        df_out["downloaded_at"] = pd.to_datetime(df_out["downloaded_at"], utc=True)

    for col in ("seq_global", "image_rank", "size_bytes"):
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("Int64")

    for col in ("pk","listing_id","image_url","gcs_path","content_type","url"):
        if col in df_out.columns:
            df_out[col] = df_out[col].astype("string")

    table_id = f"{PROJECT_ID}.{DATASET}.images"
    df_aligned = ensure_table_and_align_df(client_bq, df_out, table_id)

    # finálny load (append)
    job = client_bq.load_table_from_dataframe(
        df_aligned,
        table_id,
        location=LOCATION
    )
    job.result()
    print(f"[BQ] Written {len(df_aligned)} rows to {table_id}.")

if __name__ == "__main__":
    main()
