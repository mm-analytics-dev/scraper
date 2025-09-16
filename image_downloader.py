# -*- coding: utf-8 -*-
import os, re, time, math, random, unicodedata
from datetime import datetime, timezone
from urllib.parse import urlparse
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery, storage

# ---------------- ENV ----------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
DATASET    = os.getenv("BQ_DATASET", "realestate_v2")
LOCATION   = os.getenv("BQ_LOCATION", "EU")
BUCKET     = os.getenv("GCS_BUCKET", "")

LOOKBACK_DAYS        = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS         = int(os.getenv("IMAGES_MAX_LISTINGS", "10"))      # nastavíš v jobe
MAX_PER_LISTING      = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))   # nastavíš v jobe
BATCH_DATE_OVERRIDE  = os.getenv("BATCH_DATE", "")  # YYYYMMDD ak chceš
RECREATE_TABLE       = os.getenv("IMAGES_TABLE_RECREATE", "false").lower() == "true"

# ---------------- HTTP ----------------
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]
def new_session():
    s = requests.Session()
    retry = Retry(total=4, connect=3, read=3, backoff_factor=1.2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset(["GET", "HEAD"]))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=20))
    s.mount("http://", HTTPAdapter(max_retries=retry, pool_maxsize=20))
    return s
def headers():
    return {
        "User-Agent": random.choice(UAS),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Referer": "https://www.nehnutelnosti.sk/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

# ---------------- helpers ----------------
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_\-]+")
def safe_slug(s: str) -> str:
    if not s: return "NA"
    # strip diacritics
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(" ", "_")
    s = SAFE_NAME_RE.sub("-", s).strip("-_")
    return s or "NA"

EXT_FROM_CT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "image/gif": ".gif",
}
ALLOW_EXT = (".jpg",".jpeg",".png",".webp",".avif",".gif",".jfif")

def guess_ext(url: str, content_type: str|None) -> str:
    u = url.lower()
    for e in ALLOW_EXT:
        if u.split("?")[0].endswith(e):
            return e
    if content_type:
        return EXT_FROM_CT.get(content_type.split(";")[0].lower(), ".webp")
    return ".webp"

def bq_client():      return bigquery.Client(project=PROJECT_ID)
def gcs_client():     return storage.Client(project=PROJECT_ID)

def table_exists(client: bigquery.Client, table_id: str) -> bool:
    try:
        client.get_table(table_id); return True
    except Exception:
        return False

def ensure_images_table(client: bigquery.Client, table_id: str):
    schema = [
        bigquery.SchemaField("pk", "STRING"),
        bigquery.SchemaField("listing_id", "STRING"),
        bigquery.SchemaField("seq_global", "INTEGER"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("image_rank", "INTEGER"),
        bigquery.SchemaField("gcs_uri", "STRING"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
    ]
    if RECREATE_TABLE and table_exists(client, table_id):
        client.delete_table(table_id, not_found_ok=True)
    if not table_exists(client, table_id):
        client.create_table(bigquery.Table(table_id, schema=schema))
    return schema

def query_df(client: bigquery.Client, sql: str, params: list[bigquery.ScalarQueryParameter] = None) -> pd.DataFrame:
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params or []), location=LOCATION)
    return job.result().to_dataframe(create_bqstorage_client=False)

# ---------------- load candidate URLs ----------------
def load_candidates(client: bigquery.Client) -> pd.DataFrame:
    # Prefer images_urls_daily in lookback window; fallback to image_urls_staging
    sql_daily = f"""
    SELECT pk, listing_id, seq_global, url, image_url, image_rank, batch_ts
    FROM `{PROJECT_ID}.{DATASET}.images_urls_daily`
    WHERE DATE(batch_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL @d DAY)
    ORDER BY COALESCE(seq_global, 0), image_rank
    """
    try:
        df = query_df(client, sql_daily, [bigquery.ScalarQueryParameter("d", "INT64", LOOKBACK_DAYS)])
        if not df.empty:
            return df
    except Exception:
        pass

    sql_staging = f"""
    SELECT pk, listing_id, seq_global, url, image_url, image_rank, CURRENT_TIMESTAMP() AS batch_ts
    FROM `{PROJECT_ID}.{DATASET}.image_urls_staging`
    ORDER BY COALESCE(seq_global, 0), image_rank
    """
    try:
        return query_df(client, sql_staging)
    except Exception:
        return pd.DataFrame(columns=["pk","listing_id","seq_global","url","image_url","image_rank","batch_ts"])

def filter_limits(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    # within each listing keep first MAX_PER_LISTING by image_rank
    df = df.sort_values(["pk","image_rank"], kind="mergesort")
    df["__rn"] = df.groupby("pk").cumcount() + 1
    df = df[df["__rn"] <= MAX_PER_LISTING].drop(columns="__rn")
    # keep only first MAX_LISTINGS distinct pk by seq_global asc
    order = df.groupby("pk")["seq_global"].min().sort_values(kind="mergesort")
    keep_pks = set(order.index[:MAX_LISTINGS].tolist())
    return df[df["pk"].isin(keep_pks)].copy()

def already_downloaded(client: bigquery.Client, pks: list[str]) -> set[str]:
    if not pks: return set()
    if not table_exists(client, f"{PROJECT_ID}.{DATASET}.images"):
        return set()
    sql = f"""
    SELECT DISTINCT image_url
    FROM `{PROJECT_ID}.{DATASET}.images`
    WHERE pk IN UNNEST(@pks)
    """
    df = query_df(client, sql, [bigquery.ArrayQueryParameter("pks", "STRING", pks)])
    return set(df["image_url"].dropna().astype(str).tolist())

# ---------------- main ----------------
def main():
    if not (PROJECT_ID and DATASET and LOCATION and BUCKET):
        raise SystemExit("Missing one of required env vars: GCP_PROJECT_ID, BQ_DATASET, BQ_LOCATION, GCS_BUCKET")

    client_bq  = bq_client()
    client_gcs = gcs_client()
    bucket     = client_gcs.bucket(BUCKET)

    # Prepare output table
    table_id = f"{PROJECT_ID}.{DATASET}.images"
    schema   = ensure_images_table(client_bq, table_id)

    # Load candidates & apply limits
    df = load_candidates(client_bq)
    if df.empty:
        print("No image URLs available. Done.")
        return
    df = filter_limits(df)

    # Dedup with already downloaded
    pks = sorted(df["pk"].dropna().astype(str).unique().tolist())
    done_urls = already_downloaded(client_bq, pks)
    df = df[~df["image_url"].isin(done_urls)].copy()
    if df.empty:
        print("No new image URLs to download. Done.")
        return

    # Batch date prefix
    if BATCH_DATE_OVERRIDE:
        batch_date = BATCH_DATE_OVERRIDE
    else:
        # UTC date so je stabilná naprieč runnermi
        batch_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    s = new_session()
    rows = []
    # ensure proper dtypes present
    for col in ("seq_global","image_rank"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # group by listing for folder naming
    df = df.sort_values(["seq_global","pk","image_rank"], kind="mergesort")
    for (pk, listing_id, seq_global), grp in df.groupby(["pk","listing_id","seq_global"], dropna=False):
        seq = int(seq_global) if not pd.isna(seq_global) else 0
        lid = safe_slug(listing_id)
        folder = f"images_v2/{batch_date}/{seq:06d}_{lid}/"

        # iterate images in rank order
        for _, r in grp.sort_values("image_rank").iterrows():
            img_url   = str(r["image_url"])
            detail_url= str(r.get("url") or "")
            rank      = int(r.get("image_rank") or 0)

            try:
                resp = s.get(img_url, headers=headers(), timeout=30)
            except Exception as e:
                print(f"[ERR] {img_url} -> {e}")
                continue
            if resp.status_code != 200 or not resp.content or len(resp.content) < 3000:
                print(f"[SKIP {resp.status_code}] {img_url}")
                continue

            ext = guess_ext(img_url, resp.headers.get("Content-Type"))
            name = f"{rank:03d}{ext}"
            blob_path = folder + name
            blob = bucket.blob(blob_path)

            try:
                ct = resp.headers.get("Content-Type") or "application/octet-stream"
                blob.upload_from_string(resp.content, content_type=ct)
            except Exception as e:
                print(f"[ERR-UPLOAD] {blob_path} -> {e}")
                continue

            gcs_uri = f"gs://{BUCKET}/{blob_path}"
            print(f"[OK] {gcs_uri}")

            rows.append({
                "pk": str(pk) if pk is not None else None,
                "listing_id": str(listing_id) if listing_id is not None else None,
                "seq_global": seq if not pd.isna(seq_global) else None,
                "url": detail_url,
                "image_url": img_url,
                "image_rank": rank,
                "gcs_uri": gcs_uri,
                "downloaded_at": pd.Timestamp.utcnow(),  # TIMESTAMP
            })

    if not rows:
        print("Nothing saved. Done.")
        return

    df_out = pd.DataFrame(rows, columns=[
        "pk","listing_id","seq_global","url","image_url","image_rank","gcs_uri","downloaded_at"
    ])

    # Coerce dtypes for BQ
    df_out["pk"] = df_out["pk"].astype("string")
    df_out["listing_id"] = df_out["listing_id"].astype("string")
    df_out["url"] = df_out["url"].astype("string")
    df_out["image_url"] = df_out["image_url"].astype("string")
    df_out["gcs_uri"] = df_out["gcs_uri"].astype("string")
    df_out["seq_global"] = pd.to_numeric(df_out["seq_global"], errors="coerce").astype("Int64")
    df_out["image_rank"] = pd.to_numeric(df_out["image_rank"], errors="coerce").astype("Int64")
    df_out["downloaded_at"] = pd.to_datetime(df_out["downloaded_at"], utc=True)

    job_cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
    )
    job = client_bq.load_table_from_dataframe(df_out, table_id, job_config=job_cfg, location=LOCATION)
    job.result()
    print("[BQ] images append OK")

if __name__ == "__main__":
    main()
