# -*- coding: utf-8 -*-
"""
Images downloader (v2, detail-only) -> GCS + BigQuery (PK -> HTTPS GCS URL)

Zmena logiky:
- Nechodíme na /galeria. Vždy načítame HTML DETAILU inzerátu a vyextrahujeme URL fotiek:
  - z <img> (src, data-src, data-original, data-lazy)
  - zo srcset (všetky varianty)
  - z meta og:image / twitter:image
  - aj z <a href> (ak vyzerajú ako fotky)
- Absolutizujeme URL (urljoin), potom ich filtrujeme presnou heuristikou:
  - povolené hosty: img.unitedclassifieds.sk, img.nehnutelnosti.sk
  - povolené prípony: .jpg, .jpeg, .webp, .png, .avif, .jfif, .gif
  - vylúčené: data:, beacon, pixel, ads, analytics
  - ak URL nemá príponu, povolíme ak má CDN hint (napr. "_fss" alebo "?st=")
- Upload do GCS s dedupom; zápis PK->HTTPS do BQ s dedupom.
"""

import os
import re
import json
import time
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Google
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound as GCPNotFound

# --------- ENV ---------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET = os.getenv("BQ_DATASET", "realestate_v2").strip()
LOCATION = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME = os.getenv("GCS_BUCKET", "").strip()

LOOKBACK_DAYS = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS = int(os.getenv("IMAGES_MAX_LISTINGS", "30"))
MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
IMAGES_PK_GCS_TABLE = os.getenv("IMAGES_PK_GCS_TABLE", "images_pk_gcs").strip()

BASE_HOST = "https://www.nehnutelnosti.sk"

# --------- Helpers ---------
def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)

def today_yyyymmdd_utc() -> str:
    if BATCH_DATE_ENV:
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = today_yyyymmdd_utc()
BASE_PREFIX = f"images_v2/{BATCH_YYYYMMDD}"

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
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "sk-SK,sk;q=0.9,cs;q=0.8,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": BASE_HOST + "/",
    }
    if extra: h.update(extra)
    return h

def http_get(url: str, timeout=25, extra_headers=None) -> requests.Response:
    return SESSION.get(url, headers=rand_headers(extra_headers), timeout=timeout)

# --------- GCS / BQ clients ---------
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None) if PROJECT_ID else None
gcs = storage.Client(project=PROJECT_ID or None) if PROJECT_ID else None
bucket = gcs.bucket(BUCKET_NAME) if (gcs and BUCKET_NAME) else None

# --------- BQ utils ---------
def table_id(project: str, dataset: str, name: str) -> str:
    return f"{project}.{dataset}.{name}"

def bq_table_exists(full_table_id: str) -> bool:
    if not bq:
        return False
    try:
        bq.get_table(full_table_id)
        return True
    except GCPNotFound:
        return False
    except Exception:
        return False

def query_df(sql: str, params: List[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    if not bq:
        raise RuntimeError("BigQuery client is not configured (GCP_PROJECT_ID?)")
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_cfg, location=LOCATION or None)
    return job.result().to_dataframe(create_bqstorage_client=False)

# --------- candidate SQL (flexible) ---------
SQL_META_V2 = """
SELECT pk, url, listing_id, seq_global, batch_ts
FROM `{{T}}`
WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY batch_ts DESC) = 1
ORDER BY COALESCE(seq_global, 999999999), batch_ts
LIMIT @max_listings
"""

SQL_META_V1 = SQL_META_V2

SQL_IMAGES_URLS_DAILY = """
WITH recent AS (
  SELECT pk, url, listing_id, seq_global, batch_ts
  FROM `{{T}}`
  WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
),
dedup AS (
  SELECT
    pk,
    ANY_VALUE(url) AS url,
    ANY_VALUE(listing_id) AS listing_id,
    ANY_VALUE(seq_global) AS seq_global,
    MAX(batch_ts) AS batch_ts
  FROM recent
  GROUP BY pk
)
SELECT pk, url, listing_id, seq_global, batch_ts
FROM dedup
ORDER BY COALESCE(seq_global, 999999999), batch_ts
LIMIT @max_listings
"""

def fetch_candidates() -> pd.DataFrame:
    """
    Bezpečne skúša 3 zdroje v BQ (existenciu tabuľky overí). Ak nič, vráti prázdny DF.
    """
    if not bq or not PROJECT_ID or not DATASET:
        print("[WARN] BigQuery nie je nakonfigurované. Preskakujem fetch kandidátov.")
        return pd.DataFrame(columns=["pk", "url", "listing_id", "seq_global", "batch_ts"])

    tried = []

    t1 = table_id(PROJECT_ID, DATASET, "listings_meta_daily_v2")
    if bq_table_exists(t1):
        try:
            sql = SQL_META_V2.replace("{{T}}", t1)
            params = [
                bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
                bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
            ]
            df = query_df(sql, params)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[WARN] primary candidates failed → fallback ({e})")
    else:
        tried.append("listings_meta_daily_v2")

    t2 = table_id(PROJECT_ID, DATASET, "listings_meta_daily")
    if bq_table_exists(t2):
        try:
            sql = SQL_META_V1.replace("{{T}}", t2)
            params = [
                bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
                bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
            ]
            df = query_df(sql, params)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[WARN] secondary candidates failed → fallback ({e})")
    else:
        tried.append("listings_meta_daily")

    t3 = table_id(PROJECT_ID, DATASET, "images_urls_daily")
    if bq_table_exists(t3):
        try:
            sql = SQL_IMAGES_URLS_DAILY.replace("{{T}}", t3)
            params = [
                bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
                bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
            ]
            df = query_df(sql, params)
            if not df.empty:
                return df
        except Exception as e:
            print(f"[WARN] tertiary candidates failed ({e})")
    else:
        tried.append("images_urls_daily")

    print(f"[INFO] Nenašiel som žiadnu kandidátnu tabuľku v BQ ({', '.join(tried) or 'nič'}).")
    return pd.DataFrame(columns=["pk", "url", "listing_id", "seq_global", "batch_ts"])

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

# --------- Image URL filtering (detail logic) ---------
IMG_HOST_ALLOW = (
    "img.unitedclassifieds.sk",
    "img.nehnutelnosti.sk",
)
IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".jfif", ".gif")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics")

def _looks_like_real_image(abs_url: str) -> bool:
    """Prísna heuristika: len reálne fotky z povolených hostov/CDN."""
    if not abs_url:
        return False
    u = abs_url.lower()
    if any(bad in u for bad in IMG_BAD_HINTS):
        return False
    try:
        uu = urlparse(abs_url)
    except Exception:
        return False
    if uu.scheme not in ("http", "https"):
        return False
    host = (uu.netloc or "").lower()
    if not any(host.endswith(h) or h in host for h in IMG_HOST_ALLOW):
        return False
    path = (uu.path or "").lower()
    if any(path.endswith(suf) for suf in IMG_SUFFIX_ALLOW):
        return True
    # dovolíme aj „bez prípony“, ak je to typicky CDN fotka
    if "_fss" in path or "?st=" in u:
        return True
    return False

def extract_image_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    Zo stránky DETAILU pozbieraj všetky potenciálne fotky:
      - <img> (src, data-src, data-original, data-lazy)
      - všetky položky zo srcset
      - meta og:image, twitter:image
      - <a href>
    Všetko absolutizuj, prežeň _looks_like_real_image, deduplikuj a zachovaj poradie.
    """
    candidates: List[str] = []

    # <img> + data-* + srcset (všetky varianty)
    for img in soup.find_all("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy"):
            val = img.get(attr)
            if not val:
                continue
            abs_url = urljoin(base_url, val)
            candidates.append(abs_url)

        srcset = img.get("srcset") or ""
        if srcset:
            for part in srcset.split(","):
                cand = part.strip().split(" ")[0]
                if not cand:
                    continue
                abs_url = urljoin(base_url, cand)
                candidates.append(abs_url)

    # meta og:image / twitter:image
    for sel in ("meta[property='og:image']", "meta[name='twitter:image']"):
        for m in soup.select(sel):
            val = m.get("content")
            if not val:
                continue
            abs_url = urljoin(base_url, val)
            candidates.append(abs_url)

    # <a href>
    for a in soup.find_all("a", href=True):
        abs_url = urljoin(base_url, a["href"])
        candidates.append(abs_url)

    # filter + dedup (poradie)
    seen: Set[str] = set()
    out: List[str] = []
    for u in candidates:
        if not u:
            continue
        if not _looks_like_real_image(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    return out

# --------- detail fetch (bez /galeria) ---------
ID_FROM_URL_RE = re.compile(r"/detail/([^/]+)/?")

def listing_id_from_url(u: str) -> Optional[str]:
    m = ID_FROM_URL_RE.search(u or "")
    return m.group(1) if m else None

def fetch_detail_image_urls(listing_url: str) -> List[str]:
    """Načíta HTML detailu a vráti zoznam obrazkových URL podľa detail-heuristiky."""
    try:
        r = http_get(listing_url)
        if r.status_code != 200:
            print(f"[WARN] detail {listing_url} → HTTP {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        urls = extract_image_urls(soup, listing_url)
        return urls
    except Exception as e:
        print(f"[WARN] detail error {listing_url}: {e}")
        return []

# --------- GCS helpers ---------
def gcs_blob_exists(path: str) -> bool:
    if not bucket:
        return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

def ext_from_response(url: str, resp: requests.Response) -> str:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/avif" in ctype: return ".avif"
    if "image/webp" in ctype: return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype: return ".jpg"
    if "image/png" in ctype: return ".png"
    if "image/gif" in ctype: return ".gif"
    # fallback z URL
    u = url.lower().split("?", 1)[0]
    for suf in IMG_SUFFIX_ALLOW:
        if u.endswith(suf):
            return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text) ; gcs_path = relatívny path v buckete
    """
    folder = make_folder(seq_global, listing_id)
    try:
        resp = SESSION.get(
            image_url,
            timeout=30,
            headers=rand_headers({"Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"}),
            stream=True,
        )
        status = int(resp.status_code)
    except Exception as e:
        return ("", None, f"request_error:{e}")

    if status != 200:
        ctype = resp.headers.get("Content-Type")
        try: resp.close()
        except: pass
        return ("", status, f"http_{status} ({ctype})")

    try:
        ext = ext_from_response(image_url, resp)
        filename = f"{rank:03d}{ext}"
        gcs_path = f"{BASE_PREFIX}/{folder}/{filename}"

        if gcs_blob_exists(gcs_path):
            # vyčerpaj/read stream a zavri
            try: _ = resp.content
            except: pass
            finally: resp.close()
            print(f"[IMG] uložené (exist.) gs://{BUCKET_NAME}/{gcs_path}")
            return (gcs_path, 200, None)

        content = resp.content
        ctype = resp.headers.get("Content-Type")
    finally:
        resp.close()

    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=ctype)
        print(f"[IMG] uložené: gs://{BUCKET_NAME}/{gcs_path}", flush=True)
        return (gcs_path, 200, None)
    except Exception as e:
        return ("", 200, f"gcs_upload_error:{e}")

# --------- BQ: ensure table + read existing pairs + insert new ---------
def ensure_pk_gcs_table(table_id_full: str):
    if not bq:
        return
    schema = [
        bigquery.SchemaField("pk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_date", "DATE", mode="NULLABLE"),
    ]
    try:
        bq.get_table(table_id_full)
    except Exception:
        table = bigquery.Table(table_id_full, schema=schema)
        bq.create_table(table)
        print(f"[BQ] Created table {table_id_full}.")

def fetch_existing_pairs(table_id_full: str, pks: List[str]) -> Set[Tuple[str, str]]:
    if not bq or not pks:
        return set()
    sql = f"""
    SELECT pk, gcs_url
    FROM `{table_id_full}`
    WHERE pk IN UNNEST(@pks)
    """
    try:
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pks", "STRING", pks)]
        )
        df = bq.query(sql, job_config=job_cfg, location=LOCATION or None).result().to_dataframe(create_bqstorage_client=False)
        return set((str(r["pk"]), str(r["gcs_url"])) for _, r in df.iterrows())
    except Exception as e:
        print(f"[WARN] fetch_existing_pairs failed (ignored): {e}")
        return set()

def insert_pk_gcs_rows(table_id_full: str, rows: List[Dict[str, Any]]):
    if not bq:
        print("[INFO] BQ client nie je – preskakujem zápis do BQ.")
        return
    if not rows:
        print("[INFO] Nothing to insert into pk->gcs table.")
        return
    df = pd.DataFrame(rows)
    df["downloaded_at"] = pd.to_datetime(df["downloaded_at"], utc=True)
    df["batch_date"] = pd.to_datetime(df["batch_date"]).dt.date
    job_cfg = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = bq.load_table_from_dataframe(df, table_id_full, job_config=job_cfg, location=LOCATION or None)
    job.result()
    print(f"[BQ] Inserted {len(df)} rows into {table_id_full}.")

# --------- MAIN ---------
def main():
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required.")
    if not BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET is required.")

    # 1) Kandidáti z BQ (alebo nič)
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] Žiadni kandidáti (chýbajú BQ tabuľky alebo prázdny lookback). Končím bez chyby.")
        return

    # Stabilné poradie + 1 riadok/PK
    if "seq_global" in cand.columns:
        cand = cand.sort_values(["seq_global", "pk"], kind="stable")
    else:
        cand = cand.sort_values(["pk"], kind="stable")
    cand = cand.drop_duplicates(subset=["pk"], keep="first")

    # 2) Načítaj DETAIL a uploaduj
    out_records: List[Dict[str, Any]] = []
    total_downloaded = 0
    total_listings = 0

    for _, r in cand.iterrows():
        pk = str(r.get("pk") or "").strip()
        listing_url = str(r.get("url") or "").strip()
        listing_id = (r.get("listing_id") or "").strip() or listing_id_from_url(listing_url)
        seq_global = safe_int(r.get("seq_global"))

        if not pk or not listing_url:
            continue

        # URL obrázkov len z DETAILU (bez /galeria)
        urls = fetch_detail_image_urls(listing_url)
        if not urls:
            print(f"[WARN] žiadne URL fotiek pre {pk} ({listing_url})")
            continue

        # Limit per listing
        urls = urls[:max(1, int(MAX_PER_LISTING))]

        listing_downloaded = 0
        rank = 1
        for img_url in urls:
            gcs_path, http_status, err = download_to_gcs(img_url, seq_global, listing_id, rank)
            if gcs_path:
                https_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{gcs_path}"
                out_records.append({
                    "pk": pk,
                    "gcs_url": https_url,
                    "downloaded_at": now_utc(),
                    "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
                })
                listing_downloaded += 1
                total_downloaded += 1
            else:
                print(f"[WARN] skip {pk} img {rank}: {err or http_status}", flush=True)
            rank += 1
            time.sleep(random.uniform(0.03, 0.12))  # jemná pauza medzi obrázkami

        total_listings += 1
        print(f"[INFO] {pk}: {listing_downloaded} obrázkov.", flush=True)
        time.sleep(random.uniform(0.2, 0.6))  # pauza medzi inzerátmi

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    # 3) Dedup pred zápisom do BQ
    table_id_full = table_id(PROJECT_ID, DATASET, IMAGES_PK_GCS_TABLE)
    ensure_pk_gcs_table(table_id_full)

    pks = sorted({rec["pk"] for rec in out_records})
    existing = fetch_existing_pairs(table_id_full, pks)

    final_rows = [
        rec for rec in out_records
        if (rec["pk"], rec["gcs_url"]) not in existing
    ]

    # 4) Zapíš len nové páry
    insert_pk_gcs_rows(table_id_full, final_rows)

    print(f"[DONE] Listings: {total_listings}, images downloaded: {total_downloaded}, inserted new rows: {len(final_rows)}")

if __name__ == "__main__":
    main()
