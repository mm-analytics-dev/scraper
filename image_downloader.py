# -*- coding: utf-8 -*-
"""
image_downloader.py

Stiahne fotogalérie inzerátov do GCS a zapíše mapovanie (pk, HTTPS GCS URL) do BigQuery.

- Kandidáti: z {GCP_PROJECT_ID}.{BQ_DATASET}.listings_master (last_seen v posledných LOOKBACK_DAYS),
  preferuje active=TRUE, unikátne pk, radené podľa seq_global, limit MAX_LISTINGS.
- Extrakcia obrázkov: "galéria-first"
    1) hľadá kontajner galérie (gallery|carousel|swiper|slider|photo(s), data-testid*='gallery'),
       vyberá <img> (src/data-* srcset) a <a href> priamo na obrázky
    2) prejde všetky <script> a vytiahne URL obrázkov zo zabalených JSON (regex na image hosty/sufixy)
    3) fallback: meta og:image (1 ks), ak nič iné
- Host allowlist a blacklist, max IMAGES_MAX_PER_LISTING na inzerát
- GCS: images_v2/YYYYMMDD/<seq>_<listing_id>/<NNN>.<ext>, idempotentný upload (existujúce preskočí)
- BigQuery: vytvorí tabuľku IMAGES_PK_GCS_TABLE (ak treba), vloží len nové dvojice (pk, https_url)

Poznámky:
- Skript nevyužíva žiadnu "images_urls_daily" staging tabuľku.
- URL v BQ sú v tvare HTTPS (public): https://storage.googleapis.com/<bucket>/<path>
"""

import os
import re
import json
import time
import math
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

# --- (voliteľné) auto-install chýbajúcich balíkov v CI ---
def _ensure_deps():
    pkgs = [
        ("pandas", "pandas"),
        ("requests", "requests"),
        ("bs4", "beautifulsoup4"),
        ("google.cloud.bigquery", "google-cloud-bigquery"),
        ("google.cloud.storage", "google-cloud-storage"),
        ("urllib3", "urllib3"),
    ]
    for mod, pipname in pkgs:
        try:
            __import__(mod if "." not in mod else mod.split(".")[0])
        except Exception:
            os.system(f"python -m pip install -q {pipname}")

_ensure_deps()

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

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

# --------- Constants / filters ---------
IMG_HOST_ALLOW = (
    "img.unitedclassifieds.sk",
    "img.nehnutelnosti.sk",
)
IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".gif", ".jfif")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics")

# tolerantný regex na obrázky z povolených hostov
IMG_URL_RE = re.compile(
    r"https?://(?:img\.unitedclassifieds\.sk|img\.nehnutelnosti\.sk)[^\s\"'>]+?\.(?:jpg|jpeg|png|webp|avif|gif|jfif)(?:\?[^\s\"'>]*)?",
    re.I
)

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

def slugify(s: str) -> str:
    if not s:
        return "na"
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "na"

def safe_int(x):
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None

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
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "sk-SK,sk;q=0.9,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra:
        h.update(extra)
    return h

def http_get(url: str, timeout: int = 30, extra_headers: Optional[Dict[str, str]] = None) -> requests.Response:
    for attempt in range(5):
        try:
            r = SESSION.get(url, timeout=timeout, headers=rand_headers(extra_headers))
        except Exception:
            time.sleep(min(3, 0.5 * (attempt + 1)))
            continue
        if r.status_code in (429, 503):
            time.sleep((2 ** attempt) + random.random())
            continue
        return r
    return r  # posledná odpoveď

# --------- GCS / BQ clients ---------
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID or None)
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

def make_folder(seq_global: Optional[int], listing_id: Optional[str]) -> str:
    if seq_global is not None:
        if listing_id:
            return f"{seq_global:06d}_{slugify(str(listing_id))}"
        return f"{seq_global:06d}"
    return slugify(listing_id) if listing_id else "na"

def https_url(bucket_name: str, path: str) -> str:
    return f"https://storage.googleapis.com/{bucket_name}/{path.lstrip('/')}"

def gcs_blob_exists(path: str) -> bool:
    if not bucket:
        return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

def ext_from_response(url: str, resp: requests.Response) -> str:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/webp" in ctype:
        return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype:
        return ".jpg"
    if "image/png" in ctype:
        return ".png"
    if "image/avif" in ctype:
        return ".avif"
    if "image/gif" in ctype:
        return ".gif"
    u = url.lower().split("?", 1)[0]
    for suf in (".webp", ".jpg", ".jpeg", ".png", ".avif", ".gif", ".jfif"):
        if u.endswith(suf):
            return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def looks_like_real_image(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if any(bad in u for bad in IMG_BAD_HINTS):
        return False
    if not any(host in u for host in IMG_HOST_ALLOW):
        return False
    if any(u.endswith(s) for s in IMG_SUFFIX_ALLOW):
        return True
    if "_fss" in u or "?st=" in u:
        return True
    return False

# --------- BigQuery: candidates (from listings_master) ---------
SQL_CANDIDATES = """
WITH recent AS (
  SELECT pk, listing_id, seq_global, url, last_seen, active
  FROM `{{PROJECT_ID}}.{{DATASET}}.listings_master`
  WHERE last_seen >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
    AND pk IS NOT NULL
    AND url IS NOT NULL
),
dedup AS (
  SELECT
    pk, listing_id, seq_global, url, active,
    ROW_NUMBER() OVER (PARTITION BY pk ORDER BY seq_global) AS rn
  FROM recent
)
SELECT pk, listing_id, seq_global, url
FROM dedup
WHERE rn = 1
ORDER BY (active IS NOT TRUE), seq_global  -- aktívne najprv
LIMIT @max_listings
""".replace("{{PROJECT_ID}}", PROJECT_ID).replace("{{DATASET}}", DATASET)

def query_df(sql: str, params: List[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_cfg, location=LOCATION or None)
    return job.result().to_dataframe(create_bqstorage_client=False)

def fetch_candidates() -> pd.DataFrame:
    params = [
        bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
        bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
    ]
    return query_df(SQL_CANDIDATES, params)

# --------- Image extraction (gallery-first) ---------
GALLERY_SEL = [
    "[data-testid*='gallery']",
    "[class*='gallery']",
    "[class*='carousel']",
    "[class*='swiper']",
    "[class*='slider']",
    "[class*='photo']",
    "[id*='gallery']",
]

def _collect_imgs_in_scope(scope: BeautifulSoup) -> List[str]:
    urls: List[str] = []
    if scope is None:
        return urls
    # IMG tags: src, data-src, data-original, data-lazy, srcset
    for img in scope.find_all("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy"):
            u = (img.get(attr) or "").strip()
            if looks_like_real_image(u):
                urls.append(u)
        srcset = img.get("srcset") or ""
        if srcset:
            for part in srcset.split(","):
                u = part.strip().split(" ")[0]
                if looks_like_real_image(u):
                    urls.append(u)
    # Anchory priamo na obrázky
    for a in scope.find_all("a", href=True):
        u = a["href"].strip()
        if looks_like_real_image(u):
            urls.append(u)
    return urls

def extract_gallery_urls_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) nájdi galériu a ber len z nej (aby sme neťahali reklamy/podobné inzeráty)
    gal_urls: List[str] = []
    for sel in GALLERY_SEL:
        for node in soup.select(sel):
            gal_urls.extend(_collect_imgs_in_scope(node))
    # 2) ak z galérie je málo (alebo nič), skús JSON v <script> (initial-state)
    script_urls: List[str] = []
    if len(gal_urls) < 5:
        for sc in soup.find_all("script"):
            payload = sc.string or sc.get_text() or ""
            if not payload or len(payload) < 100:
                continue
            for m in IMG_URL_RE.finditer(payload):
                u = m.group(0)
                if looks_like_real_image(u):
                    script_urls.append(u)
    # 3) fallback na meta og:image (1 ks)
    meta_urls: List[str] = []
    if not gal_urls and not script_urls:
        for sel in ["meta[property='og:image']", "meta[name='twitter:image']"]:
            for m in soup.select(sel):
                u = (m.get("content") or "").strip()
                if looks_like_real_image(u):
                    meta_urls.append(u)

    # poradie: galéria -> skripty -> meta
    all_urls = gal_urls + script_urls + meta_urls

    # normalizácia a dedup (stabilné poradie)
    seen = set()
    unique: List[str] = []
    for u in all_urls:
        # odstráň bežné thumb parametre, nech nevznikajú dupy
        base = u.strip()
        if base in seen:
            continue
        seen.add(base)
        unique.append(base)
    return unique

# --------- Download + upload to GCS ---------
def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text)
    """
    folder = make_folder(seq_global, listing_id)

    try:
        resp = http_get(image_url, timeout=45, extra_headers={"Accept": "image/avif,image/webp,image/*,*/*;q=0.8"})
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
        print(f"[OK] {https_url(BUCKET_NAME, gcs_path)}", flush=True)
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
        table = bq.create_table(table)
        print(f"[BQ] Created table {table_id}.")

def fetch_existing_pairs(table_id: str, pks: List[str]) -> Set[Tuple[str, str]]:
    if not pks:
        return set()
    sql = f"""
    SELECT pk, gcs_url
    FROM `{table_id}`
    WHERE pk IN UNNEST(@pks)
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("pks", "STRING", pks)]
    )
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

    # 1) Kandidáti z listings_master
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No candidates from listings_master (check lookback/limits).")
        return

    # 2) Pre každý inzerát: načítaj detail a vytiahni galériu
    out_records: List[Dict[str, Any]] = []
    total_images = 0

    for _, r in cand.iterrows():
        pk = str(r.get("pk"))
        listing_id = r.get("listing_id")
        seq_global = safe_int(r.get("seq_global"))
        url = r.get("url")

        if not url or not pk:
            continue

        detail_resp = http_get(url, timeout=35)
        if int(detail_resp.status_code) != 200:
            print(f"[WARN] skip {pk}: HTTP {detail_resp.status_code} on detail", flush=True)
            continue

        html = detail_resp.text or ""
        try:
            img_urls = extract_gallery_urls_from_html(html)
        except Exception as e:
            print(f"[WARN] gallery parse failed for {pk}: {e}", flush=True)
            img_urls = []

        # bezpečný filtro-dedup + limit
        cleaned = []
        seen = set()
        for u in img_urls:
            if looks_like_real_image(u) and u not in seen:
                seen.add(u)
                cleaned.append(u)
            if len(cleaned) >= MAX_PER_LISTING:
                break

        if not cleaned:
            print(f"[INFO] no gallery images for {pk}", flush=True)
            continue

        # 3) Sťahuj + uploaduj
        rank = 1
        for u in cleaned:
            gcs_path, http_status, err = download_to_gcs(u, seq_global, listing_id, rank)
            if gcs_path:
                out_records.append({
                    "pk": pk,
                    "gcs_url": https_url(BUCKET_NAME, gcs_path),  # HTTPS URL!
                    "downloaded_at": now_utc(),
                    "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
                })
                total_images += 1
                rank += 1
            else:
                print(f"[WARN] skip {pk} #{rank:03d}: {err or http_status}", flush=True)
            time.sleep(random.uniform(0.05, 0.15))

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    # 4) Dedup pred zápisom do BQ (nikdy duplicitne)
    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_PK_GCS_TABLE}"
    ensure_pk_gcs_table(table_id)

    pks = sorted({rec["pk"] for rec in out_records})
    existing = fetch_existing_pairs(table_id, pks)

    final_rows = [
        rec for rec in out_records
        if (rec["pk"], rec["gcs_url"]) not in existing
    ]

    # 5) Zapíš len nové páry (pk, https gcs url)
    insert_pk_gcs_rows(table_id, final_rows)

    print(f"[DONE] listings: {len(cand)} | images uploaded this run: {total_images} | rows inserted: {len(final_rows)}", flush=True)

if __name__ == "__main__":
    main()
