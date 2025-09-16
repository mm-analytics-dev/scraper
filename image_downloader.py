# -*- coding: utf-8 -*-
"""
Images downloader (v2) -> GCS + BigQuery (PK -> HTTPS GCS URL)

Co robi:
- Z BQ (ak existuje) vytiahne kandidátov z tabuliek v poradí:
    1) {DATASET}.listings_meta_daily_v2
    2) {DATASET}.listings_meta_daily
    3) {DATASET}.images_urls_daily  (fallback – tu len berieme PK/URL, obrazky parsujeme z galérie)
  s lookbackom podľa IMAGES_LOOKBACK_DAYS a limitom IMAGES_MAX_LISTINGS.
- Pre každý inzerát ide do fotogalerie (https://www.nehnutelnosti.sk/detail/<ID>/galeria),
  vytiahne všetky velké obrázky (strict whitelist hostov), max IMAGES_MAX_PER_LISTING/fotiek,
  a nahrá do GCS: images_v2/YYYYMMDD/{FOLDER}/{NNN.ext}
  (YYYYMMDD = dnešný UTC alebo BATCH_DATE=YYYYMMDD).
- Do BQ tabuľky {DATASET}.{IMAGES_PK_GCS_TABLE} zapisuje riadky:
    pk STRING, gcs_url STRING (HTTPS), downloaded_at TIMESTAMP, batch_date DATE
  (duplikáty (pk,gcs_url) nikdy nezapisujeme).
- Dedup: neuploaduje už existujúci blob v GCS a pred zápisom do BQ preskakuje existujúce dvojice.

ENV (povinné kde sa používa BQ/GCS):
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=3
  IMAGES_MAX_LISTINGS=30
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD (optional)
  IMAGES_PK_GCS_TABLE=images_pk_gcs (optional; default)

Poznámky:
- Ak žiadna z očakávaných BQ tabuliek neexistuje, skript korektne skončí s INFO logom (exit 0).
- HTTPS linky do BQ: https://storage.googleapis.com/<bucket>/<path>
"""

import os
import re
import json
import time
import math
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

SQL_META_V1 = SQL_META_V2  # rovnaká schéma, ak existuje staršia tabuľka

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
    Postupne skúsi 3 zdroje v BQ. Pri chybe/NotFound prechádza na ďalší.
    Ak nič, vráti prázdny DataFrame (skript korektne skončí).
    """
    if not bq or not PROJECT_ID or not DATASET:
        print("[WARN] BigQuery nie je nakonfigurované. Preskakujem fetch kandidátov.")
        return pd.DataFrame(columns=["pk", "url", "listing_id", "seq_global", "batch_ts"])

    tried = []

    # 1) listings_meta_daily_v2
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

    # 2) listings_meta_daily (starší názov)
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

    # 3) images_urls_daily (pôvodná)
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

    print(f"[INFO] Nenašiel som žiadnu z kandidátnych tabuliek v BQ ({', '.join(tried) or 'nič nebolo skúšané'}).")
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

# --------- image URL filters ---------
ALLOWED_IMG_HOSTS = (
    "img.unitedclassifieds.sk",
    "img.nehnutelnosti.sk",
    "ucarecdn.com",
    "ucarecdn",
)
IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".gif")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics", "/_next/", "/static/")

def is_good_image_url(u: str) -> bool:
    if not u: return False
    if any(b in u.lower() for b in IMG_BAD_HINTS): return False
    try:
        uu = urlparse(u)
    except Exception:
        return False
    if uu.scheme not in ("http", "https"):
        return False
    host = (uu.netloc or "").lower()
    if not any(h in host for h in ALLOWED_IMG_HOSTS):
        return False
    path = uu.path.lower()
    if any(path.endswith(s) for s in IMG_SUFFIX_ALLOW):
        return True
    # často majú aj bez suffixu, ale s query parametrami; povolíme, ak to vyzerá na image CDN
    if "fss" in path or "image" in path:
        return True
    return False

def best_from_srcset(srcset: str, base: str) -> Optional[str]:
    """
    Vyber najväčší variant zo srcsetu. Vráti absolútne URL (urljoin), ak prejde whitelistom.
    """
    if not srcset:
        return None
    best = None
    best_w = -1
    for part in srcset.split(","):
        part = part.strip()
        if not part:
            continue
        bits = part.split()
        cand = bits[0]
        width = 0
        if len(bits) > 1 and bits[1].endswith("w"):
            try:
                width = int(re.sub(r"\D", "", bits[1]))
            except Exception:
                width = 0
        abs_url = urljoin(base, cand)
        if is_good_image_url(abs_url) and width >= best_w:
            best = abs_url
            best_w = width
    return best

# --------- Gallery extraction ---------
ID_FROM_URL_RE = re.compile(r"/detail/([^/]+)/?")

def listing_id_from_url(u: str) -> Optional[str]:
    m = ID_FROM_URL_RE.search(u or "")
    return m.group(1) if m else None

def gallery_url_for(listing_url: str, listing_id: Optional[str]) -> Optional[str]:
    lid = listing_id or listing_id_from_url(listing_url or "")
    if not lid:
        return None
    return f"{BASE_HOST}/detail/{lid}/galeria"

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
        if u.endswith(suf): return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def extract_images_from_soup(soup: BeautifulSoup, base_url: str) -> List[str]:
    urls: List[str] = []

    # <img> – src, data-src, srcset
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-original") or img.get("data-lazy")
        if src:
            abs_url = urljoin(base_url, src)
            if is_good_image_url(abs_url):
                urls.append(abs_url)
        srcset = img.get("srcset")
        if srcset:
            u = best_from_srcset(srcset, base_url)
            if u and is_good_image_url(u):
                urls.append(u)

    # JSON „state“ – hľadáme kľúče typu images/url/src
    for sc in soup.find_all("script"):
        payload = sc.string or sc.get_text() or ""
        if not payload or len(payload) < 40:
            continue
        # jednoduché & robustné vytiahnutie "url":"https://....(jpg|webp|png|avif)"
        for m in re.finditer(r'"(url|src|imageUrl)"\s*:\s*"(https?://[^"]+)"', payload, re.I):
            u = m.group(2)
            if is_good_image_url(u):
                urls.append(u)

    # meta og:image
    for sel in ["meta[property='og:image']", "meta[name='twitter:image']"]:
        for m in soup.select(sel):
            u = m.get("content")
            if not u: continue
            abs_url = urljoin(base_url, u)
            if is_good_image_url(abs_url):
                urls.append(abs_url)

    # dedup zachovaj poradie
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def extract_gallery_image_urls(listing_url: str, listing_id: Optional[str]) -> List[str]:
    """
    Preferuj stránku galérie; fallback na detail, všetko s absolútnymi URL a whitelistom hostov.
    """
    # 1) galeria
    g_url = gallery_url_for(listing_url, listing_id)
    if g_url:
        try:
            r = http_get(g_url)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                urls = extract_images_from_soup(soup, g_url)
                if urls:
                    return urls
            else:
                print(f"[WARN] galéria {g_url} → HTTP {r.status_code}")
        except Exception as e:
            print(f"[WARN] galéria error {g_url}: {e}")

    # 2) fallback: detail
    try:
        r2 = http_get(listing_url)
        if r2.status_code == 200:
            soup2 = BeautifulSoup(r2.text, "html.parser")
            urls2 = extract_images_from_soup(soup2, listing_url)
            return urls2
        else:
            print(f"[WARN] detail {listing_url} → HTTP {r2.status_code}")
    except Exception as e:
        print(f"[WARN] detail error {listing_url}: {e}")

    return []

# --------- GCS helpers ---------
def gcs_blob_exists(path: str) -> bool:
    if not bucket: return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text) ; gcs_path = relatívny path v buckete
    """
    folder = make_folder(seq_global, listing_id)
    try:
        resp = SESSION.get(image_url, timeout=30, headers=rand_headers({
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"
        }), stream=True)
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
            try: _ = resp.content
            except: pass
            finally: resp.close()
            return (gcs_path, 200, None)

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
    """
    Bezpečné a rýchle: IN UNNEST(@pks), žiadne ručné skladanie reťazcov.
    """
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

    # 2) Sťahuj z galérie a uploaduj
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

        # Získaj URL obrázkov z galérie/detailu
        urls = extract_gallery_image_urls(listing_url, listing_id)
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
            # malá pauza medzi obrázkami (opatrnejšie voči serveru)
            time.sleep(random.uniform(0.03, 0.12))

        total_listings += 1
        print(f"[INFO] {pk}: {listing_downloaded} obrázkov.", flush=True)
        # jemná pauza medzi inzerátmi
        time.sleep(random.uniform(0.2, 0.6))

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
