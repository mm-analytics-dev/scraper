# -*- coding: utf-8 -*-
"""
Images downloader (v4) -> GCS + BigQuery (PK -> GCS URL)

Funkcia:
- Z {DATASET}.listings_master zoberie inzeráty v lookback okne (IMAGES_LOOKBACK_DAYS),
  max IMAGES_MAX_LISTINGS kusov (podľa seq_global).
- Z detailu inzerátu vyparsuje VŠETKY rozumné image URL (og:image, JSON-LD, galéria, srcset, data-*).
- Každý obrázok (max IMAGES_MAX_PER_LISTING) stiahne, uloží do GCS pod images_v2/YYYYMMDD/{FOLDER}/{NNN.ext}
  a do BQ {DATASET}.{IMAGES_PK_GCS_TABLE} zapíše jeden riadok: (pk, gcs_url, downloaded_at, batch_date).
- Idempotentné:
    * ak gs://... už existuje -> upload preskočí,
    * pred zápisom do BQ odfiltruje už existujúce (pk,gcs_url).

ENV:
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=7
  IMAGES_MAX_LISTINGS=200
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD (optional; ak nie je, berie dnešný UTC)
  IMAGES_PK_GCS_TABLE=images_pk_gcs (optional; default)
"""

import os
import re
import time
import json
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse

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

LOOKBACK_DAYS     = int(os.getenv("IMAGES_LOOKBACK_DAYS", "7"))
MAX_LISTINGS      = int(os.getenv("IMAGES_MAX_LISTINGS", "200"))
MAX_PER_LISTING   = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE_ENV    = os.getenv("BATCH_DATE", "").strip()
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
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=20, pool_maxsize=20)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
]
def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra: h.update(extra)
    return h

def rand_headers_img(extra=None):
    h = rand_headers(extra)
    h["Accept"] = "image/avif,image/webp,image/*,*/*;q=0.8"
    return h

# --------- GCS / BQ clients ---------
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID or None)
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

# --------- SQL: kandidáti z listings_master ---------
SQL_CANDIDATES = """
SELECT pk, listing_id, seq_global, url
FROM `{{PROJECT_ID}}.{{DATASET}}.listings_master`
WHERE last_seen >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
  AND url IS NOT NULL
ORDER BY seq_global
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

# --------- Helpers ---------
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

def strip_query(u: str) -> str:
    # Pomôže dedupu rôznych variant rovnakého obrázka s query parametrami
    try:
        p = urlparse(u)
        return p._replace(query="").geturl()
    except Exception:
        return u

# --------- Parsovanie VŠETKÝCH image URL z detailu ---------
def extract_image_urls(html: str, page_url: str, limit: int) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[str] = []
    seen: Set[str] = set()

    def push(u: str):
        if not u: return
        au = urljoin(page_url, u.strip())
        key = strip_query(au)
        if key not in seen:
            seen.add(key)
            out.append(au)

    # 1) og:image (zober aj viac, ak ich je viac)
    for og in soup.find_all("meta", property="og:image"):
        if og.get("content"): push(og["content"])
        if len(out) >= limit: return out[:limit]

    # 2) JSON-LD "image" (string alebo pole)
    for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        payload = sc.string or sc.get_text() or ""
        if not payload.strip(): continue
        try: data = json.loads(payload)
        except Exception: continue
        stack = [data]
        while stack:
            obj = stack.pop()
            if isinstance(obj, dict):
                img = obj.get("image")
                if isinstance(img, str):
                    push(img)
                elif isinstance(img, list):
                    for it in img:
                        if isinstance(it, str):
                            push(it)
                for v in obj.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(obj, list):
                stack.extend(obj)
        if len(out) >= limit: return out[:limit]

    # 3) Galéria / hero – <img> s rôznymi atribútmi
    selectors = [
        "[class*='gallery'] img", "[class*='carousel'] img", "[class*='swiper'] img",
        "[class*='slider'] img", "[class*='image'] img", "img"
    ]
    for sel in selectors:
        for img in soup.select(sel):
            for attr in ("data-full", "data-original", "data-src", "data-lazy", "src"):
                if img.get(attr): push(img.get(attr))
            # srcset
            ss = img.get("srcset") or img.get("data-srcset")
            if ss:
                parts = [p.strip().split(" ")[0] for p in ss.split(",") if p.strip()]
                for u in parts: push(u)
            if len(out) >= limit: return out[:limit]

    return out[:limit]

def fetch_all_image_urls(detail_url: str, limit: int) -> List[str]:
    try:
        r = SESSION.get(detail_url, timeout=30, headers=rand_headers())
    except Exception:
        return []
    if r.status_code != 200 or not r.text:
        return []
    return extract_image_urls(r.text, detail_url, limit=limit)

# --------- Download + upload ---------
def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text)
    """
    folder = make_folder(seq_global, listing_id)

    try:
        resp = SESSION.get(image_url, timeout=30, headers=rand_headers_img(), stream=True)
        status = int(resp.status_code)
    except Exception as e:
        return ("", None, f"request_error:{e}")

    if status != 200:
        ctype = resp.headers.get("Content-Type")
        try: resp.close()
        except Exception: pass
        return ("", status, f"http_{status} ({ctype})")

    ext = ext_from_response(image_url, resp)
    filename = f"{rank:03d}{ext}"
    gcs_path = f"{BASE_PREFIX}/{folder}/{filename}"

    # Duplicitný upload = preskočiť
    if gcs_blob_exists(gcs_path):
        try: _ = resp.content
        except Exception: pass
        finally: resp.close()
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

# --------- BQ: ensure table + dedup načítanie + insert ---------
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
    if not pks:
        return set()
    sql = f"""
    SELECT pk, gcs_url
    FROM `{table_id}`
    WHERE pk IN UNNEST(@pks)
    """
    params = [bigquery.ArrayQueryParameter("pks", "STRING", pks)]
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

    # 1) Kandidáti z listings_master
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No candidates from listings_master in lookback window.")
        return

    cand = cand.sort_values(["seq_global", "pk"], kind="stable").drop_duplicates(subset=["pk"], keep="first")

    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_PK_GCS_TABLE}"
    ensure_pk_gcs_table(table_id)

    # Prednačítaj existujúce páry pre dané PK, aby sme vedeli preskočiť už zapísané fotky
    pk_list = cand["pk"].astype(str).tolist()
    existing_pairs = fetch_existing_pairs(table_id, pk_list)

    out_records: List[Dict[str, Any]] = []

    # 2) Pre každý PK stiahni VŠETKY obrázky (limit MAX_PER_LISTING)
    for _, r in cand.iterrows():
        pk = str(r.get("pk"))
        detail_url = r.get("url")
        listing_id = r.get("listing_id")
        seq_global = safe_int(r.get("seq_global"))

        if not pk or not detail_url:
            continue

        img_urls = fetch_all_image_urls(detail_url, limit=MAX_PER_LISTING)
        if not img_urls:
            print(f"[WARN] no images found for pk={pk} ({detail_url})", flush=True)
            continue

        rank = 0
        for u in img_urls[:MAX_PER_LISTING]:
            rank += 1
            # budúca GCS URL (pre dedup s BQ): musíme vedieť gcs_path, aby sme vedeli, či pár (pk,gcs_url) už existuje
            folder = make_folder(seq_global, listing_id)
            ext = ".jpg"  # dočasne; finálnu príponu určíme až po HTTP response
            gcs_path_pred = f"{BASE_PREFIX}/{folder}/{rank:03d}{ext}"
            gcs_url_pred = f"gs://{BUCKET_NAME}/{gcs_path_pred}"

            # Ak už (pk, gcs_url_pred) existuje (z predchádzajúceho behu), preskoč stiahnutie;
            # POZOR: keďže ext môže byť iná (.webp/.png), nevieš dopredu predikovať presne.
            # Preto dedup urobíme až po skutočnom uploade (podľa reálnej prípony).
            # Stále však môžeme predísť duplicitnému uploade tým, že sa pozrieme na existenciu blobu
            # s reálnou príponou po stiahnutí.

            gcs_path, http_status, err = download_to_gcs(u, seq_global, listing_id, rank)
            if not gcs_path:
                print(f"[WARN] pk={pk} img#{rank:03d} skip: {err or http_status}", flush=True)
                continue

            gcs_url = f"gs://{BUCKET_NAME}/{gcs_path}"
            if (pk, gcs_url) in existing_pairs:
                # už je zapísané v BQ – žiadny nový riadok
                continue

            out_records.append({
                "pk": pk,
                "gcs_url": gcs_url,
                "downloaded_at": now_utc(),
                "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
            })

        # jemná pauza medzi inzerátmi (nerozhnevaj hostiteľa)
        time.sleep(random.uniform(0.1, 0.25))

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    # 3) Finálna deduplikácia (ak by sa beh prekrýval) a insert do BQ
    # (pre istotu si znovu načítaj existujúce páry na subset PK)
    pk_subset = sorted({rec["pk"] for rec in out_records})
    existing_pairs2 = fetch_existing_pairs(table_id, pk_subset)
    final_rows = [rec for rec in out_records if (rec["pk"], rec["gcs_url"]) not in existing_pairs2]

    insert_pk_gcs_rows(table_id, final_rows)

if __name__ == "__main__":
    main()
