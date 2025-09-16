# -*- coding: utf-8 -*-
"""
Images downloader (galéria) -> GCS + BigQuery (PK -> HTTPS URL na GCS)

Co robi:
- Z BQ tab. {DATASET}.listings_daily (fallback {DATASET}.listings_master) vezme inzeráty
  z posledných IMAGES_LOOKBACK_DAYS podľa batch_ts/last_seen.
- Pre každý inzerát otvorí DETAIL a nájde link na GALÉRIU (/detail/galeria/foto/…).
- Z galérie vyparsuje všetky <img> z hostov img.nehnutelnosti.sk / img.unitedclassifieds.sk,
  vyberie najväčší variant zo srcset, zoradí podľa alt="..._foto_XX".
- Stiahne max IMAGES_MAX_PER_LISTING fotiek na inzerát a uloží do:
    gs://{GCS_BUCKET}/images_v2/YYYYMMDD/{SEQ_LISTING}/NNN.ext
  a do BQ {DATASET}.{IMAGES_PK_GCS_TABLE} zapíše:
    pk STRING, gcs_url STRING (HTTPS!), downloaded_at TIMESTAMP, batch_date DATE
- Dedup: neuploaduje, ak blob už existuje; pred zápisom do BQ vynechá už existujúce (pk,gcs_url).

ENV (Secrets / Variables):
  GOOGLE_APPLICATION_CREDENTIALS=/path/sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2              (ak máš aj BQ_DATASET_V2, zoberie sa ten)
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=3
  IMAGES_MAX_LISTINGS=15
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD                   (optional; inak dnešný UTC)
  IMAGES_PK_GCS_TABLE=images_pk_gcs     (optional)
"""

import os
import re
import json
import time
import random
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

from google.cloud import bigquery
from google.cloud import storage

# ------------------ ENV ------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET = (os.getenv("BQ_DATASET_V2") or os.getenv("BQ_DATASET") or "realestate_v2").strip()
LOCATION = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME = os.getenv("GCS_BUCKET", "").strip()

LOOKBACK_DAYS = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS = int(os.getenv("IMAGES_MAX_LISTINGS", "15"))
MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
IMAGES_PK_GCS_TABLE = os.getenv("IMAGES_PK_GCS_TABLE", "images_pk_gcs").strip()

# ------------------ Batch date / prefix ------------------
def today_yyyymmdd_utc() -> str:
    if BATCH_DATE_ENV:
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = today_yyyymmdd_utc()
BASE_PREFIX = f"images_v2/{BATCH_YYYYMMDD}"

# ------------------ HTTP session ------------------
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra:
        h.update(extra)
    return h

SESSION = requests.Session()
_retry = Retry(
    total=4, connect=3, read=3, backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"])
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

def http_get(url: str, extra_headers=None, timeout=30) -> requests.Response:
    return SESSION.get(url, headers=rand_headers(extra_headers), timeout=timeout)

# ------------------ BQ / GCS clients ------------------
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID or None)
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

# ------------------ Utils ------------------
def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)

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
    if not bucket:
        return False
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

# ------------------ Kandidáti z BQ ------------------
SQL_DAILY = """
SELECT pk, listing_id, seq_global, url
FROM `{{PROJECT}}.{{DATASET}}.listings_daily`
WHERE DATE(batch_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY batch_ts DESC) = 1
ORDER BY seq_global
LIMIT @max_listings
""".replace("{{PROJECT}}", PROJECT_ID).replace("{{DATASET}}", DATASET)

SQL_MASTER = """
SELECT pk, listing_id, seq_global, url
FROM `{{PROJECT}}.{{DATASET}}.listings_master`
WHERE last_seen >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY last_seen DESC) = 1
ORDER BY seq_global
LIMIT @max_listings
""".replace("{{PROJECT}}", PROJECT_ID).replace("{{DATASET}}", DATASET)

def query_df(sql: str, params: List[bigquery.ScalarQueryParameter]) -> pd.DataFrame:
    job_cfg = bigquery.QueryJobConfig(query_parameters=params)
    job = bq.query(sql, job_config=job_cfg, location=LOCATION or None)
    return job.result().to_dataframe(create_bqstorage_client=False)

def fetch_candidates() -> pd.DataFrame:
    params = [
        bigquery.ScalarQueryParameter("lookback_days", "INT64", LOOKBACK_DAYS),
        bigquery.ScalarQueryParameter("max_listings", "INT64", MAX_LISTINGS),
    ]
    try:
        return query_df(SQL_DAILY, params)
    except Exception as e:
        print(f"[WARN] listings_daily query failed, falling back to listings_master ({e})")
        return query_df(SQL_MASTER, params)

# ------------------ Galéria – parsovanie ------------------
IMG_HOST_ALLOW = ("img.unitedclassifieds.sk", "img.nehnutelnosti.sk")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics")
GALLERY_LINK_RE = re.compile(r"/detail/galeria/foto/[^\s\"'>]+", re.I)

def is_good_image_url(u: str) -> bool:
    if not u:
        return False
    lu = u.lower()
    if any(b in lu for b in IMG_BAD_HINTS):
        return False
    # host filter (len ak máme absolútne URL)
    try:
        host = re.search(r"https?://([^/]+)/", lu)
        if host:
            h = host.group(1)
            if not any(allow in h for allow in IMG_HOST_ALLOW):
                return False
    except Exception:
        pass
    return True

def best_from_srcset(srcset: str) -> Optional[str]:
    """
    Vyber najväčší zdroj zo srcset (podľa w-descriptora). Ak nie je, None.
    """
    if not srcset:
        return None
    best_u, best_w = None, -1
    for part in srcset.split(","):
        p = part.strip()
        if not p:
            continue
        bits = p.split()
        url = bits[0]
        w = -1
        if len(bits) >= 2 and bits[1].endswith("w"):
            try:
                w = int(re.sub(r"\D", "", bits[1]))
            except Exception:
                w = -1
        if w > best_w:
            best_w, best_u = w, url
    return best_u

def find_gallery_url(detail_url: str, detail_html: str) -> Optional[str]:
    soup = BeautifulSoup(detail_html, "html.parser")
    a = soup.find("a", href=GALLERY_LINK_RE)
    if a and a.get("href"):
        return urljoin(detail_url, a.get("href"))
    m = GALLERY_LINK_RE.search(detail_html or "")
    if m:
        return urljoin(detail_url, m.group(0))
    return None

def extract_gallery_images(gallery_html: str) -> List[str]:
    """
    Zober všetky <img> z galérie, preferuj najväčší variant zo srcset, zorad podľa alt _foto_N.
    """
    soup = BeautifulSoup(gallery_html, "html.parser")
    items: List[Tuple[int, str]] = []

    def order_from_img(img) -> int:
        alt = (img.get("alt") or "")
        m = re.search(r"_foto_(\d+)", alt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
        return 10**9

    # <img> s povolenými hostami
    for img in soup.find_all("img"):
        srcset = (img.get("srcset") or "").strip()
        src = (img.get("src") or img.get("data-src") or "").strip()
        cand = best_from_srcset(srcset) or src
        if not is_good_image_url(cand):
            continue
        items.append((order_from_img(img), cand))

    # prípadné <a href="..."> s priamymi linkami
    for a in soup.find_all("a", href=True):
        u = a["href"].strip()
        if is_good_image_url(u):
            child = a.find("img")
            order = order_from_img(child) if child else 10**9
            items.append((order, u))

    # zoradenie + dedup + rozbalenie _next/image url parametra ?url=
    items.sort(key=lambda x: x[0])
    seen: Set[str] = set()
    out: List[str] = []
    for _, u in items:
        if "/_next/image" in u and "url=" in u:
            try:
                inner = re.search(r"[?&]url=([^&]+)", u).group(1)
                u = unquote(inner)
            except Exception:
                pass
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# ------------------ Download + upload ------------------
def download_to_gcs(image_url: str, seq_global: Optional[int], listing_id: Optional[str], rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    """
    Vracia: (gcs_path, http_status, error_text)
    """
    folder = make_folder(seq_global, listing_id)

    try:
        resp = http_get(image_url, extra_headers={"Accept": "image/avif,image/webp,image/*,*/*;q=0.8"}, timeout=40)
        status = int(resp.status_code)
    except Exception as e:
        return ("", None, f"request_error:{e}")

    if status != 200:
        try:
            resp.close()
        except Exception:
            pass
        return ("", status, f"http_{status}")

    ext = ext_from_response(image_url, resp)
    filename = f"{rank:03d}{ext}"
    gcs_path = f"{BASE_PREFIX}/{folder}/{filename}"

    if gcs_blob_exists(gcs_path):
        try:
            _ = resp.content
        finally:
            resp.close()
        return (gcs_path, 200, None)

    try:
        content = resp.content
        ctype = resp.headers.get("Content-Type")
    finally:
        resp.close()

    try:
        blob = bucket.blob(gcs_path)
        blob.cache_control = "public, max-age=31536000"
        blob.upload_from_string(content, content_type=ctype)
        print(f"[OK] gs://{BUCKET_NAME}/{gcs_path}", flush=True)
        return (gcs_path, 200, None)
    except Exception as e:
        return ("", 200, f"gcs_upload_error:{e}")

# ------------------ BQ tabuľka (pk -> https url) ------------------
def ensure_pk_gcs_table(table_id: str):
    schema = [
        bigquery.SchemaField("pk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_url", "STRING", mode="REQUIRED"),  # HTTPS
        bigquery.SchemaField("downloaded_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_date", "DATE", mode="NULLABLE"),
    ]
    try:
        bq.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        _ = bq.create_table(table)
        print(f"[BQ] Created table {table_id}.")

def fetch_existing_pairs(table_id: str, pks: List[str]) -> Set[Tuple[str, str]]:
    if not pks:
        return set()
    qp = ",".join(["'{}'".format(pk.replace("'", "\\'")) for pk in pks])
    sql = f"SELECT pk, gcs_url FROM `{table_id}` WHERE pk IN ({qp})"
    try:
        df = bq.query(sql, location=LOCATION or None).result().to_dataframe()
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

# ------------------ Hlavná logika ------------------
def main():
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required.")
    if not BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET is required.")

    # 1) kandidáti z BQ
    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No listings in lookback window.")
        return

    cand = (
        cand
        .drop_duplicates(subset=["pk"], keep="first")
        .sort_values(["seq_global", "pk"], kind="stable")
        .head(MAX_LISTINGS)
    )

    out_records: List[Dict[str, Any]] = []

    for _, row in cand.iterrows():
        pk = str(row.get("pk"))
        listing_id = row.get("listing_id")
        seq_global = safe_int(row.get("seq_global"))
        detail_url = row.get("url")

        if not pk or not detail_url:
            continue

        # 2) detail -> nájdi link na galériu
        try:
            r = http_get(detail_url, timeout=40)
            if r.status_code != 200 or not r.text:
                print(f"[WARN] detail HTTP {r.status_code}: {detail_url}")
                continue
            detail_html = r.text
        except Exception as e:
            print(f"[WARN] detail error {detail_url} -> {e}")
            continue

        gallery_url = find_gallery_url(detail_url, detail_html)
        if not gallery_url:
            print(f"[WARN] gallery link not found: {detail_url}")
            continue

        # 3) galéria -> všetky obrázky
        try:
            gr = http_get(gallery_url, timeout=40)
            if gr.status_code != 200 or not gr.text:
                print(f"[WARN] gallery HTTP {gr.status_code}: {gallery_url}")
                continue
            image_urls = extract_gallery_images(gr.text)
        except Exception as e:
            print(f"[WARN] gallery error {gallery_url} -> {e}")
            continue

        if not image_urls:
            print(f"[INFO] no images in gallery: {gallery_url}")
            continue

        # 4) sťahuj & uploaduj max per listing
        used = 0
        rank = 1
        for u in image_urls:
            if used >= MAX_PER_LISTING:
                break
            gcs_path, http_status, err = download_to_gcs(u, seq_global, listing_id, rank)
            rank += 1
            if gcs_path:
                https_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{gcs_path}"
                out_records.append({
                    "pk": pk,
                    "gcs_url": https_url,  # HTTPS požadovaný
                    "downloaded_at": now_utc(),
                    "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
                })
                used += 1
            else:
                print(f"[WARN] skip {pk} img ({err or http_status})")

        # mierna pauza medzi inzerátmi
        time.sleep(random.uniform(0.15, 0.45))

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    # 5) Dedup pred zápisom do BQ
    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_PK_GCS_TABLE}"
    ensure_pk_gcs_table(table_id)

    pks = sorted({rec["pk"] for rec in out_records})
    existing = fetch_existing_pairs(table_id, pks)

    final_rows = [rec for rec in out_records if (rec["pk"], rec["gcs_url"]) not in existing]

    # 6) Insert
    insert_pk_gcs_rows(table_id, final_rows)

if __name__ == "__main__":
    main()
