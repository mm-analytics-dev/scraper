# -*- coding: utf-8 -*-
"""
Image downloader (detail-only, no-BQ-input) -> GCS + one BQ table (pk, https)

- Crawl SEARCH_URLS výsledkovky (bez JS) a pozbieraj /detail/ linky.
- Pre každý detail:
  - vytiahni URL fotiek priamo z HTML (img/src, data-*, srcset, og:image, a[href])
  - filtruj len reálne fotky (whitelist hostov, suffixy, CDN hinty)
  - stiahni max 60 ks s Accept image hlavickou (min. payload ~3 KB)
  - nahraj do GCS: images_v2/YYYYMMDD/<folder>/<NNN.ext>
  - zapíš do jednej BQ tabuľky len (pk, gcs_url, downloaded_at, batch_date); duplikáty preskoč

PK = listing_id, inak fallback na URL.

ENV:
  GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BQ_DATASET, BQ_LOCATION, GCS_BUCKET
  SEARCH_URLS="https://www.nehnutelnosti.sk/vysledky/okres-liptovsky-mikulas/predaj"
  RESULTS_MAX_PAGES=1
  MAX_LINKS_PER_PAGE=30
  IMAGES_MAX_LISTINGS=30
  IMAGES_MAX_PER_LISTING=60
  IMAGES_PK_GCS_TABLE=images_pk_gcs
  BATCH_DATE=YYYYMMDD (optional)
"""

import os
import re
import time
import random
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any, Set
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# Google
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound as GCPNotFound

# -------- ENV / defaults --------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET = os.getenv("BQ_DATASET", "").strip()
LOCATION = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME = os.getenv("GCS_BUCKET", "").strip()

SEARCH_URLS = [u.strip() for u in os.getenv(
    "SEARCH_URLS",
    "https://www.nehnutelnosti.sk/vysledky/okres-liptovsky-mikulas/predaj"
).split(",") if u.strip()]

RESULTS_MAX_PAGES = int(os.getenv("RESULTS_MAX_PAGES", "1"))
MAX_LINKS_PER_PAGE = int(os.getenv("MAX_LINKS_PER_PAGE", "30"))
MAX_LISTINGS = int(os.getenv("IMAGES_MAX_LISTINGS", "30"))
MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))
IMAGES_PK_GCS_TABLE = os.getenv("IMAGES_PK_GCS_TABLE", "images_pk_gcs").strip()

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
def batch_yyyymmdd() -> str:
    if BATCH_DATE_ENV:
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = batch_yyyymmdd()
BASE_PREFIX = f"images_v2/{BATCH_YYYYMMDD}"

BASE_HOST = "https://www.nehnutelnosti.sk"

# -------- HTTP session --------
SESSION = requests.Session()
_retry = Retry(
    total=4, connect=3, read=3, backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"]),
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
ACCEPT_LANG = [
    "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
    "cs-CZ,sk;q=0.9,en-US;q=0.7,en;q=0.6",
    "en-US,en;q=0.9,sk;q=0.8,cs;q=0.7",
]

def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANG),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": BASE_HOST + "/",
    }
    if extra: h.update(extra)
    if random.random() < 0.3:
        h["Connection"] = "close"
    return h

def http_get(url: str, timeout=25, extra_headers=None) -> requests.Response:
    return SESSION.get(url, headers=rand_headers(extra_headers), timeout=timeout)

def sleep_a_bit():
    time.sleep(random.uniform(0.2, 0.6))

# -------- GCS / BQ --------
if not PROJECT_ID or not DATASET or not BUCKET_NAME:
    raise RuntimeError("Missing one of required ENV: GCP_PROJECT_ID, BQ_DATASET, GCS_BUCKET")

bq = bigquery.Client(project=PROJECT_ID, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)

def table_full_id(name: str) -> str:
    return f"{PROJECT_ID}.{DATASET}.{name}"

def ensure_pk_gcs_table():
    """Create single output table if not exists."""
    full = table_full_id(IMAGES_PK_GCS_TABLE)
    schema = [
        bigquery.SchemaField("pk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("batch_date", "DATE", mode="NULLABLE"),
    ]
    try:
        bq.get_table(full)
    except GCPNotFound:
        bq.create_table(bigquery.Table(full, schema=schema))
        print(f"[BQ] Created table {full}.")
    return full

def fetch_existing_pairs(full_table_id: str, pks: List[str]) -> Set[Tuple[str, str]]:
    if not pks:
        return set()
    sql = f"""
    SELECT pk, gcs_url
    FROM `{full_table_id}`
    WHERE pk IN UNNEST(@pks)
    """
    try:
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pks", "STRING", pks)]
        )
        df = bq.query(sql, job_config=job_cfg, location=LOCATION or None).result() \
                .to_dataframe(create_bqstorage_client=False)
        return {(str(r["pk"]), str(r["gcs_url"])) for _, r in df.iterrows()}
    except Exception as e:
        print(f"[WARN] fetch_existing_pairs failed (ignored): {e}")
        return set()

def insert_rows(full_table_id: str, rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] Nothing to insert.")
        return
    df = pd.DataFrame(rows)
    df["downloaded_at"] = pd.to_datetime(df["downloaded_at"], utc=True)
    df["batch_date"] = pd.to_datetime(df["batch_date"]).dt.date
    job = bq.load_table_from_dataframe(
        df, full_table_id,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
        location=LOCATION or None
    )
    job.result()
    print(f"[BQ] Inserted {len(df)} rows into {full_table_id}.")

# -------- Listing links (results pages) --------
DETAIL_ABS_RE = re.compile(r"https?://www\.nehnutelnosti\.sk/detail/[^\s\"<>]+", re.I)
ID_FROM_URL_RE = re.compile(r"/detail/([^/]+)/")

def extract_detail_links(html: str, page_url: str, limit: Optional[int]) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen_ids = set()
    unique_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/detail/" not in href:
            continue
        abs_url = urljoin(page_url, href)
        m = ID_FROM_URL_RE.search(abs_url)
        lid = m.group(1) if m else abs_url
        if lid in seen_ids:
            continue
        seen_ids.add(lid)
        unique_urls.append(abs_url)
        if limit is not None and len(unique_urls) >= limit:
            break
    if not unique_urls:
        for m in DETAIL_ABS_RE.finditer(html):
            abs_url = m.group(0)
            lidm = ID_FROM_URL_RE.search(abs_url)
            lid = lidm.group(1) if lidm else abs_url
            if lid not in seen_ids:
                seen_ids.add(lid)
                unique_urls.append(abs_url)
                if limit is not None and len(unique_urls) >= limit:
                    break
    return unique_urls

def crawl_candidate_listing_urls() -> List[str]:
    urls: List[str] = []
    total_cap = MAX_LISTINGS if MAX_LISTINGS and MAX_LISTINGS > 0 else None

    for base in SEARCH_URLS:
        for page in range(1, RESULTS_MAX_PAGES + 1):
            if total_cap is not None and len(urls) >= total_cap:
                break
            page_url = base if page == 1 else f"{base}{'&' if '?' in base else '?'}page={page}"
            r = http_get(page_url)
            html = r.text
            if r.status_code == 404:
                break
            links = extract_detail_links(html, page_url, limit=MAX_LINKS_PER_PAGE or None)
            if not links:
                break
            for u in links:
                if total_cap is not None and len(urls) >= total_cap:
                    break
                if u not in urls:
                    urls.append(u)
            sleep_a_bit()
    return urls

# -------- Image filters (detail logic) --------
IMG_HOST_ALLOW = ("img.unitedclassifieds.sk", "img.nehnutelnosti.sk")
IMG_SUFFIX_ALLOW = (".jpg", ".jpeg", ".webp", ".png", ".avif", ".jfif", ".gif")
IMG_BAD_HINTS = ("data:image/", "beacon", "pixel", "ads", "analytics")

def _looks_like_real_image(abs_url: str) -> bool:
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
    if "_fss" in path or "?st=" in u:
        return True
    return False

def extract_image_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    candidates: List[str] = []

    for img in soup.find_all("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy"):
            val = img.get(attr)
            if not val:
                continue
            candidates.append(urljoin(base_url, val))
        srcset = img.get("srcset") or ""
        if srcset:
            for part in srcset.split(","):
                cand = part.strip().split(" ")[0]
                if cand:
                    candidates.append(urljoin(base_url, cand))

    for sel in ("meta[property='og:image']", "meta[name='twitter:image']"):
        for m in soup.select(sel):
            val = m.get("content")
            if val:
                candidates.append(urljoin(base_url, val))

    for a in soup.find_all("a", href=True):
        candidates.append(urljoin(base_url, a["href"]))

    # filter + dedup
    seen: Set[str] = set()
    out: List[str] = []
    for u in candidates:
        if u and _looks_like_real_image(u) and u not in seen:
            seen.add(u)
            out.append(u)
    return out

# -------- Download & GCS --------
def gcs_blob_exists(path: str) -> bool:
    blob = bucket.blob(path)
    return blob.exists(client=gcs)

def ext_from_response(url: str, resp: requests.Response) -> str:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/avif" in ctype: return ".avif"
    if "image/webp" in ctype: return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype: return ".jpg"
    if "image/png" in ctype: return ".png"
    if "image/gif" in ctype: return ".gif"
    u = url.lower().split("?", 1)[0]
    for suf in IMG_SUFFIX_ALLOW:
        if u.endswith(suf):
            return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def folder_for(listing_id: Optional[str], idx_seq: int) -> str:
    lid_part = (listing_id or "NA").lower()
    return f"{idx_seq:06d}_{re.sub(r'[^a-z0-9_-]+','-', lid_part)}"

def download_to_gcs(image_url: str, folder: str, rank: int) -> Tuple[str, Optional[int], Optional[str]]:
    try:
        resp = SESSION.get(
            image_url, timeout=30,
            headers=rand_headers({"Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8"}),
            stream=True
        )
        status = int(resp.status_code)
    except Exception as e:
        return ("", None, f"request_error:{e}")

    if status != 200:
        try: resp.close()
        except: pass
        print(f"[IMG] skip (HTTP {status}) {image_url}")
        return ("", status, f"http_{status}")

    try:
        content = resp.content
        if not content or len(content) < 3000:
            return ("", 200, "too_small")
        ext = ext_from_response(image_url, resp)
        filename = f"{rank:03d}{ext}"
        gcs_path = f"{BASE_PREFIX}/{folder}/{filename}"

        if gcs_blob_exists(gcs_path):
            print(f"[IMG] uložené (exist.) gs://{BUCKET_NAME}/{gcs_path}")
            return (gcs_path, 200, None)
    finally:
        resp.close()

    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=None)
        print(f"[IMG] uložené: gs://{BUCKET_NAME}/{gcs_path}")
        return (gcs_path, 200, None)
    except Exception as e:
        print(f"[IMG] chyba {image_url} -> {e}")
        return ("", 200, f"gcs_upload_error:{e}")

# -------- Detail → images --------
def listing_id_from_url(u: str) -> Optional[str]:
    m = ID_FROM_URL_RE.search(u or "")
    return m.group(1) if m else None

def fetch_detail_image_urls(listing_url: str) -> List[str]:
    try:
        r = http_get(listing_url)
        if r.status_code != 200:
            print(f"[WARN] detail {listing_url} → HTTP {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        return extract_image_urls(soup, listing_url)
    except Exception as e:
        print(f"[WARN] detail error {listing_url}: {e}")
        return []

# -------- MAIN --------
def main():
    # 0) priprav výstupnú tabuľku
    table_id_full = ensure_pk_gcs_table()

    # 1) vycrawluj kandidátov (detail linky)
    listing_urls = crawl_candidate_listing_urls()
    if not listing_urls:
        print("[INFO] Nenašiel som žiadne inzeráty na výsledkovkách. Skontroluj SEARCH_URLS.")
        return

    # 2) existujúce páry pre dedup zápisu
    #    (najprv zober pks podľa zoznamu, aby sme zbytočne netlačili duplicitné HTTPS)
    pks_seed = []
    for u in listing_urls:
        lid = listing_id_from_url(u)
        pk = lid if lid else u
        pks_seed.append(pk)
    existing_pairs = fetch_existing_pairs(table_id_full, list(sorted(set(pks_seed))))

    # 3) stiahnutie + upload + príprava BQ riadkov
    out_rows: List[Dict[str, Any]] = []
    total_downloaded = 0

    for idx, listing_url in enumerate(listing_urls, start=1):
        lid = listing_id_from_url(listing_url)
        pk = lid if lid else listing_url

        urls = fetch_detail_image_urls(listing_url)
        if not urls:
            print(f"[WARN] žiadne URL fotiek pre {pk} ({listing_url})")
            continue

        urls = urls[:max(1, int(MAX_PER_LISTING or 60))]
        folder = folder_for(lid, idx)

        rank = 1
        for img_url in urls:
            gcs_path, http_status, err = download_to_gcs(img_url, folder, rank)
            if gcs_path:
                https_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{gcs_path}"
                # dedup proti existujúcim párom
                if (pk, https_url) not in existing_pairs:
                    out_rows.append({
                        "pk": pk,
                        "gcs_url": https_url,
                        "downloaded_at": pd.Timestamp.now(tz=timezone.utc),
                        "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
                    })
                    existing_pairs.add((pk, https_url))
                total_downloaded += 1
            else:
                # už zalogované v download_to_gcs
                pass
            rank += 1
            time.sleep(random.uniform(0.03, 0.12))
        sleep_a_bit()

    if not out_rows:
        print("[INFO] Nothing new to insert. Exiting.")
        return

    insert_rows(table_id_full, out_rows)
    print(f"[DONE] listings: {len(listing_urls)}, images uploaded: {total_downloaded}, new rows: {len(out_rows)}")

if __name__ == "__main__":
    main()
