# -*- coding: utf-8 -*-
"""
Images downloader (v2.2) → GCS + BigQuery (PK → HTTPS GCS URL)

- Z BQ vyberie pk/listing_id/seq_global/url za posledné dni.
- Z každého detailu vytiahne celú fotogalériu:
    1) inline IMG/SRCSET v detaile (iba hosty img.nehnutelnosti.sk / img.unitedclassifieds.sk a len ak URL obsahuje /foto/)
    2) ak existuje, otvorí /detail/galeria/foto/<listing_id>/ a vytiahne všetky obrázky
    3) regexom prejde <script> bloky a dozbiera URL na fotky (len /foto/)
- Stiahne do GCS prefixu: images_v2/YYYYMMDD/{FOLDER}/{NNN.ext}
- Zapíše do BQ tabuľky {BQ_DATASET}.{IMAGES_PK_GCS_TABLE} páry:
      pk STRING, gcs_url STRING (HTTPS), downloaded_at TIMESTAMP, batch_date DATE
  (pre jeden pk môže byť veľa riadkov – 1 riadok = 1 fotka)
- Nikdy duplicitne: pred zápisom skontroluje existujúce (pk, gcs_url).

ENV (Secrets/Variables v Actions):
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  IMAGES_LOOKBACK_DAYS=3
  IMAGES_MAX_LISTINGS=15
  IMAGES_MAX_PER_LISTING=60
  BATCH_DATE=YYYYMMDD (optional)
  IMAGES_PK_GCS_TABLE=images_pk_gcs (optional; default)
"""

import os
import re
import time
import json
import math
import random
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from google.cloud import bigquery
from google.cloud import storage

# ---------------- ENV ----------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET = os.getenv("BQ_DATASET", "realestate_v2").strip()
LOCATION = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME = os.getenv("GCS_BUCKET", "").strip()

LOOKBACK_DAYS = int(os.getenv("IMAGES_LOOKBACK_DAYS", "3"))
MAX_LISTINGS = int(os.getenv("IMAGES_MAX_LISTINGS", "15"))
MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
IMAGES_PK_GCS_TABLE = os.getenv("IMAGES_PK_GCS_TABLE", "images_pk_gcs").strip()

# ---------------- Helpers ----------------
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

# ---------------- HTTP session ----------------
SESSION = requests.Session()
_retry = Retry(
    total=4, connect=3, read=3, backoff_factor=1.2,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"])
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=16, pool_maxsize=16)
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
        "Accept-Language": "sk-SK,sk;q=0.9,cs;q=0.7,en-US;q=0.6,en;q=0.5",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra: h.update(extra)
    if random.random() < 0.25:
        h["Connection"] = "close"
    return h

def http_get(url: str, timeout=30, extra_headers=None) -> requests.Response:
    return SESSION.get(url, timeout=timeout, headers=rand_headers(extra_headers))

# ---------------- GCS / BQ clients ----------------
bq = bigquery.Client(project=PROJECT_ID or None, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID or None)
bucket = gcs.bucket(BUCKET_NAME) if BUCKET_NAME else None

# ---------------- SQL candidates ----------------
# primárny zdroj – meta tabuľka s URL detailu
SQL_CANDIDATES_PRIMARY = f"""
SELECT pk, listing_id, seq_global, url
FROM `{PROJECT_ID}.{DATASET}.listings_meta_daily_v2`
WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND url LIKE 'https://www.nehnutelnosti.sk/detail/%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY batch_ts DESC) = 1
ORDER BY MIN(seq_global)
LIMIT @max_listings
"""

# fallback – ak by primárna tab. neexistovala
SQL_CANDIDATES_FALLBACK = f"""
SELECT pk, listing_id, seq_global, url
FROM `{PROJECT_ID}.{DATASET}.listings_meta_daily`
WHERE batch_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @lookback_days DAY)
  AND url LIKE 'https://www.nehnutelnosti.sk/detail/%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY batch_ts DESC) = 1
ORDER BY MIN(seq_global)
LIMIT @max_listings
"""

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
        return query_df(SQL_CANDIDATES_PRIMARY, params)
    except Exception as e:
        print(f"[WARN] primary candidates failed → fallback ({e})", flush=True)
        return query_df(SQL_CANDIDATES_FALLBACK, params)

# ---------------- Path / naming helpers ----------------
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

# ---------------- Image URL extraction ----------------
IMG_HOST_ALLOW = ("img.nehnutelnosti.sk", "img.unitedclassifieds.sk")
BAD_HINTS = ("logo", "sprite", "icon", "mail.", "lightbulb", "fb.", "gdpr", "analytics", "pixel")

def absolutize(base_url: str, u: str) -> Optional[str]:
    if not u: return None
    absu = urljoin(base_url, u)
    pr = urlparse(absu)
    if not pr.scheme or not pr.netloc:
        return None
    return absu

def is_real_photo(u: str) -> bool:
    try:
        pr = urlparse(u)
        host_ok = any(h in pr.netloc for h in IMG_HOST_ALLOW)
        path = (pr.path or "").lower()
        if not host_ok: return False
        if "/foto/" not in path: return False
        if any(b in path for b in BAD_HINTS): return False
        return True
    except Exception:
        return False

def pick_best_from_srcset(srcset: str, base_url: str) -> Optional[str]:
    # "url1 300w, url2 600w, url3 1200w"
    best = None; bestw = -1
    for part in srcset.split(","):
        tok = part.strip().split()
        if not tok: continue
        u = absolutize(base_url, tok[0])
        if not u: continue
        w = -1
        if len(tok) > 1 and tok[1].endswith("w"):
            try: w = int(tok[1][:-1])
            except: w = -1
        if w > bestw:
            bestw = w; best = u
    return best

SCRIPT_IMG_RE = re.compile(
    r"https?://(?:img\.nehnutelnosti\.sk|img\.unitedclassifieds\.sk)/[^\s\"'<>]+",
    re.I
)

def collect_inline_photos(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    # <img>
    for img in soup.find_all("img"):
        # prefer srcset (najväčší)
        srcset = img.get("srcset") or img.get("data-srcset") or ""
        if srcset:
            u = pick_best_from_srcset(srcset, base_url)
            if u and is_real_photo(u): out.append(u)
        for attr in ("src", "data-src", "data-original", "data-lazy"):
            u = absolutize(base_url, img.get(attr))
            if u and is_real_photo(u): out.append(u)
    # <source srcset>
    for src in soup.find_all("source"):
        srcset = src.get("srcset") or src.get("data-srcset")
        if srcset:
            u = pick_best_from_srcset(srcset, base_url)
            if u and is_real_photo(u): out.append(u)
    # odkazy priamo na obrázky (zriedka)
    for a in soup.find_all("a", href=True):
        u = absolutize(base_url, a["href"])
        if u and is_real_photo(u): out.append(u)
    # zo skriptov (iba veľké fotky z /foto/)
    in_scripts = SCRIPT_IMG_RE.findall(html or "")
    for u in in_scripts:
        if is_real_photo(u): out.append(u)
    # dedup – zachovať poradie
    uniq = []
    seen = set()
    for u in out:
        key = u.split("?")[0]   # bez query pre lepší dedup
        if key not in seen:
            seen.add(key); uniq.append(u)
    return uniq

def find_gallery_link(html: str, base_url: str, listing_id: Optional[str]) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    # priama kotva
    a = soup.find("a", href=re.compile(r"/detail/galeria/foto/", re.I))
    if a and a.get("href"):
        return absolutize(base_url, a["href"])
    # fallback cez pattern s listing_id
    if listing_id:
        cand = f"/detail/galeria/foto/{listing_id}/"
        return absolutize(base_url, cand)
    return None

def extract_all_photos(detail_url: str, listing_id: Optional[str]) -> List[str]:
    photos: List[str] = []

    # 1) detail
    r = http_get(detail_url)
    if r.status_code != 200:
        print(f"[WARN] detail HTTP {r.status_code}: {detail_url}", flush=True)
        return photos
    html = r.text

    # inline
    photos.extend(collect_inline_photos(html, detail_url))

    # 2) galéria (ak vieme link)
    gal_url = find_gallery_link(html, detail_url, listing_id)
    if gal_url:
        try:
            rg = http_get(gal_url, extra_headers={"Accept": "text/html"})
            if rg.status_code == 200:
                photos.extend(collect_inline_photos(rg.text, gal_url))
            else:
                print(f"[WARN] gallery HTTP {rg.status_code}: {gal_url}", flush=True)
        except Exception as e:
            print(f"[WARN] gallery fetch error {gal_url} -> {e}", flush=True)
    else:
        print(f"[WARN] gallery link not found: {detail_url}", flush=True)

    # dedup + max per listing
    uniq = []
    seen = set()
    for u in photos:
        # prefer „väčšiu“ verziu – ak má query param w=..., môžeme zvýšiť na 1600
        try:
            pr = urlparse(u)
            q = dict(parse_qsl(pr.query))
            if "w" in q:
                try:
                    w = int(q["w"])
                    if w < 1600:
                        q["w"] = "1600"
                        pr = pr._replace(query=urlencode(q))
                        u = urlunparse(pr)
                except: pass
        except: pass

        key = u.split("?")[0]
        if key not in seen:
            seen.add(key); uniq.append(u)
        if len(uniq) >= MAX_PER_LISTING:
            break
    return uniq

# ---------------- Download + upload ----------------
def https_gcs_url(path: str) -> str:
    # https URL použiteľná v BQ/Lookeri
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{path}"

def download_one(image_url: str, gcs_path: str) -> Tuple[bool, Optional[str]]:
    """Vracia (success, error_text)."""
    if gcs_blob_exists(gcs_path):
        return True, None
    try:
        resp = SESSION.get(image_url, timeout=35, headers=rand_headers({"Accept": "image/avif,image/webp,image/*,*/*;q=0.8"}), stream=True)
        status = int(resp.status_code)
    except Exception as e:
        return False, f"request_error:{e}"
    if status != 200:
        try: resp.close()
        except: pass
        return False, f"http_{status}"
    try:
        content = resp.content
        ctype = resp.headers.get("Content-Type")
    finally:
        resp.close()
    try:
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(content, content_type=ctype)
        return True, None
    except Exception as e:
        return False, f"gcs_upload_error:{e}"

def download_listing_images(pk: str, seq_global: Optional[int], listing_id: Optional[str], detail_url: str) -> List[str]:
    urls = extract_all_photos(detail_url, listing_id)
    if not urls:
        return []
    folder = make_folder(seq_global, listing_id)
    saved_https_urls: List[str] = []
    rank = 1
    for u in urls:
        # názov podľa poradia + detekcia prípony
        # najprv si „nasucho“ zistíme príponu GET headerom? – urobíme inline pri prvom requeste
        ext = ".webp"
        # aby sme vedeli názov hneď, skúsime uhádnuť z URL
        low = u.lower().split("?", 1)[0]
        for sfx in (".jpg", ".jpeg", ".png", ".webp", ".avif", ".gif"):
            if low.endswith(sfx):
                ext = ".jpg" if sfx == ".jpeg" else sfx
                break
        gcs_path = f"{BASE_PREFIX}/{folder}/{rank:03d}{ext}"
        ok, err = download_one(u, gcs_path)
        if ok:
            saved_https_urls.append(https_gcs_url(gcs_path))
            rank += 1
        else:
            print(f"[WARN] skip {pk} img ({err})", flush=True)
        # jemná pauza
        time.sleep(random.uniform(0.05, 0.12))
        if rank > MAX_PER_LISTING:
            break
    return saved_https_urls

# ---------------- BQ: ensure table + insert ----------------
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
    """
    Bezpečne načíta existujúce dvojice (pk, gcs_url) pre dané PKs.
    Používa parametrizovaný dotaz s UNNEST, žiadne ručné skladanie IN (...) reťazca.
    """
    if not pks:
        return set()

    sql = f"""
    SELECT pk, gcs_url
    FROM `{table_id}`
    WHERE pk IN UNNEST(@pks)
    """

    try:
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pks", "STRING", pks)]
        )
        df = bq.query(sql, job_config=job_cfg, location=LOCATION or None) \
               .result().to_dataframe(create_bqstorage_client=False)
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

# ---------------- Main ----------------
def main():
    if not PROJECT_ID:
        raise RuntimeError("GCP_PROJECT_ID is required.")
    if not BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET is required.")

    cand = fetch_candidates()
    if cand.empty:
        print("[INFO] No candidates (check lookback/limits).")
        return

    # stabilné poradie, dedup pk
    cand = (
        cand
        .drop_duplicates(subset=["pk"], keep="first")
        .sort_values(["seq_global", "pk"], kind="stable")
    )

    out_records: List[Dict[str, Any]] = []

    for _, r in cand.iterrows():
        pk = str(r.get("pk"))
        listing_id = r.get("listing_id")
        seq_global = safe_int(r.get("seq_global"))
        detail_url = r.get("url")
        if not pk or not detail_url:
            continue

        saved_https = download_listing_images(pk, seq_global, listing_id, detail_url)
        for https_url in saved_https:
            out_records.append({
                "pk": pk,
                "gcs_url": https_url,
                "downloaded_at": now_utc(),
                "batch_date": pd.to_datetime(BATCH_YYYYMMDD, format="%Y%m%d"),
            })

    if not out_records:
        print("[INFO] Nothing downloaded/uploaded. Exiting.")
        return

    table_id = f"{PROJECT_ID}.{DATASET}.{IMAGES_PK_GCS_TABLE}"
    ensure_pk_gcs_table(table_id)

    pks = sorted({rec["pk"] for rec in out_records})
    existing = fetch_existing_pairs(table_id, pks)

    final_rows = [rec for rec in out_records if (rec["pk"], rec["gcs_url"]) not in existing]
    insert_pk_gcs_rows(table_id, final_rows)

if __name__ == "__main__":
    main()
