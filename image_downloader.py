# -*- coding: utf-8 -*-
"""
Images downloader (v3) -> GCS + BigQuery (iba pk, gcs_url)

Čo robí:
- Z BQ tab. {BQ_DATASET}.listings_master načíta záznamy (pk, url) podľa filtrov.
- Z DETAIL stránky pre každý pk vyextrahuje URL obrázkov, stiahne a uloží do GCS:
  gs://{GCS_BUCKET}/{GCS_PREFIX}/{safe_pk}/{NNN.ext}
- Je idempotentný: ak blob už existuje, preskočí upload.
- Do BQ {BQ_DATASET}.{DEST_TABLE} (default 'images_gcs') zapíše iba:
  pk STRING, gcs_url STRING. Pred loadom deduplikuje (pk, gcs_url).

ENV:
  GOOGLE_APPLICATION_CREDENTIALS=./sa.json
  GCP_PROJECT_ID=...
  BQ_DATASET=realestate_v2
  BQ_LOCATION=EU
  GCS_BUCKET=...
  GCS_PREFIX=images_v3                      (voliteľné)
  DEST_TABLE=images_gcs                     (voliteľné)
  SOURCE_LIMIT=0                            (0 = bez limitu)
  ACTIVE_ONLY=true|false                    (default true)
  LOOKBACK_DAYS=0                           (0 = bez filtra)
  PER_LISTING_MAX=60                        (max fotiek na inzerát)
  REQ_TIMEOUT=30
"""

import os, re, time, random, unicodedata
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from google.cloud import bigquery
from google.cloud import storage

# --------- ENV ---------
PROJECT_ID      = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET         = os.getenv("BQ_DATASET", "realestate_v2").strip()
LOCATION        = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME     = os.getenv("GCS_BUCKET", "").strip()
GCS_PREFIX      = os.getenv("GCS_PREFIX", "images_v3").strip()
DEST_TABLE      = os.getenv("DEST_TABLE", "images_gcs").strip()

SOURCE_LIMIT    = int(os.getenv("SOURCE_LIMIT", "0"))
ACTIVE_ONLY     = os.getenv("ACTIVE_ONLY", "true").lower() == "true"
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "0"))
PER_LISTING_MAX = int(os.getenv("PER_LISTING_MAX", "60"))
REQ_TIMEOUT     = int(os.getenv("REQ_TIMEOUT", "30"))

if not PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID is required.")
if not BUCKET_NAME:
    raise RuntimeError("GCS_BUCKET is required.")

# --------- Clients ---------
bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
gcs = storage.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)

# --------- HTTP ---------
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
        "Accept-Language": "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
        "Cache-Control": "no-cache", "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if extra: h.update(extra)
    return h

SESSION = requests.Session()
_retry = Retry(total=4, connect=3, read=3, backoff_factor=1.2,
               status_forcelist=(429,500,502,503,504),
               allowed_methods=frozenset(["GET","HEAD"]))
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

def http_get(url: str) -> requests.Response:
    return SESSION.get(url, headers=rand_headers(), timeout=REQ_TIMEOUT)

# --------- Utils ---------
def strip_diacritics(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def safe_pk(pk: str) -> str:
    """bezpečný názov priečinka pre pk"""
    if pk is None: return "na"
    s = strip_diacritics(str(pk)).lower()
    s = re.sub(r"[^a-z0-9_-]+", "-", s).strip("-")
    return s or "na"

IMG_HOST_ALLOW = ("img.unitedclassifieds.sk", "img.nehnutelnosti.sk")
IMG_SUFFIX_ALLOW = (".jpg",".jpeg",".webp",".png",".avif",".jfif",".gif")
IMG_BAD_HINTS = ("data:image/","beacon","pixel","ads","analytics")

def looks_like_image(u: str) -> bool:
    if not u: return False
    L = u.lower()
    if any(b in L for b in IMG_BAD_HINTS): return False
    if not any(h in L for h in IMG_HOST_ALLOW): return False
    if any(L.split("?")[0].endswith(s) for s in IMG_SUFFIX_ALLOW): return True
    if "_fss" in L or "?st=" in L: return True
    return False

def extract_image_urls_from_detail(detail_html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(detail_html, "html.parser")
    seen, out = set(), []

    def _add(u: str):
        if not u: return
        if looks_like_image(u) and u not in seen:
            seen.add(u); out.append(u)

    for img in soup.find_all("img"):
        for attr in ("src","data-src","data-original","data-lazy"):
            _add(img.get(attr))
        srcset = img.get("srcset") or ""
        if srcset:
            for part in srcset.split(","):
                _add(part.strip().split(" ")[0])

    for sel in ["meta[property='og:image']","meta[name='twitter:image']"]:
        for m in soup.select(sel):
            _add(m.get("content"))

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href: continue
        if not href.startswith("http"):
            href = urljoin(page_url, href)
        _add(href)

    return out

def ext_from_headers(url: str, resp: requests.Response) -> str:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    if "image/webp" in ctype: return ".webp"
    if "image/jpeg" in ctype or "image/jpg" in ctype: return ".jpg"
    if "image/png" in ctype: return ".png"
    if "image/avif" in ctype: return ".avif"
    if "image/gif" in ctype: return ".gif"
    u = url.lower().split("?")[0]
    for suf in IMG_SUFFIX_ALLOW:
        if u.endswith(suf): return ".jpg" if suf == ".jpeg" else suf
    return ".webp"

def blob_exists(path: str) -> bool:
    return bucket.blob(path).exists(client=gcs)

def upload_bytes(path: str, content: bytes, content_type: str | None):
    blob = bucket.blob(path)
    blob.upload_from_string(content, content_type=content_type)

# --------- Source (pk, url) ---------
def fetch_source_from_master() -> pd.DataFrame:
    where = []
    if ACTIVE_ONLY:
        where.append("active = TRUE")
    if LOOKBACK_DAYS > 0:
        where.append("last_seen >= DATE_SUB(CURRENT_DATE(), INTERVAL @look INT64 DAY)")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = " LIMIT @lim" if SOURCE_LIMIT > 0 else ""

    sql = f"""
    SELECT pk, url
    FROM `{PROJECT_ID}.{DATASET}.listings_master`
    {where_sql}
    ORDER BY last_seen DESC
    {limit_sql}
    """
    params = []
    if LOOKBACK_DAYS > 0:
        params.append(bigquery.ScalarQueryParameter("look", "INT64", LOOKBACK_DAYS))
    if SOURCE_LIMIT > 0:
        params.append(bigquery.ScalarQueryParameter("lim", "INT64", SOURCE_LIMIT))

    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=LOCATION)
    df = job.result().to_dataframe(create_bqstorage_client=False)
    df["pk"] = df["pk"].astype("string")
    df["url"] = df["url"].astype("string")
    df = df.dropna(subset=["pk","url"]).drop_duplicates(subset=["pk"])
    return df

# --------- Core ---------
def process_listing(pk: str, url: str, per_listing_max: int) -> list[str]:
    """Vracia zoznam GCS URL (gs://...) pre daný pk, po uploade (alebo skip ak existuje)."""
    try:
        r = http_get(url)
        if r.status_code != 200:
            print(f"[WARN] detail {url} -> {r.status_code}")
            return []
        html = r.text
    except Exception as e:
        print(f"[WARN] GET detail fail {url}: {e}")
        return []

    imgs = extract_image_urls_from_detail(html, url)
    if per_listing_max and len(imgs) > per_listing_max:
        imgs = imgs[:per_listing_max]

    folder = f"{GCS_PREFIX}/{safe_pk(pk)}"
    gcs_urls = []
    rank = 1

    for img_url in imgs:
        try:
            resp = SESSION.get(img_url, headers=rand_headers(), timeout=REQ_TIMEOUT, stream=True)
        except Exception as e:
            print(f"[WARN] img GET fail {img_url}: {e}")
            continue
        if resp.status_code != 200:
            print(f"[WARN] img {img_url} -> {resp.status_code}")
            try: resp.close()
            except: pass
            continue

        ext = ext_from_headers(img_url, resp)
        filename = f"{rank:03d}{ext}"
        gcs_path = f"{folder}/{filename}"
        full_url = f"gs://{BUCKET_NAME}/{gcs_path}"

        # idempotentne: ak blob existuje, neuploaduj znova
        if not blob_exists(gcs_path):
            try:
                content = resp.content
                ctype = resp.headers.get("Content-Type")
            finally:
                resp.close()
            try:
                upload_bytes(gcs_path, content, ctype)
                print(f"[OK] {full_url}")
            except Exception as e:
                print(f"[WARN] upload fail {full_url}: {e}")
                continue
        else:
            try:
                _ = resp.content
            finally:
                resp.close()

        gcs_urls.append(full_url)
        rank += 1
        time.sleep(random.uniform(0.05, 0.15))

    return gcs_urls

# --------- Sink (BQ pk,gcs_url) ---------
def write_pk_gcs_rows(rows: list[dict]):
    if not rows:
        print("[INFO] Nothing to write.")
        return

    df = pd.DataFrame(rows, columns=["pk","gcs_url"]).dropna()
    # dedup protection
    df = df.drop_duplicates(subset=["pk","gcs_url"])

    table_id = f"{PROJECT_ID}.{DATASET}.{DEST_TABLE}"
    schema = [
        bigquery.SchemaField("pk", "STRING"),
        bigquery.SchemaField("gcs_url", "STRING"),
    ]
    job_cfg = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_cfg, location=LOCATION)
    job.result()
    print(f"[BQ] Loaded {len(df)} rows into {table_id}.")

# --------- Main ---------
def main():
    src = fetch_source_from_master()
    if src.empty:
        print("[INFO] No listings to process.")
        return

    out_rows: list[dict] = []
    for _, r in src.iterrows():
        pk = str(r["pk"])
        url = str(r["url"])
        gcs_urls = process_listing(pk, url, PER_LISTING_MAX)
        for u in gcs_urls:
            out_rows.append({"pk": pk, "gcs_url": u})

    write_pk_gcs_rows(out_rows)

if __name__ == "__main__":
    main()
