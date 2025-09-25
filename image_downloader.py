# -*- coding: utf-8 -*-
"""
Playwright → GCS + BigQuery (1 tabuľka: pk -> gcs_url)

ZMENY / HARDENING:
- Nová stránka (tab) pre každý listing → žiadne leaknuté event listenery.
- goto_with_retries() s backoffom (ošetrený ERR_ABORTED).
- Fallback na httpx, ak Playwright nedá resp.body() (No resource id / cache / preruš. odpoveď).
- Kopíruje cookies + Referer do httpx requestu.
- Cache busting cez hlavičky (Cache-Control: no-cache).
- Priebežný flush do BQ po každom inzeráte.
- Dedup na úrovni GCS path a (pk, gcs_url).

ENV (vyžadované):
  GCP_PROJECT_ID
  BQ_DATASET
  BQ_LOCATION           (napr. "EU")
  GCS_BUCKET
  GOOGLE_APPLICATION_CREDENTIALS  (path k SA JSON)

Konfigurácia (ENV s defaultami):
  BASE_URL=https://www.nehnutelnosti.sk/vysledky/okres-liptovsky-mikulas/predaj
  MAX_PAGES=40
  MAX_PER_PAGE=100
  IMAGES_MAX_PER_LISTING=60
  BQ_TABLE=images_pk_gcs
  BATCH_DATE=YYYYMMDD (voliteľné, inak UTC today)
"""

import os, re, json, asyncio, time, random
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit
from typing import Dict, Any, List, Set, Tuple

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright._impl._errors import Error as PWError

# ---------- GCP ----------
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound as GCPNotFound

# ---------- Konštanty / ENV ----------
BASE_URL = os.getenv("BASE_URL", "https://www.nehnutelnosti.sk/vysledky/okres-liptovsky-mikulas/predaj")

MAX_PAGES = int(os.getenv("MAX_PAGES", "40"))
MAX_PER_PAGE = int(os.getenv("MAX_PER_PAGE", "100"))
IMAGES_MAX_PER_LISTING = int(os.getenv("IMAGES_MAX_PER_LISTING", "60"))

PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "").strip()
DATASET      = os.getenv("BQ_DATASET", "").strip()
LOCATION     = os.getenv("BQ_LOCATION", "EU").strip()
BUCKET_NAME  = os.getenv("GCS_BUCKET", "").strip()
BQ_TABLE     = os.getenv("BQ_TABLE", "images_pk_gcs").strip()

if not PROJECT_ID or not DATASET or not BUCKET_NAME:
    raise RuntimeError("Missing ENV: GCP_PROJECT_ID / BQ_DATASET / GCS_BUCKET")

BATCH_DATE_ENV = os.getenv("BATCH_DATE", "").strip()
def batch_yyyymmdd() -> str:
    if BATCH_DATE_ENV:
        if not re.fullmatch(r"\d{8}", BATCH_DATE_ENV):
            raise ValueError("BATCH_DATE must be YYYYMMDD")
        return BATCH_DATE_ENV
    return datetime.utcnow().strftime("%Y%m%d")

BATCH_YYYYMMDD = batch_yyyymmdd()
GCS_PREFIX = f"images_v3/{BATCH_YYYYMMDD}"

# ---------- Selektory / regexy ----------
DETAIL_ID_RE = re.compile(r"/detail/([^/]+)/")
ALLOW_HOSTS = (
    "img.nehnutelnosti.sk",
    "img.unitedclassifieds.sk",
    # pridaj, ak sa objaví iná CDN:
    # "images.nehnutelnosti.sk",
)

def listing_id_from_url(u: str) -> str | None:
    m = DETAIL_ID_RE.search(u or "")
    return m.group(1) if m else None

def file_ext_from_url_or_ct(u: str, content_type: str | None) -> str:
    path = urlsplit(u).path.lower()
    for ext in (".jpg",".jpeg",".png",".webp",".avif",".gif",".jfif"):
        if path.endswith(ext): return ".jpg" if ext==".jpeg" else ext
    if content_type:
        c = content_type.lower()
        if "jpeg" in c: return ".jpg"
        if "png"  in c: return ".png"
        if "webp" in c: return ".webp"
        if "avif" in c: return ".avif"
        if "gif"  in c: return ".gif"
    return ".webp"

def dedup_key(url: str) -> str:
    sp = urlsplit(url)
    return f"{sp.netloc}{sp.path}"

# ---------- GCP klienti ----------
bq = bigquery.Client(project=PROJECT_ID, location=LOCATION or None)
gcs = storage.Client(project=PROJECT_ID)
bucket = gcs.bucket(BUCKET_NAME)

def bq_table_full() -> str:
    return f"{PROJECT_ID}.{DATASET}.{BQ_TABLE}"

def ensure_bq_table() -> str:
    full = bq_table_full()
    schema = [
        bigquery.SchemaField("pk", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("gcs_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("downloaded_at", "TIMESTAMP"),
        bigquery.SchemaField("batch_date", "DATE"),
    ]
    try:
        bq.get_table(full)
    except GCPNotFound:
        bq.create_table(bigquery.Table(full, schema=schema))
        print(f"[BQ] created {full}")
    return full

async def fetch_existing_pairs_for_pks(pks: List[str]) -> Set[Tuple[str, str]]:
    """Na minimalizáciu INSERT duplicit načítame existujúce páry (pk, gcs_url) pre zadané PKs."""
    if not pks:
        return set()
    sql = f"""
      SELECT pk, gcs_url
      FROM `{bq_table_full()}`
      WHERE pk IN UNNEST(@pks)
    """
    try:
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("pks", "STRING", list(sorted(set(pks))))],
            priority=bigquery.QueryPriority.INTERACTIVE,
        )
        df = bq.query(sql, job_config=job_cfg, location=LOCATION or None).result().to_dataframe()
        return {(str(r["pk"]), str(r["gcs_url"])) for _, r in df.iterrows()}
    except Exception as e:
        print(f"[WARN] fetch_existing_pairs failed: {e}")
        return set()

def gcs_blob_exists(path: str) -> bool:
    return bucket.blob(path).exists(client=gcs)

def gcs_upload_bytes(path: str, data: bytes, content_type: str | None = None):
    blob = bucket.blob(path)
    blob.upload_from_string(data, content_type=content_type)

async def bq_append_rows(rows: List[Dict[str, Any]]):
    if not rows:
        return
    table = bq_table_full()
    errors = bq.insert_rows_json(
        table,
        [
            {
                "pk": r["pk"],
                "gcs_url": r["gcs_url"],
                "downloaded_at": r["downloaded_at"].isoformat(),
                "batch_date": r["batch_date"].strftime("%Y-%m-%d"),
            }
            for r in rows
        ],
        row_ids=[None]*len(rows)
    )
    if errors:
        print(f"[BQ] partial errors: {errors}")
    else:
        print(f"[BQ] inserted {len(rows)} rows into {table}")

# ---------- Helpers: retry goto + httpx hlavičky/cookies ----------
async def goto_with_retries(page, url: str, attempts: int = 3, wait: str = "domcontentloaded", referer: str | None = None):
    last_err = None
    for i in range(1, attempts+1):
        try:
            await page.goto(url, wait_until=wait, referer=referer, timeout=30000)
            return True
        except PWError as e:
            msg = str(e)
            last_err = e
            # typicky: ERR_ABORTED alebo preruš. navigácia / detach frame
            if "ERR_ABORTED" in msg or "Navigation failed because page was closed" in msg or "frame was detached" in msg:
                await page.wait_for_timeout(1000 * i)
                continue
            await page.wait_for_timeout(1000 * i)
    print(f"[WARN] goto failed for {url}: {last_err}")
    return False

async def build_httpx_headers_and_cookies(ctx, ua: str, referer: str | None) -> tuple[dict, dict]:
    st = await ctx.storage_state()
    cookies = {}
    for c in st.get("cookies", []):
        cookies[c["name"]] = c["value"]
    headers = {
        "User-Agent": ua,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    if referer:
        headers["Referer"] = referer
    # niekedy pomôže:
    headers["Origin"] = "https://www.nehnutelnosti.sk"
    return headers, cookies

# ---------- Zber linkov z výsledkovky ----------
async def collect_detail_links(page, base_url: str, max_pages: int, max_per_page: int) -> List[str]:
    def lid_of(u: str):
        m = DETAIL_ID_RE.search(u or "")
        return m.group(1) if m else None

    all_links, seen_ids = [], set()
    print("[1] Zbieram detail linky po stránkach…")
    for p in range(1, max_pages + 1):
        target = base_url if p == 1 else f"{base_url}{'&' if '?' in base_url else '?'}page={p}"
        print(f"    - otváram {target}")
        ok = await goto_with_retries(page, target, attempts=3, wait="domcontentloaded")
        if not ok:
            break

        # cookies banner
        try:
            btn = page.get_by_role("button", name=re.compile("Súhlasím|Prijať|Accept", re.I))
            if await btn.count() > 0:
                await btn.first.click(timeout=1500)
        except:
            pass

        # lazy scroll
        last_h = -1
        for _ in range(6):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
            await page.wait_for_timeout(600)
            h = await page.evaluate("document.body.scrollHeight")
            if h == last_h: break
            last_h = h

        soup = BeautifulSoup(await page.content(), "html.parser")

        per_page_added = 0
        for a in soup.find_all("a", href=True):
            if per_page_added >= max_per_page:
                break
            href = a["href"]
            if "/detail/" not in href or "/developersky-projekt/" in href:
                continue
            absu = urljoin(page.url, href)
            lid = lid_of(absu)
            if not lid or lid in seen_ids:
                continue
            seen_ids.add(lid)
            all_links.append(absu)
            per_page_added += 1

        print(f"      +{per_page_added} (spolu {len(all_links)})")
        if per_page_added == 0:
            break

    print(f"[1] Nájdených spolu: {len(all_links)}\n")
    return all_links

# ---------- Hlavné scrapovanie ----------
async def scrape_to_gcs_bq():
    ensure_bq_table()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ctx = await browser.new_context(
            locale="sk-SK",
            user_agent=UA,
        )
        # jedna stránka na listovanie
        list_page = await ctx.new_page()

        links = await collect_detail_links(list_page, BASE_URL, MAX_PAGES, MAX_PER_PAGE)
        if not links:
            print("[INFO] Žiadne detail linky.")
            await browser.close()
            return

        # načítaj existujúce páry pre zníženie duplicit
        pks = [listing_id_from_url(u) or u for u in links]
        existing_pairs = await fetch_existing_pairs_for_pks(pks)

        rows_to_insert: List[Dict[str, Any]] = []

        # httpx klient
        base_headers, base_cookies = await build_httpx_headers_and_cookies(ctx, UA, referer=None)
        async with httpx.AsyncClient(follow_redirects=True, timeout=30) as httpx_client:

            for detail_url in links:
                lid = listing_id_from_url(detail_url) or "NA"
                pk = lid
                folder_prefix = f"{GCS_PREFIX}/{lid}"

                # izolovaná stránka pre listing
                page = await ctx.new_page()

                seen_keys: Set[str] = set()
                saved = 0
                idx = 1

                print(f"[go] {detail_url}")

                ok = await goto_with_retries(page, detail_url, attempts=3, wait="domcontentloaded")
                if not ok:
                    await page.close()
                    continue

                # nájdi URL galérie
                sd = BeautifulSoup(await page.content(), "html.parser")
                a = sd.select_one('a[href*="/detail/galeria/foto/"]')
                gallery_url = urljoin(detail_url, a.get("href")) if a else urljoin(detail_url, f"/detail/galeria/foto/{lid}")
                print(f"     -> galéria: {gallery_url}")

                # priprav httpx hlavičky s refererom na detail
                headers, cookies = await build_httpx_headers_and_cookies(ctx, UA, referer=detail_url)

                download_tasks = []

                async def handle_image_response(resp):
                    nonlocal saved, idx, seen_keys, rows_to_insert, existing_pairs
                    try:
                        if resp.request.resource_type != "image":
                            return
                        u = resp.url
                        sp = urlsplit(u)
                        if not any(h in sp.netloc for h in ALLOW_HOSTS):
                            return
                        key = dedup_key(u)
                        if key in seen_keys or saved >= IMAGES_MAX_PER_LISTING:
                            return

                        # 1) PW body()
                        body = b""
                        try:
                            await resp.finished()
                            body = await resp.body()
                        except Exception:
                            body = b""

                        # 2) Fallback: httpx (rovnaké URL, cookies, referer)
                        if (not body) or (len(body) < 2000):
                            try:
                                r = await httpx_client.get(u, headers=headers, cookies=cookies)
                                if r.status_code == 200 and len(r.content) >= 2000:
                                    body = r.content
                                else:
                                    return
                            except Exception:
                                return

                        ext = file_ext_from_url_or_ct(u, resp.headers.get("content-type", ""))
                        gcs_path = f"{folder_prefix}/{idx:03d}{ext}"
                        https_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{gcs_path}"

                        # GCS/BQ dedup
                        if gcs_blob_exists(gcs_path):
                            if (pk, https_url) not in existing_pairs:
                                rows_to_insert.append({
                                    "pk": pk,
                                    "gcs_url": https_url,
                                    "downloaded_at": datetime.now(timezone.utc),
                                    "batch_date": datetime.strptime(BATCH_YYYYMMDD, "%Y%m%d").date(),
                                })
                                existing_pairs.add((pk, https_url))
                            seen_keys.add(key)
                            idx += 1
                            saved += 1
                            return

                        gcs_upload_bytes(gcs_path, body, content_type=resp.headers.get("content-type"))
                        print(f"    [+] gs://{BUCKET_NAME}/{gcs_path}  <- {sp.path.split('/')[-1]}")
                        rows_to_insert.append({
                            "pk": pk,
                            "gcs_url": https_url,
                            "downloaded_at": datetime.now(timezone.utc),
                            "batch_date": datetime.strptime(BATCH_YYYYMMDD, "%Y%m%d").date(),
                        })
                        existing_pairs.add((pk, https_url))
                        seen_keys.add(key)
                        idx += 1
                        saved += 1
                    except Exception as e:
                        print(f"    [!] save-fail: {e}")

                def on_response(resp):
                    download_tasks.append(asyncio.create_task(handle_image_response(resp)))

                page.on("response", on_response)

                try:
                    ok = await goto_with_retries(page, gallery_url, attempts=3, wait="domcontentloaded", referer=detail_url)
                    if not ok:
                        page.remove_listener("response", on_response)
                        await page.close()
                        continue

                    # lazy scroll
                    for _ in range(10):
                        try:
                            await page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
                        except:
                            pass
                        await page.wait_for_timeout(600)

                    # thumbnaily
                    thumbs = page.locator('img[alt*="foto_"]')
                    count = await thumbs.count()
                    if count == 0:
                        thumbs = page.locator('.MuiGrid2-container img[alt*="foto_"]')
                        count = await thumbs.count()
                    print(f"    [thumbnails] count={count}")

                    for i in range(count):
                        if saved >= IMAGES_MAX_PER_LISTING:
                            break
                        try:
                            await thumbs.nth(i).scroll_into_view_if_needed()
                            await thumbs.nth(i).click(timeout=2000)
                        except:
                            try:
                                await thumbs.nth(i).hover(timeout=1200)
                            except:
                                pass
                        await page.wait_for_timeout(450)

                    if download_tasks:
                        await asyncio.gather(*download_tasks)
                finally:
                    page.remove_listener("response", on_response)
                    await page.close()

                print(f"[OK] {pk} saved:{saved}\n")
                if rows_to_insert:
                    await bq_append_rows(rows_to_insert)
                    rows_to_insert.clear()

                time.sleep(random.uniform(0.2, 0.5))

        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_to_gcs_bq())
