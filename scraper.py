# -*- coding: utf-8 -*-
"""
Real-estate scraper -> BigQuery (+ voliteľne Excel)
---------------------------------------------------
• Bezpečné retry/backoff, NaN/Inf-safe história cien, detekcia typu nehnuteľnosti.
• Denný "snapshot" sa appenduje do BQ, "master" sa vždy nahrádza (replace).
• Konfigurácia cez ENV premenné:

  SAVE_EXCEL   = "true" | "false"   (default true)
  BQ_ENABLE    = "true" | "false"   (default true)
  GCP_PROJECT_ID = <tvoj_gcp_project>  (povinné, ak BQ_ENABLE=true)
  BQ_DATASET     = "realestate"     (default)
  BQ_LOCATION    = "EU"             (default; maj rovnaké ako dataset)
  WORKSPACE_HOME = cesta pre xlsx   (default ".")

V GitHub Actions sa kľúč service accountu dáva do súboru a ukáže sa cez
GOOGLE_APPLICATION_CREDENTIALS=<cesta_k_súboru>.
"""

import os, re, time, json, random, math
from urllib.parse import urljoin
from datetime import datetime, date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

# ----------------- ENV prepínače (pre BQ/Excel) -----------------
SAVE_EXCEL = os.getenv("SAVE_EXCEL", "true").lower() == "true"
BQ_ENABLE  = os.getenv("BQ_ENABLE",  "true").lower() == "true"
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET     = os.getenv("BQ_DATASET", "realestate")
BQ_LOCATION    = os.getenv("BQ_LOCATION", "EU")

# ----------------- Nastavenia -----------------
# TIP: ak chceš len Liptovský Mikuláš (okres), použi:
# BASE_URL = "https://www.nehnutelnosti.sk/vysledky/liptovsky-mikulas/predaj"
BASE_URL = "https://www.nehnutelnosti.sk/vysledky/zilinsky-kraj/predaj"
MAX_PAGES = 2
REQ_TIMEOUT = 25
PAUSE = (1.5, 4.0)
VERBOSE = True
PROGRESS_EVERY = 1

TS = datetime.now().strftime("%Y-%m-%d_%H%M%S")
TODAY = date.today()

# Pre Databricks sa používa Workspace cesta; inde zober aktuálny priečinok
WORKSPACE_HOME = os.getenv("WORKSPACE_HOME", ".").rstrip("/")

# Pevné názvy/umiestnenia súborov (mimo pôvodného nehnutelnosti_output/)
BASE_NAME     = "nehnutelnosti_zilinsky_kraj"
MASTER_XLSX   = os.path.join(WORKSPACE_HOME, f"{BASE_NAME}_master.xlsx")
SNAPSHOT_DIR  = os.path.join(WORKSPACE_HOME, f"{BASE_NAME}_snapshots")
DEBUG_DIR     = os.path.join(WORKSPACE_HOME, f"{BASE_NAME}_debug")
os.makedirs(SNAPSHOT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
SNAPSHOT_XLSX = os.path.join(SNAPSHOT_DIR, f"{BASE_NAME}_{TS}.xlsx")

# ----------------- Regexy a slovníky -----------------
PRICE_RE = re.compile(r"(?<!\d)(\d{1,3}(?:[ \u00A0\u202F.]?\d{3})+|\d+)\s*(?:€|EUR)(?!\S)", re.I)
PER_SQM_HINT = re.compile(r"(?:€|EUR)\s*/\s*m|za\s*m2|m²", re.I)
AD_PRICE_SANITIZE_RE = re.compile(r"\s+")
DETAIL_RE = re.compile(r'href="(/detail/[^"]+)"', re.I)
DETAIL_ABS_RE = re.compile(r"https?://www\.nehnutelnosti\.sk/detail/[^\s\"<>]+", re.I)
ID_FROM_URL_RE = re.compile(r"/detail/([^/]+)/")
ROOMS_RE = re.compile(r"(\d+)\s*[-\s]*izbov", re.I)
AREA_RE  = re.compile(r"(\d{1,3}(?:[ \u00A0\u202F.]?\d{3})*(?:[.,]\d+)?|\d+(?:[.,]\d+)?)\s*m(?:2|²)\b", re.I)
CONDITION_KEYWORDS = [
    "Novostavba","Kompletná rekonštrukcia","Čiastočná rekonštrukcia",
    "Pôvodný stav","Vo výstavbe","Rekonštrukcia","Developerský projekt",
]
KNOWN_TYPES = [
    "Pozemok pre rodinné domy","Stavebný pozemok","Ostatné pozemky","Orná pôda",
    "Garsónka","1-izbový byt","2-izbový byt","3-izbový byt","4-izbový byt","5 a viac izbový byt","Byt","Apartmán","Mezonet","Loft",
    "Rodinný dom","Dvojdom","Chalupa, rekreačný domček","Chalupa","Chata","Rekreačný domček",
    "Polyfunkčný objekt","Kancelárie","Obchodné priestory","Sklady a haly","Prevádzkové priestory","Hotel","Penzión","Administratívna budova",
    "Garáž","Parkovacie miesto",
]

# ----------------- Utility -----------------
UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
ACCEPT_LANGS = [
    "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
    "cs-CZ,sk;q=0.9,en-US;q=0.7,en;q=0.6",
    "en-US,en;q=0.9,sk;q=0.8,cs;q=0.7",
]

def rand_headers():
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Cache-Control": "no-cache", "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
    }
    if random.random() < 0.3:
        h["Connection"] = "close"
    return h

def new_session():
    s = requests.Session()
    retry = Retry(total=4, connect=3, read=3, backoff_factor=1.2,
                  status_forcelist=(429,500,502,503,504),
                  allowed_methods=frozenset(["GET","HEAD"]))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def sleep_a_bit(): time.sleep(random.uniform(*PAUSE))

def get(session: requests.Session, url: str) -> requests.Response:
    for attempt in range(5):
        r = session.get(url, headers=rand_headers(), timeout=REQ_TIMEOUT)
        if r.status_code in (429, 503) and attempt < 4:
            wait = (2 ** attempt) + random.random()
            if VERBOSE: print(f"[BACKOFF] {r.status_code} -> spím {wait:.1f}s", flush=True)
            time.sleep(wait); continue
        return r
    return r

def save_debug(html: str, name: str):
    path = os.path.join(DEBUG_DIR, name)
    with open(path, "w", encoding="utf-8") as f: f.write(html)
    if VERBOSE: print(f"[DEBUG] uložené HTML: {path}", flush=True)
    return path

def extract_detail_links(html: str, page_url: str) -> list:
    links = set(DETAIL_RE.findall(html)) | set(DETAIL_ABS_RE.findall(html))
    links = {urljoin(page_url, l) for l in links}
    if not links:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if "/detail/" in a["href"]:
                links.add(urljoin(page_url, a["href"]))

    # niektoré duplicity majú # alebo parametre – normalizuj
    links = {l.split("#")[0] for l in links}
    return sorted(links)

def clean_spaces(s: str) -> str:
    return AD_PRICE_SANITIZE_RE.sub(" ", s or "").strip()

def parse_int_price(s: str):
    if not s: return None
    digits = re.sub(r"[^0-9]", "", str(s))
    return int(digits) if digits else None

def _to_float_area(num_raw: str):
    if not num_raw: return None
    s = str(num_raw)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    return float(s.replace("\u00A0","").replace("\u202F","").replace(" ",""))

def extract_from_address(addr: str) -> dict:
    out = {"street_or_locality":None,"city":None,"district":None,"rooms":None,"area_m2":None,"condition":None}
    if not addr: return out
    txt = " ".join(addr.split())
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if parts: out["street_or_locality"] = parts[0]
    for p in parts[1:]:
        if out["city"] is None and "okres" not in p.lower(): out["city"] = p
        if out["district"] is None and "okres" in p.lower(): out["district"] = p
    m = ROOMS_RE.search(txt)
    if m:
        try: out["rooms"] = int(m.group(1))
        except: pass
    m = AREA_RE.search(txt)
    if m:
        try: out["area_m2"] = _to_float_area(m.group(1))
        except: pass
    low = txt.lower()
    for ck in CONDITION_KEYWORDS:
        if ck.lower() in low: out["condition"] = ck; break
    return out

def price_from_jsonld(soup: BeautifulSoup):
    for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        payload = sc.string or sc.get_text() or ""
        if not payload.strip(): continue
        try: data = json.loads(payload)
        except Exception: continue
        stack = [data]
        while stack:
            obj = stack.pop()
            if isinstance(obj, dict):
                typ = (obj.get("@type") or obj.get("type") or "").lower()
                if typ in ("offer","aggregateoffer"):
                    for key in ("price","highPrice","lowPrice"):
                        p = parse_int_price(obj.get(key))
                        if p: return p, "jsonld"
                for v in obj.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(obj, list): stack.extend(obj)
    return None

def price_from_meta(soup: BeautifulSoup):
    for sel in ["meta[itemprop='price']","meta[property='product:price:amount']","meta[property='og:price:amount']"]:
        for m in soup.select(sel):
            p = parse_int_price(m.get("content"))
            if p: return p, "meta"
    return None

def is_per_sqm(text: str) -> bool: return bool(PER_SQM_HINT.search(text))

def price_from_text(soup: BeautifulSoup, h1_node=None):
    candidates = list(soup.select("[class*='price'], [class*='cena'], [data-testid*='price']"))
    if h1_node:
        near = [h1_node, h1_node.parent]; near.extend(h1_node.find_all_next(limit=50))
        for n in near:
            if n and n not in candidates: candidates.append(n)
    best = None
    for node in filter(None, candidates):
        txt = " ".join(node.stripped_strings)
        if not txt or is_per_sqm(txt): continue
        m = PRICE_RE.search(txt)
        if not m: continue
        p = parse_int_price(m.group(1))
        if p and (best is None or p > best): best = p
    if best is not None: return best, "text"
    text_all = " ".join(soup.stripped_strings)
    for m in PRICE_RE.finditer(text_all):
        start = m.start(); near = text_all[start:start+30]
        if is_per_sqm(near): continue
        p = parse_int_price(m.group(1))
        if p: return p, "text_all"
    return None

def id_from_detail_url(u: str):
    m = ID_FROM_URL_RE.search(u)
    return m.group(1) if m else None

# --------- Normalizácia / kanonizácia a NaN/Inf-safe ---------
def normalize_note(note: str):
    if not note: return None
    n = str(note).strip().lower()
    repl = {"cena v rk":"v rk","v realitnej kancelarii":"v rk","v realitnej kancelárii":"v rk","na vyziadanie":"na vyžiadanie"}
    n = repl.get(n, n)
    if "dohodou" in n: return "cena dohodou"
    if "rk" in n: return "v rk"
    if "vyžiadanie" in n or "vyziadanie" in n: return "na vyžiadanie"
    return n or None

def _nan_to_none(x):
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if not math.isfinite(float(x)):
                return None
        if pd.isna(x):
            return None
    except Exception:
        pass
    return x

def _canon_price(p):
    p = _nan_to_none(p)
    if p is None: return None
    try:
        return int(round(float(p)))
    except Exception:
        return None

def current_price_status(price_eur, price_note):
    price_eur = _nan_to_none(price_eur)
    note = normalize_note(price_note)
    if price_eur is not None: return "number"
    if note: return "note"
    return "unknown"

def sanitize_event(ev: dict) -> dict:
    if not isinstance(ev, dict):
        ev = {}
    price = _canon_price(ev.get("price_eur"))
    note  = normalize_note(ev.get("price_note"))
    status = ev.get("status")
    if status not in ("number", "note", "unknown"):
        status = current_price_status(price, note)
    date_str = ev.get("date") or TODAY.isoformat()
    src = ev.get("price_source")
    return {
        "date": str(date_str),
        "status": status,
        "price_eur": price,
        "price_note": note,
        "price_source": src if isinstance(src, str) and src else None,
    }

def sanitize_history(hist) -> list:
    if not isinstance(hist, list):
        return []
    out = []
    for ev in hist:
        try:
            out.append(sanitize_event(dict(ev)))
        except Exception:
            continue
    # zreťaz duplicitných stavov
    dedup = []
    for ev in out:
        key = (ev.get("status"), ev.get("price_eur"), ev.get("price_note"))
        if not dedup or key != (dedup[-1].get("status"), dedup[-1].get("price_eur"), dedup[-1].get("price_note")):
            dedup.append(ev)
    return dedup

# --------- Typ nehnuteľnosti z listu/detailu ---------
def best_type_match(texts):
    hay = " | ".join([t for t in texts if t])[:20000].lower()
    found = None
    for t in sorted(KNOWN_TYPES, key=len, reverse=True):
        if t.lower() in hay:
            found = t; break
    return found

def extract_types_from_list(html: str, page_url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    mapping = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/detail/" not in href:
            continue
        abs_url = urljoin(page_url, href)
        # nájdi rodičovský "card" element
        card = a
        for _ in range(6):
            if card is None:
                break
            if getattr(card, "name", None) in ("article", "li", "div"):
                break
            card = card.parent
        if card is None:
            card = a.parent
        texts = []
        for node in card.find_all(["a","span","div","p","h2","h3"], recursive=True):
            txt = " ".join((node.get_text(" ") or "").split())
            if not txt:
                continue
            if PRICE_RE.search(txt) or is_per_sqm(txt):
                continue
            if 2 <= len(txt) <= 60:
                texts.append(txt)
        t = best_type_match(texts)
        if t:
            mapping[abs_url] = t
    return mapping

def property_type_from_detail(soup: BeautifulSoup):
    # 1) structured data
    for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        payload = sc.string or sc.get_text() or ""
        if not payload.strip(): continue
        try: data = json.loads(payload)
        except Exception: continue
        stack = [data]
        while stack:
            obj = stack.pop()
            if isinstance(obj, dict):
                for k in ("category","propertyType","realEstateType","@type","type"):
                    v = obj.get(k)
                    if isinstance(v, str):
                        cand = best_type_match([v])
                        if cand: return cand
                for v in obj.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(obj, list): stack.extend(obj)
    # 2) breadcrumbs / okolie H1
    texts = []
    h1 = soup.find("h1")
    if h1:
        near = [h1, h1.parent]; near.extend(h1.find_all_previous(limit=20)); near.extend(h1.find_all_next(limit=40))
        for n in near:
            if not hasattr(n,"get_text"): continue
            txt = " ".join((n.get_text(" ") or "").split())
            if txt and len(txt) <= 80: texts.append(txt)
    if not texts: texts = [" ".join(soup.stripped_strings)[:8000]]
    return best_type_match(texts)

# ----------------- Perzistencia -----------------
def load_master_xlsx(path: str) -> pd.DataFrame:
    cols = [
        "pk","listing_id","url","title","address","price_eur","price_source","price_note",
        "property_type",
        "street_or_locality","city","district","rooms","area_m2","condition",
        "first_seen","last_seen","active","days_listed","scraped_at",
        "price_first_eur","price_first_note","price_first_date",
        "price_last_change_date","price_changes_count","price_status","price_history",
        "inactive_since"
    ]
    if os.path.exists(path):
        df = pd.read_excel(path, engine="openpyxl")
        for c in cols:
            if c not in df.columns:
                df[c] = pd.Series(dtype="object")
        for c in ("first_seen","last_seen","price_first_date","price_last_change_date","inactive_since"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
        if "price_changes_count" in df.columns:
            df["price_changes_count"] = pd.to_numeric(df["price_changes_count"], errors="coerce").fillna(0).astype(int)
        # sane historické price_history
        if "price_history" in df.columns:
            fixed = []
            for _, r in df.iterrows():
                hist = load_history_from_cell(r.get("price_history"))
                fixed.append(json.dumps(sanitize_history(hist), ensure_ascii=False, allow_nan=False))
            df["price_history"] = fixed
        return df[cols]
    return pd.DataFrame(columns=cols)

def make_pk(row: dict) -> str:
    return row.get("listing_id") or row.get("url")

def finalize_days(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    df["first_seen"] = pd.to_datetime(df["first_seen"]).dt.date
    df["last_seen"]  = pd.to_datetime(df["last_seen"]).dt.date
    df["days_listed"] = (pd.to_datetime(df["last_seen"]) - pd.to_datetime(df["first_seen"])).dt.days + 1
    df.loc[df["days_listed"] < 1, "days_listed"] = 1
    return df

def save_excel(df: pd.DataFrame, path: str):
    try: df.to_excel(path, index=False, engine="openpyxl")
    except Exception as e:
        print(f"[WARN] Excel zápis zlyhal ({e}). Skús `%pip install openpyxl` a spustiť znova.", flush=True)
        raise

# ----------------- História cien (NaN-safe, kanonizácia) -----------------
def load_history_from_cell(cell) -> list:
    if cell is None: return []
    try:
        if pd.isna(cell): return []
    except Exception:
        pass
    if isinstance(cell, (list, dict)): return cell
    if isinstance(cell, str):
        s = cell.strip()
        if not s or s.lower() in ("nan","none","null"): return []
        try:
            return json.loads(s)
        except Exception:
            return []
    try:
        s = str(cell).strip()
        if not s or s.lower() in ("nan","none","null"): return []
        return json.loads(s)
    except Exception:
        return []

def _state_key(d):
    return (
        d.get("status"),
        _canon_price(d.get("price_eur")),
        normalize_note(d.get("price_note"))
    )

def append_history_if_changed(row_dict, today_state: dict):
    # načítaj a vyčisti históriu
    hist_raw = load_history_from_cell(row_dict.get("price_history"))
    hist = sanitize_history(hist_raw)

    # vyčisti dnešný stav
    today_state = sanitize_event(today_state)

    changed = False
    if hist:
        last = hist[-1]
        if not (last.get("date") == TODAY.isoformat() and _state_key(last) == _state_key(today_state)):
            if _state_key(last) != _state_key(today_state):
                hist.append(today_state); changed = True
    else:
        hist.append(today_state); changed = True

    if not row_dict.get("price_first_date"):
        if today_state["status"] == "number":
            row_dict["price_first_eur"] = today_state["price_eur"]; row_dict["price_first_note"] = None
        elif today_state["status"] == "note":
            row_dict["price_first_eur"] = None; row_dict["price_first_note"] = today_state["price_note"]
        else:
            row_dict["price_first_eur"] = None; row_dict["price_first_note"] = None
        row_dict["price_first_date"] = TODAY

    if changed:
        row_dict["price_last_change_date"] = TODAY

    row_dict["price_status"] = today_state["status"]
    row_dict["price_changes_count"] = max(0, len(hist) - 1)
    # dôležité: ukladáme už sanitizovaný list (bez NaN/Inf)
    row_dict["price_history"] = json.dumps(hist, ensure_ascii=False, allow_nan=False)
    return row_dict

# ----------------- Detail parser -----------------
def parse_detail(session: requests.Session, url: str, type_hint: str = None) -> dict:
    r = get(session, url); r.raise_for_status()
    html = r.text; soup = BeautifulSoup(html, "html.parser")

    listing_id = None
    m = re.search(r"Číslo\s+inzerátu\s*:\s*([A-Za-z0-9_-]+)", html)
    if m: listing_id = m.group(1)
    else:
        m2 = ID_FROM_URL_RE.search(url)
        if m2: listing_id = m2.group(1)

    title = None; h1 = soup.find("h1")
    if h1: title = clean_spaces(h1.get_text(" "))

    # adresa
    address = None
    if h1:
        nxt = h1.find_next()
        for _ in range(8):
            if not nxt: break
            txt = clean_spaces(nxt.get_text(" "))
            if ("," in txt or "okres" in txt.lower()) and 5 < len(txt) < 200:
                address = txt; break
            nxt = nxt.find_next()
    if not address:
        all_txt = soup.get_text(" ", strip=True)
        for chunk in re.split(r"\s{2,}|[\r\n]+", all_txt):
            if ("okres" in chunk.lower() or "liptovský mikuláš" in chunk.lower()) and len(chunk) < 220:
                address = clean_spaces(chunk); break

    # cena
    price_eur = None; price_source = None
    got = price_from_jsonld(soup) or price_from_meta(soup)
    if got: price_eur, price_source = got
    else:
        got = price_from_text(soup, h1)
        if got: price_eur, price_source = got

    price_note = None
    if price_eur is None:
        ta = (soup.get_text(" ", strip=True) or "").lower()
        for note in ("cena dohodou","cena v rk","na vyžiadanie","v rk"):
            if note in ta: price_note = note; break

    if price_eur is None and not price_note:
        dbg = os.path.join(DEBUG_DIR, f"detail_no_price_{listing_id or 'NA'}_{datetime.now().strftime('%H%M%S')}.html")
        with open(dbg, "w", encoding="utf-8") as f: f.write(html)
        if VERBOSE: print(f"[DEBUG] cena nezistená -> {dbg}", flush=True)

    addr_info = extract_from_address(address)
    prop_type = type_hint or property_type_from_detail(soup)

    return {
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "listing_id": listing_id,
        "url": url,
        "title": title,
        "address": address,
        "price_eur": _nan_to_none(price_eur),
        "price_source": price_source,
        "price_note": normalize_note(price_note),
        "property_type": prop_type,
        **addr_info,
    }

# ----------------- BigQuery upload -----------------
def upload_to_bigquery(df_master: pd.DataFrame, df_today: pd.DataFrame):
    """
    MASTER -> replace (WRITE_TRUNCATE)
    DAILY  -> append (s pridaným stĺpcom batch_ts)
    """
    if not BQ_ENABLE:
        return
    if not GCP_PROJECT_ID:
        raise RuntimeError("Missing GCP_PROJECT_ID env var (required for BigQuery upload)")

    import pandas_gbq

    df_daily = df_today.copy()
    if not df_daily.empty:
        df_daily["batch_ts"] = pd.Timestamp.utcnow()

    # replace master (schéma/partition ostáva)
    if not df_master.empty:
        pandas_gbq.to_gbq(
            df_master,
            f"{BQ_DATASET}.listings_master",
            project_id=GCP_PROJECT_ID,
            if_exists="replace",
            location=BQ_LOCATION
        )

    # append daily
    if not df_daily.empty:
        pandas_gbq.to_gbq(
            df_daily,
            f"{BQ_DATASET}.listings_daily",
            project_id=GCP_PROJECT_ID,
            if_exists="append",
            location=BQ_LOCATION
        )

# ----------------- Hlavný crawler -----------------
def crawl_to_excel(base_url: str = BASE_URL, max_pages: int = MAX_PAGES):
    s = new_session()
    all_detail_urls = []
    list_type_hints = {}  # url -> property_type hint
    print(f"➡️  CRAWL: {base_url}", flush=True)

    for page in range(1, max_pages + 1):
        page_url = base_url if page == 1 else f"{base_url}{'&' if '?' in base_url else '?'}page={page}"
        r = get(s, page_url); status = r.status_code; html = r.text
        dbg_path = save_debug(html, f"results_page{page}_{TS}.html")
        print(f"{dbg_path}", flush=True)
        print(f"[INFO] page {page} {page_url} -> status {status}", flush=True)

        if status == 404:
            print(f"[INFO] Strana {page} neexistuje (404).", flush=True); break
        if "enable JavaScript" in html or "Access denied" in html or status >= 400:
            print("[WARN] Možná anti-bot ochrana / chybová odpoveď.", flush=True)

        # typy + odkazy
        try:
            list_type_hints.update(extract_types_from_list(html, page_url))
        except Exception as e:
            if VERBOSE:
                print(f"[WARN] extract_types_from_list zlyhalo: {e}", flush=True)

        links = extract_detail_links(html, page_url)
        if VERBOSE: print(f"[DEBUG] found detail links on page {page}: {len(links)}", flush=True)
        if not links:
            if page == 1: print("[DEBUG] 0 linkov na prvej stránke – skontroluj DEBUG HTML.", flush=True)
            break

        all_detail_urls.extend(links)
        if len(links) < 10 and page > 1: break
        sleep_a_bit()

    all_detail_urls = sorted(set(all_detail_urls)); random.shuffle(all_detail_urls)

    rows = []; total = len(all_detail_urls)
    print(f"🔗 Detailov na spracovanie: {total}", flush=True)
    for i, u in enumerate(all_detail_urls, 1):
        lid_hint = id_from_detail_url(u)
        try:
            row = parse_detail(s, u, type_hint=list_type_hints.get(u))
            rows.append(row)
            lid = row.get("listing_id") or lid_hint or "?"
            if (i % PROGRESS_EVERY == 0) or VERBOSE: print(f"  · {i}/{total} OK: {lid}", flush=True)
        except Exception as e:
            print(f"  · {i}/{total} FAIL: {lid_hint or '?'} -> {e}", flush=True)
        sleep_a_bit()

    # dnešný dataframe
    df_today = pd.DataFrame(rows, columns=[
        "scraped_at","listing_id","url","title","address",
        "price_eur","price_source","price_note","property_type",
        "street_or_locality","city","district","rooms","area_m2","condition"
    ])
    if not df_today.empty:
        df_today["pk"] = df_today.apply(lambda r: make_pk(r), axis=1)
        df_today = df_today.dropna(subset=["pk"]).drop_duplicates(subset=["pk"], keep="first")

    # načítaj a priprav master
    master = load_master_xlsx(MASTER_XLSX)
    if master.empty:
        master["seen_today"] = pd.Series(dtype=bool)
    else:
        master["seen_today"] = False

    # merge/update do mastera
    if not df_today.empty:
        today_map = df_today.set_index("pk").to_dict(orient="index")

        # update existujúcich
        if not master.empty:
            for idx, row in master.iterrows():
                pk = row["pk"]
                if pk in today_map:
                    t = today_map[pk]
                    for col in ["listing_id","url","title","address","price_eur","price_source","price_note","property_type",
                                "street_or_locality","city","district","rooms","area_m2","condition","scraped_at"]:
                        master.at[idx, col] = t.get(col, row.get(col))
                    if pd.isna(row.get("first_seen")):
                        master.at[idx, "first_seen"] = TODAY
                    master.at[idx, "last_seen"] = TODAY
                    master.at[idx, "active"] = True
                    master.at[idx, "seen_today"] = True
                    master.at[idx, "inactive_since"] = None

                    # história cien
                    price_val = _canon_price(t.get("price_eur"))
                    note_val  = normalize_note(t.get("price_note"))
                    today_state = {
                        "date": TODAY.isoformat(),
                        "status": current_price_status(price_val, note_val),
                        "price_eur": price_val,
                        "price_note": note_val,
                        "price_source": t.get("price_source"),
                    }
                    updated = append_history_if_changed(master.loc[idx].to_dict(), today_state)
                    for k, v in updated.items(): master.at[idx, k] = v

        # pridaj nové
        existing_pks = set(master["pk"]) if not master.empty else set()
        new_rows = []
        for pk, t in today_map.items():
            if pk not in existing_pks:
                price_val = _canon_price(t.get("price_eur"))
                note_val  = normalize_note(t.get("price_note"))
                today_state = {
                    "date": TODAY.isoformat(),
                    "status": current_price_status(price_val, note_val),
                    "price_eur": price_val,
                    "price_note": note_val,
                    "price_source": t.get("price_source"),
                }
                first_eur  = price_val if today_state["status"] == "number" else None
                first_note = note_val  if today_state["status"] == "note"   else None
                new_rows.append({
                    "pk": pk, **t,
                    "first_seen": TODAY, "last_seen": TODAY,
                    "active": True, "seen_today": True,
                    "days_listed": 1,
                    "price_first_eur": first_eur,
                    "price_first_note": first_note,
                    "price_first_date": TODAY,
                    "price_last_change_date": TODAY,
                    "price_changes_count": 0,
                    "price_status": today_state["status"],
                    "price_history": json.dumps([sanitize_event(today_state)], ensure_ascii=False, allow_nan=False),
                    "inactive_since": None,
                })
        if new_rows:
            master = pd.concat([master, pd.DataFrame(new_rows)], ignore_index=True)

    # dnes nevidené -> inactive
    if not master.empty:
        mask_not_seen = master["seen_today"] == False
        was_active = master["active"] == True
        to_inactivate = mask_not_seen & was_active
        master.loc[to_inactivate, "active"] = False
        master.loc[to_inactivate & master["inactive_since"].isna(), "inactive_since"] = TODAY
        master.drop(columns=["seen_today"], inplace=True)

    # dopočítaj dni
    master = finalize_days(master)

    # Excel (voliteľné)
    if SAVE_EXCEL:
        if not master.empty:
            save_excel(master, MASTER_XLSX)
        save_excel(df_today, SNAPSHOT_XLSX)
        print(f"✅ Master XLSX:   {MASTER_XLSX}", flush=True)
        print(f"📝 Snapshot XLSX: {SNAPSHOT_XLSX}", flush=True)

    # BigQuery upload
    upload_to_bigquery(master, df_today)

    print(f"📊 Aktívnych dnes: {int(master['active'].sum()) if not master.empty else 0} / spolu: {len(master)}", flush=True)

# Spusti
if __name__ == "__main__":
    crawl_to_excel()
