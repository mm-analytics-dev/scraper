# -*- coding: utf-8 -*-
"""
Real-estate scraper -> Excel (master + denné snapshoty) + popis + vlastnosti

ZMENY:
- Robustnejší HTTP retry/backoff pre detaily.
- Fallback na Playwright pri neúspechu requests (500/429/403/časovač).
- Garantovaná unikátnosť podľa 'pk' + drop_duplicates pred exportom.
- BQ upload ako predtým (master REPLACE, daily APPEND).
"""

import os, re, time, json, random, math, unicodedata
from urllib.parse import urljoin
from datetime import datetime, date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

# ----------------- ENV / v2 BQ prepínače -----------------

SAVE_EXCEL      = os.getenv("SAVE_EXCEL", "false").lower() == "true"
BQ_ENABLE       = os.getenv("BQ_ENABLE", "true").lower() == "true"
GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET      = os.getenv("BQ_DATASET", "realestate_v2")
BQ_LOCATION     = os.getenv("BQ_LOCATION", "EU")

# Playwright fallback (ak je dostupný v prostredí)
USE_PLAYWRIGHT_FALLBACK = os.getenv("USE_PLAYWRIGHT_FALLBACK", "true").lower() == "true"

# ----------------- Nastavenia -----------------

BASE_URL = "https://www.nehnutelnosti.sk/vysledky/okres-liptovsky-mikulas/predaj"

MAX_PAGES = 1                 # koľko strán výsledkov prejsť
MAX_LINKS_PER_PAGE = 2        # maximum unikátnych inzerátov z 1 stránky
MAX_LISTINGS_TOTAL = None     # celkový strop (None = bez limitu)

REQ_TIMEOUT = 25
PAUSE = (1.0, 2.2)            # jemnejšie pauzy (menšie bursty)
VERBOSE = True
PROGRESS_EVERY = 1

TS = datetime.now().strftime("%Y-%m-%d_%H%M%S")
TODAY = date.today()

WORKSPACE_HOME = os.getenv("WORKSPACE_HOME", "/content")
BASE_NAME     = "nehnutelnosti_liptovsky_mikulas_okres"
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
DETAIL_ABS_RE = re.compile(r"https?://www\.nehnutelnosti\.sk/detail/[^\s\"<>]+", re.I)
ID_FROM_URL_RE = re.compile(r"/detail/([^/]+)/")
ROOMS_RE = re.compile(r"(\d+)\s*[-\s]*izbov", re.I)
AREA_RE  = re.compile(r"(\d{1,3}(?:[ \u00A0\u202F.]?\d{3})*(?:[.,]\d+)?|\d+(?:[.,]\d+)?)\s*m(?:2|²)\b", re.I)

CONDITION_KEYWORDS = [
    "Novostavba","Kompletná rekonštrukcia","Čiastočná rekonštrukcia",
    "Pôvodný stav","Vo výstavbe","Rekonštrukcia","Developerský projekt",
]

KNOWN_TYPES = [
    "Pozemok pre rodinné domy","Stavebný pozemok","Ostatné pozemky","Orná pôda","Záhrada",
    "Garsónka","1-izbový byt","2-izbový byt","3-izbový byt","4-izbový byt","5 a viac izbový byt","Byt","Apartmán","Mezonet","Loft",
    "Rodinný dom","Dvojdom","Chalupa, rekreačný domček","Chalupa","Chata","Rekreačný domček",
    "Polyfunkčný objekt","Kancelárie","Obchodné priestory","Sklady a haly","Prevádzkové priestory","Hotel","Penzión","Administratívna budova",
    "Garáž","Parkovacie miesto",
]
KEY_POZEMOK = ("pozemok","pozemky","stavebný pozemok","stavebny pozemok","orná pôda","orna poda","záhrada","zahrada")
KEY_OBJEKT  = ("objekt","polyfunk", "administratív", "administrativ", "komerčný objekt","komercny objekt")

# ----------------- Obce LM -----------------

LM_OBCE = [
    "Liptovský Mikuláš","Liptovský Hrádok","Beňadiková","Bobrovček","Bobrovec","Bobrovník","Bukovina","Demänovská Dolina","Dúbrava","Galovany","Gôtovany","Huty","Hybe","Ižipovce","Jakubovany","Jalovec","Jamník","Konská","Kráľova Lehota","Kvačany","Lazisko",
    "Liptovská Anna","Liptovská Kokava","Liptovská Porúbka","Liptovská Sielnica","Liptovské Beharovce","Liptovské Kľačany","Liptovské Matiašovce",
    "Liptovský Ján","Liptovský Ondrej","Liptovský Peter","Liptovský Trnovec","Ľubeľa","Malatíny","Malé Borové","Malužiná","Nižná Boca",
    "Partizánska Ľupča","Pavčina Lehota","Pavlova Ves","Podtureň","Pribylina","Prosiek","Smrečany","Svätý Kríž","Trstené","Uhorská Ves",
    "Vavrišovo","Važec","Veľké Borové","Veterná Poruba","Vlachy","Východná","Vyšná Boca","Závažná Poruba","Žiar"
]
LM_OBCE_NORM = { unicodedata.normalize("NFKD", o).encode("ascii","ignore").decode().lower(): o for o in LM_OBCE }

def strip_diacritics(s: str) -> str:
    if s is None: return None
    s = unicodedata.normalize("NFKD", str(s))
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_txt(s: str) -> str:
    if s is None: return ""
    s = strip_diacritics(s).lower()
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def most_specific_obec(*texts) -> str | None:
    hay = norm_txt(" | ".join([t for t in texts if t]))
    found = None; maxlen = -1
    for key, proper in LM_OBCE_NORM.items():
        if key in hay and len(proper) > maxlen:
            found = proper; maxlen = len(proper)
    return found

# ----------------- HTTP -----------------

UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
]
ACCEPT_LANGS = [
    "sk-SK,sk;q=0.9,cs-CZ;q=0.8,en-US;q=0.7,en;q=0.6",
    "cs-CZ,sk;q=0.9,en-US;q=0.7,en;q=0.6",
    "en-US,en;q=0.9,sk;q=0.8,cs;q=0.7",
]

def rand_headers(extra=None):
    h = {
        "User-Agent": random.choice(UAS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": random.choice(ACCEPT_LANGS),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.nehnutelnosti.sk/",
        "Upgrade-Insecure-Requests": "1",
        "Accept-Encoding": "gzip, deflate, br",
    }
    if extra: h.update(extra)
    if random.random() < 0.3:
        h["Connection"] = "close"
    return h

def new_session():
    s = requests.Session()
    retry = Retry(
        total=6, connect=3, read=3, backoff_factor=1.6,
        status_forcelist=(429,500,502,503,504),
        allowed_methods=frozenset(["GET","HEAD"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter); s.mount("http://", adapter)
    return s

def sleep_a_bit(): time.sleep(random.uniform(*PAUSE))

def get(session: requests.Session, url: str, extra_headers=None) -> requests.Response:
    # vlastný loop s exponential backoff (nad rámec urllib3 Retry)
    delay = 0.7
    for attempt in range(1, 6+1):
        try:
            r = session.get(url, headers=rand_headers(extra_headers), timeout=REQ_TIMEOUT, allow_redirects=True)
        except Exception:
            if attempt >= 6: raise
            time.sleep(delay + random.random()*0.4); delay *= 1.5
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            if attempt >= 6: return r
            time.sleep(delay + random.random()*0.4); delay *= 1.5
            continue
        return r
    return r

def save_debug(html: str, name: str):
    path = os.path.join(DEBUG_DIR, name)
    with open(path, "w", encoding="utf-8") as f: f.write(html)
    if VERBOSE: print(f"[DEBUG] uložené HTML: {path}", flush=True)
    return path

# ----------------- Playwright fallback -----------------

_pw = {"ready": False, "playwright": None, "browser": None, "context": None, "page": None}

def _pw_start():
    if _pw["ready"]: return
    from playwright.sync_api import sync_playwright
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
    ctx = browser.new_context(locale="sk-SK",
                              user_agent=random.choice(UAS))
    page = ctx.new_page()
    _pw.update({"ready": True, "playwright": pw, "browser": browser, "context": ctx, "page": page})

def _pw_stop():
    if not _pw["ready"]: return
    try:
        _pw["context"].close()
        _pw["browser"].close()
        _pw["playwright"].stop()
    finally:
        _pw.update({"ready": False, "playwright": None, "browser": None, "context": None, "page": None})

def fetch_html_with_fallback(url: str, session: requests.Session) -> str:
    """Primárne requests; pri 500/429/403 alebo prázdnom tele fallback cez Playwright (ak dostupný a povolený)."""
    r = get(session, url)
    if r.status_code == 200 and (r.text or "").strip():
        return r.text

    if r.status_code in (429,403,500,502,503,504) and USE_PLAYWRIGHT_FALLBACK:
        try:
            _pw_start()
            page = _pw["page"]
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # cookie banner
            try:
                btn = page.get_by_role("button", name=re.compile("Súhlasím|Prijať|Accept|I agree", re.I))
                if btn.count() > 0:
                    btn.first.click(timeout=2000)
            except Exception:
                pass
            # malý lazy scroll – niekedy doplní DOM
            last_h = -1
            for _ in range(3):
                page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
                page.wait_for_timeout(400)
                h = page.evaluate("document.body.scrollHeight")
                if h == last_h: break
                last_h = h
            html = page.content()
            if html and len(html) > 1000:
                return html
        except Exception:
            # ak zlyhá aj playwright, vráť aspoň text/None (nech sa zaloguje detail fail)
            pass

    # requests fallback – vráť čo je (aj keby to bolo prázdne), nech sa to zaloguje
    return r.text or ""

# ----------------- Linky (unikátne podľa listing_id) -----------------

def extract_detail_links(html: str, page_url: str, limit: int | None) -> list:
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

# ----------------- Základné util -----------------

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

# ----------------- Adresa → street_or_locality -----------------

def extract_from_address(addr: str) -> dict:
    out = {"street_or_locality":None,"rooms":None,"area_m2":None,"condition":None}
    if not addr: return out
    txt = " ".join(addr.split())
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if parts:
        out["street_or_locality"] = parts[0] or None
    m = ROOMS_RE.search(txt)
    if m:
        try: out["rooms"] = int(m.group(1))
        except: pass
    m = AREA_RE.search(txt)
    if m:
        try: out["area_m2"] = _to_float_area(m.group(1))
        except: pass
    low = norm_txt(txt)
    for ck in CONDITION_KEYWORDS:
        if norm_txt(ck) in low:
            out["condition"] = ck; break
    return out

# ----------------- Cena -----------------

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
                        if p: return p
                for v in obj.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(obj, list): stack.extend(obj)
    return None

def price_from_meta(soup: BeautifulSoup):
    for sel in ["meta[itemprop='price']","meta[property='product:price:amount']","meta[property='og:price:amount']"]:
        for m in soup.select(sel):
            p = parse_int_price(m.get("content"))
            if p: return p
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
    if best is not None: return best
    text_all = " ".join(soup.stripped_strings)
    for m in PRICE_RE.finditer(text_all):
        start = m.start()
        near = text_all[start:start+30]
        if is_per_sqm(near): continue
        p = parse_int_price(m.group(1))
        if p: return p
    return None

# ----------------- Typ nehnuteľnosti -----------------

def best_type_match(texts):
    hay = " | ".join([t for t in texts if t])[:20000].lower()
    for t in sorted(KNOWN_TYPES, key=len, reverse=True):
        if t.lower() in hay:
            return t
    return None

def property_type_from_breadcrumbs(soup: BeautifulSoup):
    texts = []
    for nav in soup.select("nav, .breadcrumbs, [class*='breadcrumb']"):
        for a in nav.find_all(["a","span"]):
            t = clean_spaces(a.get_text(" "))
            if t and len(t) <= 60: texts.append(t)
    return best_type_match(texts)

def property_type_from_detail(soup: BeautifulSoup, title: str = None, description_text: str = None, addr_text: str = None):
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
    breadcrumb_t = property_type_from_breadcrumbs(soup)
    if breadcrumb_t: return breadcrumb_t
    texts = []
    h1 = soup.find("h1")
    if h1:
        near = [h1, h1.parent]; near.extend(h1.find_all_previous(limit=15)); near.extend(h1.find_all_next(limit=30))
        for n in near:
            if not hasattr(n,"get_text"): continue
            txt = " ".join((n.get_text(" ") or "").split())
            if txt and len(txt) <= 120: texts.append(txt)
    if title: texts.append(title)
    if description_text: texts.append(description_text)
    if addr_text: texts.append(addr_text)
    return best_type_match(texts)

def fallback_type_from_text(*texts):
    hay = norm_txt(" | ".join([t for t in texts if t]))
    if any(norm_txt(k) in hay for k in KEY_POZEMOK): return "Pozemok"
    if any(norm_txt(k) in hay for k in KEY_OBJEKT):  return "Objekt"
    return None

# ----------------- POPIS (plný text) -----------------

def _text_of(node):
    if not node: return None
    for br in node.find_all("br"):
        br.replace_with("\n")
    txt = (node.get_text("\n") or "").strip()
    txt = re.sub(r"[ \t\f\v]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt

UI_BLACKLIST_LINE = (
    "správa predajcovi","napísať správu","meno a priezvisko","telefón","e-mail",
    "odoslať správu","súhlasím so všeobecnými obchodnými podmienkami",
    "informáciou o spracúvaní osobných údajov","nahlásiť ponuku",
    "ďalšie inzeráty predajcu","objavte viac","exkluzívne projekty",
    "zobraziť na mape","zobraziť číslo","kontaktovať predajcu","predávajúci",
    "premium","video","3d obhliadka","magazín","užitočné info","reklama",
)
UI_BLACKLIST_RE = re.compile("|".join([re.escape(x) for x in UI_BLACKLIST_LINE]), re.I)

STOP_HEADINGS = (
    "lokalita","predávajúci","predavajuci","ďalšie inzeráty","dalsie inzeraty",
    "objavte viac","exkluzívne projekty","exkluzivne projekty","podobné",
    "nenašli ste všetky informácie","nenasli ste vsetky informacie",
)

def _clean_description_text(txt: str) -> str:
    lines = [l.strip() for l in txt.splitlines()]
    keep = []
    for l in lines:
        if not l:
            keep.append(""); continue
        if UI_BLACKLIST_RE.search(l): continue
        if len(l) <= 2: continue
        keep.append(l)
    out = "\n".join(keep).strip()
    out = re.sub(r"\s+Čítať ďalej\s*$", "", out, flags=re.I)
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()

def _scope_main(soup: BeautifulSoup):
    main = soup.find("main")
    if main: return main
    candidates = []
    for tag in soup.find_all(["article","section","div"]):
        try:
            t = clean_spaces(tag.get_text(" "))
        except Exception:
            t = ""
        if len(t) > 1000:
            candidates.append((len(t), tag))
    if candidates:
        candidates.sort(reverse=True, key=lambda x: x[0])
        return candidates[0][1]
    return soup

def _find_heading_node(scope: BeautifulSoup):
    for h in scope.find_all(["h1","h2","h3","h4","h5"]):
        t = clean_spaces(h.get_text(" "))
        n = norm_txt(t)
        if any(k in n for k in ("popis", "opis nehnutel", "o nehnutel", "informac", "detail")):
            return h
    return None

def _collect_after_heading(heading_node):
    parts = []
    cur = heading_node
    hops = 0
    while cur and hops < 180:
        cur = cur.find_next_sibling()
        if not cur: break
        hops += 1
        name = getattr(cur,"name","")
        if name in ("h1","h2","h3","h4","h5"):
            t = norm_txt(_text_of(cur) or "")
            if any(st in t for st in STOP_HEADINGS): break
            break
        if name in ("style","script","form","nav","aside"): continue
        try:
            txt = _text_of(cur) or ""
        except Exception:
            txt = ""
        if not txt.strip(): continue
        if UI_BLACKLIST_RE.search(txt): continue
        if len(txt) >= 10: parts.append(txt)
    text = "\n\n".join(parts).strip()
    return _clean_description_text(text)

def _from_jsonld_or_itemprop(soup: BeautifulSoup):
    for sc in soup.find_all("script", type=lambda t: t and "ld+json" in t):
        payload = sc.string or sc.get_text() or ""
        if not payload.strip(): continue
        try: data = json.loads(payload)
        except Exception: continue
        stack = [data]
        while stack:
            obj = stack.pop()
            if isinstance(obj, dict):
                desc = obj.get("description") or obj.get("Description")
                if isinstance(desc, str) and len(desc.strip()) > 80:
                    return _clean_description_text(desc)
                for v in obj.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(obj, list):
                stack.extend(obj)
    node = soup.find(attrs={"itemprop":"description"})
    if node:
        txt = _text_of(node)
        if txt and len(txt) > 80:
            return _clean_description_text(txt)
    return None

def _from_scripts_initial_state(soup: BeautifulSoup):
    keys = ("longDescription","descriptionLong","description","articleBody","body","text","content")
    best = ""
    for sc in soup.find_all("script"):
        payload = sc.string or sc.get_text() or ""
        if not payload or len(payload) < 80: continue
        for key in keys:
            pattern = r'"%s"\s*:\s*"(.*?)"' % re.escape(key)
            for m in re.finditer(pattern, payload, re.I | re.S):
                val = m.group(1)
                val = val.replace('\\"','"').replace("\\n","\n").replace("\\r"," ").replace("\\t"," ")
                val = _clean_description_text(val)
                if len(val) > len(best): best = val
    return best or None

def extract_description_block(soup: BeautifulSoup):
    scope = _scope_main(soup)
    heading = _find_heading_node(scope)
    if heading:
        t = _collect_after_heading(heading)
        if t and len(t) >= 80:
            return t, "heading-section"
    t = _from_jsonld_or_itemprop(soup)
    if t and len(t) >= 80:
        return t, "structured-data"
    t = _from_scripts_initial_state(soup)
    if t and len(t) >= 80:
        return t, "script-state"
    best = ""
    for box in scope.find_all(["section","div","article"]):
        if box.find(["form","nav","aside"]): continue
        try:
            txt = _text_of(box) or ""
        except Exception:
            continue
        if len(txt) < 200: continue
        if UI_BLACKLIST_RE.search(txt): continue
        low = norm_txt(txt)
        if any(st in low for st in STOP_HEADINGS): continue
        sentences = txt.count(".") + txt.count("?") + txt.count("!")
        score = sentences*3 - (txt.count("€") + txt.lower().count(" m2") + txt.lower().count(" m²"))*2 - len(box.find_all("a"))*2
        if score <= 0: continue
        if len(txt) > len(best): best = txt
    best = _clean_description_text(best)
    if best and len(best) >= 80:
        return best, "fallback-bigblock"
    return None, None

# ----------------- História cien -----------------

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
    try: return int(round(float(p)))
    except Exception: return None

def current_price_status(price_eur, price_note):
    if price_eur is not None: return "number"
    if price_note: return "note"
    return "unknown"

def sanitize_event(ev: dict) -> dict:
    if not isinstance(ev, dict): ev = {}
    price = _canon_price(ev.get("price_eur"))
    note  = ev.get("price_note") or None
    date_str = ev.get("date") or TODAY.isoformat()
    status = ev.get("status") or current_price_status(price, note)
    return {"date": str(date_str), "status": status, "price_eur": price, "price_note": note}

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
        try: return json.loads(s)
        except Exception: return []
    try:
        s = str(cell).strip()
        if not s or s.lower() in ("nan","none","null"): return []
        return json.loads(s)
    except Exception:
        return []

def sanitize_history(hist) -> list:
    if not isinstance(hist, list): return []
    out = []
    for ev in hist:
        try: out.append(sanitize_event(dict(ev)))
        except Exception: continue
    dedup = []
    for ev in out:
        key = (ev.get("status"), ev.get("price_eur"), ev.get("price_note"))
        if not dedup or key != (dedup[-1].get("status"), dedup[-1].get("price_eur"), dedup[-1].get("price_note")):
            dedup.append(ev)
    return dedup

def append_history_if_changed(row_dict, today_state: dict):
    hist_raw = load_history_from_cell(row_dict.get("price_history"))
    hist = sanitize_history(hist_raw)
    today_state = sanitize_event(today_state)

    changed = False
    if hist:
        last = hist[-1]
        key_last = (last.get("status"), last.get("price_eur"), last.get("price_note"))
        key_new  = (today_state.get("status"), today_state.get("price_eur"), today_state.get("price_note"))
        if not (last.get("date") == TODAY.isoformat() and key_last == key_new):
            if key_last != key_new:
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
    row_dict["price_history"] = json.dumps(hist, ensure_ascii=False, allow_nan=False)
    return row_dict

# ----------------- VLASTNOSTI (EXTRAKCIA + NORMALIZÁCIA) -----------------

def _num_from_text_m(text: str):
    if not text: return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*m\b", text.replace("\u00A0"," "), flags=re.I)
    if not m:
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*$", text)
    if not m: return None
    s = m.group(1).replace(",", ".")
    try: return float(s)
    except: return None

def _num_from_text_m2(text: str):
    if not text: return None
    m = re.search(r"(\d{1,3}(?:[ \u00A0\u202F.]?\d{3})*(?:[.,]\d+)?|\d+(?:[.,]\d+)?)\s*m(?:2|²)\b", text, flags=re.I)
    if not m:
        m = re.search(r"(\d{2,5})(?!\d)", text)
    if not m: return None
    s = m.group(1)
    s = s.replace("\u00A0","").replace("\u202F","").replace(" ","").replace(".","").replace(",", ".")
    try: return float(s)
    except: return None

def extract_features_pairs(soup: BeautifulSoup) -> list[dict]:
    pairs = []
    def push(k, v):
        k = clean_spaces(k or ""); v = clean_spaces(v or "")
        if k and v: pairs.append({"key": k, "value": v})
    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th","td"])
            if len(cells) >= 2:
                k = cells[0].get_text(" ", strip=True)
                v = cells[1].get_text(" ", strip=True)
                push(k, v)
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            push(dt.get_text(" ", strip=True), dd.get_text(" ", strip=True))
    for li in soup.find_all("li"):
        t = clean_spaces(li.get_text(" ", strip=True))
        if ":" in t and 3 < len(t) <= 200:
            k, v = t.split(":", 1); push(k, v)
    for row in soup.select("[class*='row'], [class*='item'], [class*='property'], [class*='attribute']"):
        lbl = None; val = None
        lab = row.find(attrs={"class": re.compile(r"(label|name|title)", re.I)})
        if lab: lbl = lab.get_text(" ", strip=True)
        valn = row.find(attrs={"class": re.compile(r"(value|data|content)", re.I)})
        if valn: val = valn.get_text(" ", strip=True)
        if lbl and val: push(lbl, val)
    cleaned = []
    for p in pairs:
        k = norm_txt(p["key"])
        if not k or any(bad in k for bad in ("kontakt","predavaj","inzer","map","cena za m")):
            continue
        cleaned.append(p)
    return cleaned

def normalize_features(pairs: list[dict]) -> dict:
    out = {
        "land_area_m2": None,
        "builtup_area_m2": None,
        "plot_width_m": None,
        "orientation": None,
        "ownership": None,
        "terrain": None,
        "water": None,
        "electricity": None,
        "gas": None,
        "waste": None,
        "heating": None,
        "rooms_count": None,
    }
    def key_is(k: str, *needles):
        nk = norm_txt(k)
        return any(n in nk for n in needles)
    for p in pairs:
        k, v = p.get("key",""), p.get("value","")
        if key_is(k, "vymera pozemku","rozloha pozemku","plocha pozemku","pozemok","celkova plocha pozemku"):
            val = _num_from_text_m2(v)
            if val: out["land_area_m2"] = val; continue
        if key_is(k, "zastavana plocha"):
            val = _num_from_text_m2(v)
            if val: out["builtup_area_m2"] = val; continue
        if key_is(k, "sirka pozemku","sirka"):
            val = _num_from_text_m(v)
            if val: out["plot_width_m"] = val; continue
        if key_is(k, "orientacia"):
            out["orientation"] = v; continue
        if key_is(k, "vlastnictvo","vlastnik"):
            out["ownership"] = v; continue
        if key_is(k, "teren","sklon"):
            out["terrain"] = v; continue
        if key_is(k, "voda","pripojka vody","vodovod"):
            out["water"] = v; continue
        if key_is(k, "elektrina","elektro","elektr"):
            out["electricity"] = v; continue
        if key_is(k, "plyn"):
            out["gas"] = v; continue
        if key_is(k, "kanalizacia","odpad","splask"):
            out["waste"] = v; continue
        if key_is(k, "kurenie","vykurovanie"):
            out["heating"] = v; continue
        if key_is(k, "pocet izieb","izby","izieb"):
            m = re.search(r"\d+", v)
            if m:
                try: out["rooms_count"] = int(m.group(0))
                except: pass
    return out

# ----------------- Detail parser -----------------

def parse_detail(session: requests.Session, url: str, seq_in_run: int, type_hint: str = None) -> dict:
    html = fetch_html_with_fallback(url, session)
    if not html or len(html) < 500:
        raise RuntimeError(f"empty/short HTML for {url}")

    soup = BeautifulSoup(html, "html.parser")

    listing_id = None
    m = re.search(r"Číslo\s+inzerátu\s*:\s*([A-Za-z0-9_-]+)", html)
    if m: listing_id = m.group(1)
    else:
        m2 = ID_FROM_URL_RE.search(url)
        if m2: listing_id = m2.group(1)

    title = None; h1 = soup.find("h1")
    if h1: title = clean_spaces(h1.get_text(" "))

    raw_address = None
    if h1:
        nxt = h1.find_next()
        for _ in range(8):
            if not nxt: break
            txt = clean_spaces(nxt.get_text(" "))
            if ("," in txt or "okres" in txt.lower()) and 5 < len(txt) < 200:
                raw_address = txt; break
            nxt = nxt.find_next()
    if not raw_address:
        all_txt = soup.get_text(" ", strip=True)
        for chunk in re.split(r"\s{2,}|[\r\n]+", all_txt):
            if ("okres" in chunk.lower()) and len(chunk) < 220:
                raw_address = clean_spaces(chunk); break

    price_eur = price_from_jsonld(soup) or price_from_meta(soup) or price_from_text(soup, h1)
    price_note = None
    if price_eur is None:
        ta = (soup.get_text(" ", strip=True) or "").lower()
        for note in ("cena dohodou","cena v rk","na vyžiadanie","v rk"):
            if note in ta: price_note = note; break

    if price_eur is None and not price_note:
        dbg = os.path.join(DEBUG_DIR, f"detail_no_price_{listing_id or 'NA'}_{datetime.now().strftime('%H%M%S')}.html")
        with open(dbg, "w", encoding="utf-8") as f: f.write(html)
        if VERBOSE: print(f"[DEBUG] cena nezistená -> {dbg}", flush=True)

    description_text, _src = extract_description_block(soup)

    pairs = extract_features_pairs(soup)
    features_json = json.dumps(pairs, ensure_ascii=False)
    norm_feats = normalize_features(pairs)

    prop_type = type_hint or property_type_from_detail(soup, title=title, description_text=description_text, addr_text=raw_address)
    if not prop_type:
        prop_type = fallback_type_from_text(raw_address, title, description_text, url) or None

    addr_info = extract_from_address(raw_address)
    obec = most_specific_obec(raw_address, title, description_text) or "Liptovský Mikuláš"
    street = addr_info.get("street_or_locality")
    if street:
        s_norm = norm_txt(street)
        if s_norm in LM_OBCE_NORM or s_norm == norm_txt(obec):
            street = None

    if addr_info.get("area_m2") is None:
        for p in pairs:
            nk = norm_txt(p.get("key",""))
            if any(x in nk for x in ("uzitkova plocha","podlahova plocha","obyvarna plocha","celkova podlahova plocha")):
                val = _num_from_text_m2(p.get("value",""))
                if val:
                    addr_info["area_m2"] = val
                    break

    return {
        "listing_id": listing_id,
        "pk": (listing_id or url),
        "url": url,
        "obec": obec,
        "street_or_locality": street,
        "price_eur": _nan_to_none(price_eur),
        "price_note": (price_note or None),
        "title": title,
        "property_type": prop_type,
        "rooms": addr_info.get("rooms"),
        "area_m2": addr_info.get("area_m2"),
        "condition": addr_info.get("condition"),
        "description_text": description_text,
        "features_json": features_json,
        **norm_feats,
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# ----------------- Perzistencia -----------------

def load_master_xlsx(path: str) -> pd.DataFrame:
    cols = [
        "seq_global","listing_id","pk","url",
        "obec","street_or_locality",
        "price_eur","price_note","price_status",
        "price_first_eur","price_first_note","price_first_date",
        "price_last_change_date","price_changes_count","price_history",
        "title","property_type","rooms","area_m2","condition",
        "description_text","features_json",
        "land_area_m2","builtup_area_m2","plot_width_m","orientation","ownership",
        "terrain","water","electricity","gas","waste","heating","rooms_count",
        "first_seen","last_seen","days_listed","active","inactive_since","scraped_at",
    ]
    if os.path.exists(path):
        df = pd.read_excel(path, engine="openpyxl")
        for legacy in ("address","adresa","mesto","city","district","seq_in_run"):
            if legacy in df.columns:
                df.drop(columns=[legacy], inplace=True, errors="ignore")
        for c in cols:
            if c not in df.columns: df[c] = pd.Series(dtype="object")
        for c in ("first_seen","last_seen","price_first_date","price_last_change_date","inactive_since"):
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
        if "price_changes_count" in df.columns:
            df["price_changes_count"] = pd.to_numeric(df["price_changes_count"], errors="coerce").fillna(0).astype(int)
        if "seq_global" in df.columns:
            df["seq_global"] = pd.to_numeric(df["seq_global"], errors="coerce")
        if "price_history" in df.columns:
            fixed = []
            for _, r in df.iterrows():
                hist = load_history_from_cell(r.get("price_history"))
                fixed.append(json.dumps(sanitize_history(hist), ensure_ascii=False, allow_nan=False))
            df["price_history"] = fixed
        return df[[c for c in cols]]
    return pd.DataFrame(columns=cols)

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

# ----------------- BigQuery upload (v2) -----------------

def upload_to_bigquery(master: pd.DataFrame, df_today: pd.DataFrame):
    if not BQ_ENABLE:
        print("[BQ] Preskakujem upload (BQ_ENABLE!=true).")
        return
    try:
        import pandas_gbq
    except Exception as e:
        print(f"[BQ] pandas-gbq nie je dostupný: {e}")
        return

    project_id = GCP_PROJECT_ID
    dataset = BQ_DATASET
    location = BQ_LOCATION

    def to_str(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype("string").where(df[c].notna(), None)

    def to_int(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    def to_float(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    def to_bool(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = df[c].astype("boolean")

    def to_date(df, cols):
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # MASTER
    m = master.copy()
    to_str(m, ["listing_id","pk","url","obec","street_or_locality","price_note","title","property_type",
               "condition","description_text","features_json","orientation","ownership","terrain","water",
               "electricity","gas","waste","heating","price_status","price_history","scraped_at"])
    to_int(m, ["seq_global","price_eur","price_first_eur","price_changes_count","rooms","rooms_count"])
    to_float(m, ["area_m2","land_area_m2","builtup_area_m2","plot_width_m"])
    to_bool(m, ["active"])
    to_date(m, ["first_seen","last_seen","price_first_date","price_last_change_date","inactive_since"])
    if "days_listed" in m.columns:
        m["days_listed"] = pd.to_numeric(m["days_listed"], errors="coerce").astype("Int64")

    master_schema = [
        {"name":"seq_global","type":"INT64"},
        {"name":"listing_id","type":"STRING"},
        {"name":"pk","type":"STRING"},
        {"name":"url","type":"STRING"},
        {"name":"obec","type":"STRING"},
        {"name":"street_or_locality","type":"STRING"},
        {"name":"price_eur","type":"INT64"},
        {"name":"price_note","type":"STRING"},
        {"name":"price_status","type":"STRING"},
        {"name":"price_first_eur","type":"INT64"},
        {"name":"price_first_note","type":"STRING"},
        {"name":"price_first_date","type":"DATE"},
        {"name":"price_last_change_date","type":"DATE"},
        {"name":"price_changes_count","type":"INT64"},
        {"name":"price_history","type":"STRING"},
        {"name":"title","type":"STRING"},
        {"name":"property_type","type":"STRING"},
        {"name":"rooms","type":"INT64"},
        {"name":"area_m2","type":"FLOAT64"},
        {"name":"condition","type":"STRING"},
        {"name":"description_text","type":"STRING"},
        {"name":"features_json","type":"STRING"},
        {"name":"land_area_m2","type":"FLOAT64"},
        {"name":"builtup_area_m2","type":"FLOAT64"},
        {"name":"plot_width_m","type":"FLOAT64"},
        {"name":"orientation","type":"STRING"},
        {"name":"ownership","type":"STRING"},
        {"name":"terrain","type":"STRING"},
        {"name":"water","type":"STRING"},
        {"name":"electricity","type":"STRING"},
        {"name":"gas","type":"STRING"},
        {"name":"waste","type":"STRING"},
        {"name":"heating","type":"STRING"},
        {"name":"rooms_count","type":"INT64"},
        {"name":"first_seen","type":"DATE"},
        {"name":"last_seen","type":"DATE"},
        {"name":"days_listed","type":"INT64"},
        {"name":"active","type":"BOOL"},
        {"name":"inactive_since","type":"DATE"},
        {"name":"scraped_at","type":"STRING"},
    ]

    d = df_today.copy()
    if not d.empty:
        d["batch_ts"] = pd.Timestamp.utcnow()
    daily_schema = [s for s in master_schema if s["name"] not in ("seq_global","days_listed","active","inactive_since")]
    daily_schema += [{"name":"batch_ts","type":"TIMESTAMP"}]

    pandas_gbq.to_gbq(
        m, f"{dataset}.listings_master", project_id=project_id,
        if_exists="replace", table_schema=master_schema, location=location, progress_bar=False
    )
    if not d.empty:
        pandas_gbq.to_gbq(
            d, f"{dataset}.listings_daily", project_id=project_id,
            if_exists="append", table_schema=daily_schema, location=location, progress_bar=False
        )
    print("[BQ] Upload hotový.")

# ----------------- Crawler -----------------

def crawl_to_excel(base_url: str = BASE_URL, max_pages: int = MAX_PAGES,
                   max_links_per_page: int | None = MAX_LINKS_PER_PAGE,
                   max_listings_total: int | None = MAX_LISTINGS_TOTAL):
    s = new_session()

    master = load_master_xlsx(MASTER_XLSX)
    if master.empty:
        master["seen_today"] = pd.Series(dtype=bool)
    else:
        master["seen_today"] = False

    next_seq = (int(master["seq_global"].max()) if pd.notna(master["seq_global"]).any() else 0)

    total_added = 0
    list_type_hints = {}

    print(f"➡️  CRAWL: {base_url}", flush=True)

    for page in range(1, max_pages + 1):
        if max_listings_total is not None and total_added >= max_listings_total:
            break

        page_url = base_url if page == 1 else f"{base_url}{'&' if '?' in base_url else '?'}page={page}"
        r = get(s, page_url)
        status = r.status_code
        html = r.text

        dbg_path = save_debug(html, f"results_page{page}_{TS}.html")
        print(f"{dbg_path}", flush=True)
        print(f"[INFO] page {page} {page_url} -> status {status}", flush=True)

        if status == 404:
            print(f"[INFO] Strana {page} neexistuje (404).", flush=True)
            break

        links = extract_detail_links(html, page_url, limit=max_links_per_page)

        if max_listings_total is not None:
            remain = max(0, max_listings_total - total_added)
            if len(links) > remain:
                links = links[:remain]

        if VERBOSE:
            print(f"[DEBUG] found detail links on page {page}: {len(links)}", flush=True)
        if not links:
            if page == 1:
                print("[DEBUG] 0 linkov na prvej stránke – skontroluj DEBUG HTML.", flush=True)
            break

        # type-hint z list karty
        try:
            soup = BeautifulSoup(html, "html.parser")
            mapping = {}
            for card in soup.select("article, li, .card, .ListingCard, [data-testid*='listing-card']"):
                a = card.find("a", href=True)
                if not a or "/detail/" not in a.get("href", ""):
                    continue
                abs_url = urljoin(page_url, a["href"])
                texts = []
                for sel in ["h2", "h3", "[class*='title']", "[data-testid*='title']"]:
                    el = card.select_one(sel)
                    if el:
                        t = clean_spaces(el.get_text(" "))
                        if t and len(t) <= 160:
                            texts.append(t)
                for badge in card.select(".badge, .chip, .tag, [class*='badge'], [class*='chip'], [class*='tag']"):
                    t = clean_spaces(badge.get_text(" "))
                    if t and len(t) <= 60:
                        texts.append(t)
                for small in card.select("small, .subtitle, [class*='subtitle']"):
                    t = clean_spaces(small.get_text(" "))
                    if t and len(t) <= 100:
                        texts.append(t)
                hint = best_type_match(texts)
                if hint:
                    mapping[abs_url] = hint
            list_type_hints.update(mapping)
        except Exception as e:
            if VERBOSE:
                print(f"[WARN] type-hints parse error on page {page}: {e}", flush=True)

        # parsovanie detailov
        for idx, url in enumerate(links, start=1):
            if max_listings_total is not None and total_added >= max_listings_total:
                break

            type_hint = list_type_hints.get(url)
            try:
                rec = parse_detail(s, url, seq_in_run=idx, type_hint=type_hint)
            except Exception as e:
                if VERBOSE:
                    print(f"Error:  detail fail {url} -> {e}", flush=True)
                continue

            pk = rec.get("pk") or url
            existing_idx = master.index[master["pk"] == pk].tolist()

            if existing_idx:
                # update existujúceho záznamu
                i = existing_idx[0]
                row = master.loc[i].to_dict()

                today_state = {"price_eur": rec.get("price_eur"), "price_note": rec.get("price_note")}
                row = append_history_if_changed(row, today_state)

                for k in [
                    "url", "listing_id",
                    "obec", "street_or_locality",
                    "title", "property_type", "rooms", "area_m2", "condition",
                    "description_text", "features_json",
                    "land_area_m2", "builtup_area_m2", "plot_width_m",
                    "orientation", "ownership", "terrain",
                    "water", "electricity", "gas", "waste", "heating", "rooms_count",
                ]:
                    v = rec.get(k)
                    if v is not None and v != "":
                        row[k] = v

                row["last_seen"] = TODAY
                row["active"] = True
                row["inactive_since"] = None
                row["scraped_at"] = rec.get("scraped_at")
                row["seen_today"] = True

                for k, v in row.items():
                    if k in master.columns:
                        master.at[i, k] = v

            else:
                # nový záznam
                next_seq += 1
                rec_out = {c: None for c in master.columns}
                rec_out.update(rec)

                rec_out["seq_global"] = next_seq
                rec_out["first_seen"] = TODAY
                rec_out["last_seen"] = TODAY
                rec_out["active"] = True
                rec_out["inactive_since"] = None
                rec_out["price_status"] = current_price_status(rec.get("price_eur"), rec.get("price_note"))
                rec_out["price_changes_count"] = 0
                rec_out["price_history"] = "[]"
                rec_out["seen_today"] = True

                rec_out = append_history_if_changed(
                    rec_out, {"price_eur": rec.get("price_eur"), "price_note": rec.get("price_note")}
                )

                master = pd.concat([master, pd.DataFrame([rec_out])], ignore_index=True)

            total_added += 1
            if PROGRESS_EVERY and (total_added % PROGRESS_EVERY == 0):
                print(f"[{total_added}] {url}", flush=True)

            sleep_a_bit()

    # označ nevidené ako neaktívne
    if "seen_today" in master.columns:
        mask_unseen = master["seen_today"] != True
        master.loc[mask_unseen & (master["active"] == True), "inactive_since"] = TODAY
        master.loc[mask_unseen, "active"] = False

    # prepočítaj days_listed
    master = finalize_days(master)

    # snapshot iba dnešné
    try:
        snap = master[master["seen_today"] == True].copy()
    except Exception:
        snap = master.copy()

    # GARANCIA UNIKÁTNEHO PK
    if "pk" in master.columns:
        master = master.drop_duplicates(subset=["pk"], keep="last")
    if "pk" in snap.columns:
        snap = snap.drop_duplicates(subset=["pk"], keep="last")

    # persist XLSX (debug)
    if SAVE_EXCEL:
        save_excel(master.drop(columns=["seen_today"]), MASTER_XLSX)
        save_excel(snap.drop(columns=["seen_today"], errors="ignore"), SNAPSHOT_XLSX)
        print(f"✅ Uložený master:   {MASTER_XLSX}", flush=True)
        print(f"✅ Uložený snapshot: {SNAPSHOT_XLSX}", flush=True)

    # upload do BQ
    upload_to_bigquery(
        master.drop(columns=["seen_today"], errors="ignore"),
        snap.drop(columns=["seen_today"], errors="ignore"),
    )

    # vypni playwright ak bol spustený
    _pw_stop()

if __name__ == "__main__":
    crawl_to_excel()
