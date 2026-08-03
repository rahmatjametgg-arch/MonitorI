"""
SPIDERMAT OTP BOT — FORWARD MODE (REFACTORED)
Baca cookie dari cookie.json → poll IVAS → forward OTP ke Telegram.
Command: /addbot /removebot /listbot
"""

import httpx
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import threading
import json
import os
import hashlib
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import phonenumbers
from phonenumbers import geocoder
from colorama import init, Fore, Style

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

# CONFIG
BOT_TOKEN  = os.getenv("BOT_TOKEN", "")
OWNER_ID   = int(os.getenv("OWNER_ID", "0"))

IVAS_USERNAME = os.getenv("IVAS_USERNAME", "")
IVAS_PASSWORD = os.getenv("IVAS_PASSWORD", "")

DEFAULT_TARGET = -1003686221386

CHANNEL_LINK = "https://t.me/matchaappp"
NUMBER_LINK  = "https://t.me/matchaappp"

COOKIE_FILE        = "cookie.json"
CACHE_FILE         = "file/sent_cache.json"
GROUPS_FILE        = "file/groups.json"
MAX_CACHE          = 2000

POLL_INTERVAL_MAX  = 5.0
MIN_IDLE_SLEEP     = 2.0
KEEPALIVE_INTERVAL = 480

# LOGGING
_LOG_ICONS = {
    "OTP":      "🟢",
    "COOKIE":   "🍪",
    "CONFIG":   "⚙️ ",
    "WORKER":   "🔄",
    "RANGE":    "📡",
    "CSRF":     "🔑",
    "KA-OK":    "💚",
    "KA-WARN":  "🟡",
    "KA-ERR":   "🔴",
    "KEEPALIVE":"🫀",
    "SERVER":   "🌐",
    "CACHE":    "💾",
    "TG-ERR":   "❌",
    "NUM":      "📟",
    "SMS":      "📨",
    "THREAD+":  "🧵",
    "SHUTDOWN": "🛑",
    "FATAL":    "💀",
    "CMD":      "⌨️ ",
    "GROUP":    "👥",
    "LOGIN":    "🔐",
}

def _log(tag, msg, color=Fore.CYAN):
    icon  = _LOG_ICONS.get(tag, "•")
    ts    = datetime.now().strftime("%H:%M:%S")
    label = f"{icon} {tag:<9}"
    print(color + f"  {ts}  {label}  {msg}" + Style.RESET_ALL, flush=True)

# WORKER POOL
WORKER_POOL = [
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

_worker_lock          = threading.Lock()
_active_worker_idx    = 0
_worker_limited_until = {}
WORKER_LIMIT_COOLDOWN = 900

def get_base():
    with _worker_lock:
        return WORKER_POOL[_active_worker_idx % len(WORKER_POOL)]

def _all_workers_limited() -> bool:
    now = time.time()
    with _worker_lock:
        return all(_worker_limited_until.get(w, 0) >= now for w in WORKER_POOL)
def mark_worker_limited(url):
    global _active_worker_idx
    now = time.time()
    switched = False
    with _worker_lock:
        _worker_limited_until[url] = now + WORKER_LIMIT_COOLDOWN
        for i in range(1, len(WORKER_POOL) + 1):
            idx = (_active_worker_idx + i) % len(WORKER_POOL)
            if _worker_limited_until.get(WORKER_POOL[idx], 0) < now:
                _active_worker_idx = idx
                switched = True
                break
    if switched:
        _log("WORKER", f"rate-limited → pindah ke {get_base()}", Fore.YELLOW)
    else:
        _log("WORKER", "semua worker kena rate-limit — tunggu cooldown", Fore.RED)

_RATE_LIMIT_MARKERS = (
    "temporarily rate limited", "error 1027", "please check back later",
    "has been rate limited", "error 1015", "you have been blocked",
    "attention required", "error 1020", "checking your browser", "just a moment",
)

def is_worker_blocked(resp) -> bool:
    if resp is None:
        return False
    try:
        if resp.status_code == 429:
            return True
        sample = resp.text[:2000].lower()
        return any(m in sample for m in _RATE_LIMIT_MARKERS)
    except:
        return False

# COOKIE LOADING
def load_cookies():
    if not os.path.exists(COOKIE_FILE):
        _log("COOKIE", f"{COOKIE_FILE} tidak ditemukan!", Fore.RED)
        return []
    try:
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return []
        if isinstance(data, list):
            if all(isinstance(x, dict) and "name" in x and "value" in x for x in data):
                return [{x["name"]: x["value"] for x in data}]
            return data
        if isinstance(data, dict) and all(isinstance(v, dict) for v in data.values()):
            return list(data.values())
        if isinstance(data, dict):
            return [data]
    except Exception as e:
        _log("COOKIE", f"error load {COOKIE_FILE}: {e}", Fore.RED)
    return []

# HTTPX SESSION
def make_session(cookies: dict, timeout=30):
    hdrs = {
        "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           "https://ivasms.com",
        "Referer":          "https://ivasms.com/",
    }
    s = httpx.Client(
        follow_redirects=True,
        timeout=timeout,
        headers=hdrs,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
    )
    s.cookies.update(cookies)
    return s

# AUTO-LOGIN IVAS
_login_lock   = {}
_login_result = {}

def _get_login_csrf(session, base) -> str:
    try:
        r = session.get(f"{base}/login", timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        if meta and meta.get("content"):
            return meta["content"]
        inp = soup.find("input", {"name": "_token"})
        if inp and inp.get("value"):
            return inp["value"]
        m = re.search(r"['\"]_token['\"]\s*[,:]?\s*['\"]([A-Za-z0-9_\-+/=]{20,})['\"]", r.text)
        if m:
            return m.group(1)
    except Exception as e:
        _log("LOGIN", f"get login-csrf error: {e}", Fore.YELLOW)
    return ""
def auto_login_ivas(acc) -> bool:
    idx = acc["idx"]
    if not IVAS_USERNAME or not IVAS_PASSWORD:
        _log("LOGIN", f"akun #{idx}: IVAS_USERNAME/IVAS_PASSWORD belum diset di env!", Fore.YELLOW)
        return False

    if idx not in _login_lock:
        _login_lock[idx] = threading.Lock()
    if not _login_lock[idx].acquire(blocking=False):
        _login_lock[idx].acquire()
        _login_lock[idx].release()
        return _login_result.get(idx, False)

    try:
        _log("LOGIN", f"akun #{idx}: mencoba auto-login sebagai {IVAS_USERNAME}...", Fore.YELLOW)
        base = get_base()
        csrf = _get_login_csrf(acc["session"], base)
        if not csrf:
            _log("LOGIN", f"akun #{idx}: gagal dapat CSRF token login", Fore.RED)
            _login_result[idx] = False
            return False

        payload = {
            "_token":   csrf,
            "email":    IVAS_USERNAME,
            "password": IVAS_PASSWORD,
        }
        r = acc["session"].post(
            f"{base}/login",
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer":      f"{base}/login",
                "Origin":       "https://ivasms.com",
            },
            timeout=20,
        )

        success = r.status_code == 200 and "/login" not in str(r.url)
        if success:
            _recv_csrf_cache.pop(idx, None)
            _log("LOGIN", f"akun #{idx}: ✅ auto-login BERHASIL", Fore.GREEN)
        else:
            _log("LOGIN", f"akun #{idx}: ❌ auto-login GAGAL (url={r.url})", Fore.RED)

        _login_result[idx] = success
        return success

    except Exception as e:
        _log("LOGIN", f"akun #{idx}: exception saat auto-login: {e}", Fore.RED)
        _login_result[idx] = False
        return False
    finally:
        _login_lock[idx].release()

# CSRF CACHE
_recv_csrf_cache = {}
RECV_CSRF_TTL    = 900

def get_recv_csrf(acc, _retry=0) -> str:
    idx    = acc["idx"]
    now    = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < RECV_CSRF_TTL:
        return cached["csrf"]
    base     = get_base()
    recv_url = f"{base}/portal/sms/received"
    try:
        worker_before = base
        r = acc["session"].get(recv_url, timeout=15)
        if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
            mark_worker_limited(worker_before)
            return get_recv_csrf(acc, _retry + 1)
        if "/login" in str(r.url):
            if auto_login_ivas(acc):
                return get_recv_csrf(acc, _retry)
            return acc.get("csrf_token", "")
        soup = BeautifulSoup(r.text, "html.parser")
        csrf = ""
        meta = soup.find("meta", {"name": "csrf-token"})
        if meta:
            csrf = meta.get("content", "")
        if not csrf:
            inp = soup.find("input", {"name": "_token"})
            if inp:
                csrf = inp.get("value", "")
        if not csrf:
            m = re.search(r"['\"]_token['\"]\s*[,:]?\s*['\"]([A-Za-z0-9_\-+/=]{20,})['\"]", r.text)
            if m:
                csrf = m.group(1)
        if csrf:
            acc["csrf_token"] = csrf
            _recv_csrf_cache[idx] = {"csrf": csrf, "ts": now}
            return csrf
    except Exception as e:
        _log("CSRF", f"akun #{idx}: {e}", Fore.YELLOW)
    return acc.get("csrf_token", "")

# IVAS API
_ranges_cache     = {}
_ranges_429_until = {}
RANGES_CACHE_TTL  = 300

def _recv_headers(base):
    return {
        "Accept":           "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          f"{base}/portal/sms/received",
        "Origin":           "https://ivasms.com",
    }
def _check_and_handle_login_redirect(r, acc) -> bool:
    if "/login" in str(r.url):
        idx = acc["idx"]
        _log("WORKER", f"akun #{idx}: redirect ke /login, coba auto-login...", Fore.YELLOW)
        auto_login_ivas(acc)
        return True
    return False

def get_ranges(acc, _retry=0):
    idx = acc["idx"]
    now = time.time()
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    base          = get_base()
    today         = datetime.now().strftime("%Y-%m-%d")
    csrf          = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms",
        data={"_token": csrf, "from": today, "to": today},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r):
        mark_worker_limited(worker_before)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            _log("RANGE", "semua worker limited — skip poll ini", Fore.RED)
            entry = _ranges_cache.get(acc["idx"])
            return entry[1] if entry else []
        time.sleep(4 * (_retry + 1))
        return get_ranges(acc, _retry + 1)
    if r.status_code == 429:
        _ranges_429_until[idx] = now + 180
        entry = _ranges_cache.get(idx)
        _log("RANGE", f"akun #{idx} — 429, cooldown 3 menit, pakai cache lama", Fore.YELLOW)
        return entry[1] if entry else []
    if _check_and_handle_login_redirect(r, acc):
        return []
    soup   = BeautifulSoup(r.text, "html.parser")
    ranges = []
    for div in soup.find_all("div", onclick=True):
        if "toggleRange" in div["onclick"]:
            try:
                ranges.append(div["onclick"].split("'")[1])
            except:
                pass
    result = list(set(ranges))
    _ranges_429_until.pop(idx, None)
    if result:
        _ranges_cache[idx] = (now, result)
    return result

def get_ranges_cached(acc):
    idx  = acc["idx"]
    now  = time.time()
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    entry = _ranges_cache.get(idx)
    if entry:
        ts, cached = entry
        if now - ts < RANGES_CACHE_TTL:
            return cached
    return get_ranges(acc)

def get_numbers(acc, rng, _retry=0):
    base          = get_base()
    today         = datetime.now().strftime("%Y-%m-%d")
    csrf          = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms/number",
        data={"_token": csrf, "start": today, "end": today, "range": rng},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r):
        mark_worker_limited(worker_before)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            return []
        time.sleep(4 * (_retry + 1))
        return get_numbers(acc, rng, _retry + 1)
    if r.status_code == 429:
        return []
    if _check_and_handle_login_redirect(r, acc):
        return []
    soup    = BeautifulSoup(r.text, "html.parser")
    numbers = []
    for div in soup.find_all("div", onclick=True):
        try:
            val = div["onclick"].split("'")[1]
            if val and val != rng:
                numbers.append(val)
        except:
            pass
    return list(set(numbers))

def get_sms(acc, rng, number, _retry=0):
    base          = get_base()
    today         = datetime.now().strftime("%Y-%m-%d")
    csrf          = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms/number/sms",
        data={"_token": csrf, "start": today, "end": today, "Number": number, "Range": rng},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r):
        mark_worker_limited(worker_before)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            return []
        time.sleep(4 * (_retry + 1))
        return get_sms(acc, rng, number, _retry + 1)
    if r.status_code == 429:
        return []
    if _check_and_handle_login_redirect(r, acc):
        return []
    soup      = BeautifulSoup(r.text, "html.parser")
    sms_texts = []
    try:
        for t in soup.stripped_strings:
            t = t.strip().replace("<#>", "").strip()
            if re.fullmatch(r"[A-Za-z0-9]{10,}", t):
                continue
            t_low = t.lower()
            if any(x in t_low for x in ["sender", "revenue", "time"]):
                continue
            if re.search(r"\b\d{2}:\d{2}:\d{2}\b", t):
                continue
            if "$" in t:
                continue
            if t and "No SMS Found" not in t:
                sms_texts.append(t)
    except Exception as e:
        _log("SMS", f"parse error: {e}", Fore.RED)
    return list(dict.fromkeys(sms_texts))
# PLATFORM DETECTION
SERVICE_INFO = {
    "WHATSAPP":  {"icon": "💬",  "name": "WhatsApp",  "code": "WS"},
    "TELEGRAM":  {"icon": "✈️",  "name": "Telegram",  "code": "TG"},
    "GOOGLE":    {"icon": "🔍",  "name": "Google",    "code": "G" },
    "FACEBOOK":  {"icon": "📘",  "name": "Facebook",  "code": "FB"},
    "INSTAGRAM": {"icon": "📷",  "name": "Instagram", "code": "IG"},
    "TIKTOK":    {"icon": "🎵",  "name": "TikTok",    "code": "TT"},
    "GRAB":      {"icon": "🚗",  "name": "Grab",      "code": "GR"},
    "GOJEK":     {"icon": "🛵",  "name": "Gojek",     "code": "GJ"},
    "SHOPEE":    {"icon": "🟠",  "name": "Shopee",    "code": "SP"},
    "TOKOPEDIA": {"icon": "🛍️", "name": "Tokopedia", "code": "TP"},
    "PAYPAL":    {"icon": "🅿️",  "name": "PayPal",   "code": "PP"},
    "TWITTER":   {"icon": "🐦",  "name": "Twitter",   "code": "TW"},
    "AMAZON":    {"icon": "📦",  "name": "Amazon",    "code": "AMZ"},
    "NETFLIX":   {"icon": "🎬",  "name": "Netflix",   "code": "NF"},
    "APPLE":     {"icon": "🍎",  "name": "Apple",     "code": "APL"},
    "MICROSOFT": {"icon": "🪟",  "name": "Microsoft", "code": "MS"},
    "DISCORD":   {"icon": "🎮",  "name": "Discord",   "code": "DC"},
    "SNAPCHAT":  {"icon": "👻",  "name": "Snapchat",  "code": "SC"},
    "LINKEDIN":  {"icon": "💼",  "name": "LinkedIn",  "code": "LI"},
    "BINANCE":   {"icon": "🪙",  "name": "Binance",   "code": "BNB"},
    "BYBIT":     {"icon": "📊",  "name": "Bybit",     "code": "BB"},
    "OKX":       {"icon": "💹",  "name": "OKX",       "code": "OKX"},
}
_SVC_DEFAULT = {"icon": "💌", "name": "OTP", "code": "OT"}

_SVC_PATTERN = re.compile(
    r"(WhatsApp|Telegram|Google|Facebook|Instagram|TikTok|Grab|Gojek|Shopee|Tokopedia"
    r"|PayPal|Twitter|Amazon|Netflix|Apple|Microsoft|Discord|Snapchat|LinkedIn"
    r"|Binance|Bybit|OKX)",
    re.IGNORECASE,
)

def detect_service(text: str) -> dict:
    m = _SVC_PATTERN.search(text)
    if m:
        key = m.group(1).upper()
        return SERVICE_INFO.get(key, _SVC_DEFAULT)
    return _SVC_DEFAULT

# LANGUAGE DETECTION
_LANG_RULES = [
    ("ID", re.compile(r"\b(kode|verifikasi|masukkan|jangan|bagikan|konfirmasi|berlaku|menit|anda)\b", re.I)),
    ("AR", re.compile(r"[\u0600-\u06FF]")),
    ("ZH", re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]")),
    ("RU", re.compile(r"[\u0400-\u04FF]")),
    ("ES", re.compile(r"\b(código|verificación|ingresa|compartir|contraseña|minutos)\b", re.I)),
    ("FR", re.compile(r"\b(code|vérification|entrez|partagez|valide|minutes)\b", re.I)),
    ("PT", re.compile(r"\b(código|verificação|inserir|compartilhar|válido|minutos)\b", re.I)),
    ("DE", re.compile(r"\b(code|verifizierung|eingeben|teilen|gültig|minuten)\b", re.I)),
    ("TR", re.compile(r"\b(kod|doğrulama|girin|paylaşma|geçerli|dakika)\b", re.I)),
    ("HI", re.compile(r"[\u0900-\u097F]")),
    ("JA", re.compile(r"[\u3040-\u30FF\u31F0-\u31FF]")),
    ("KO", re.compile(r"[\uAC00-\uD7AF\u1100-\u11FF]")),
    ("TH", re.compile(r"[\u0E00-\u0E7F]")),
    ("VI", re.compile(r"\b(mã|xác minh|nhập|chia sẻ|hợp lệ|phút)\b", re.I)),
    ("EN", re.compile(r"\b(code|verify|enter|share|valid|minutes|otp|password)\b", re.I)),
]

def detect_sms_language(text: str) -> str:
    for lang_code, pattern in _LANG_RULES:
        if pattern.search(text):
            return lang_code
    return "EN"

# PHONE / COUNTRY HELPERS
def code_to_flag(code: str) -> str:
    try:
        return "".join(chr(127397 + ord(c)) for c in code.upper())
    except:
        return "🏳"

def detect_country_and_flag(full_num: str, fallback_country="UNKNOWN"):
    try:
        parsed  = phonenumbers.parse("+" + full_num, None)
        region  = phonenumbers.region_code_for_number(parsed)
        if region:
            flag         = code_to_flag(region)
            country_name = geocoder.description_for_number(parsed, "en")
            return (country_name.upper() if country_name else fallback_country), flag, region
    except:
        pass
    return fallback_country, "🏳", "??"

def parse_range(rng: str):
    country    = re.sub(r"\s*\(.*?\)", "", rng)
    country    = re.sub(r"\d+", "", country)
    country    = re.sub(r"\s+", " ", country).strip().upper()
    code_match = re.search(r"\((\d+)\)", rng)
    code       = code_match.group(1) if code_match else ""
    return country, code

def normalize_number(num: str, country_code: str) -> str:
    num = str(num).strip().replace(" ", "").replace("-", "").replace("+", "")
    if country_code and num.startswith(country_code):
        return num
    if num.startswith("0") and country_code:
        return country_code + num[1:]
    return num

def garage_mask_phone(full_num: str) -> tuple:
    n = str(full_num).replace("+", "").replace(" ", "")
    if len(n) >= 8:
        prefix = "+" + n[:4]
        last4  = n[-4:]
        return prefix, last4
    return "+" + n, ""

# MESSAGE BUILDER & KEYBOARD (UI SPIDERMAT)
def build_otp_message(
    otp:         str,
    svc:         dict,
    flag:        str,
    country:     str,
    region_code: str,
    full_num:    str,
    sms_text:    str = "",
) -> str:
    prefix, last4 = garage_mask_phone(full_num)
    masked_phone  = f"{prefix}🗿{last4}" if last4 else prefix
    lang_code     = detect_sms_language(sms_text) if sms_text else "ID"
    svc_code      = svc.get("code", "WS")

    return f"<b>SPIDERMAT BOT</b>   <code>SIGANTENG</code>\n{flag} #{region_code} {svc_code} {masked_phone} #{lang_code}"

def build_otp_keyboard(otp: str) -> dict:
    otp_display = f"{otp[:3]}-{otp[3:]}" if len(otp) == 6 else otp
    return {
        "inline_keyboard": [
            [
                {
                    "text":      f"📋 {otp_display}",
                    "copy_text": {"text": otp},
                },
                {
                    "text": "All File ↗",
                    "url":  CHANNEL_LINK,
                }
            ]
        ]
    }
# SENT CACHE
_sent_cache_lock = threading.Lock()
_cache_dirty     = False
_last_cache_save = 0.0

def load_sent_cache() -> set:
    os.makedirs("file", exist_ok=True)
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except:
        return set()

def save_sent_cache_now(cache: set):
    try:
        os.makedirs("file", exist_ok=True)
        lst = list(cache)
        if len(lst) > MAX_CACHE:
            lst = lst[-MAX_CACHE:]
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f)
    except Exception as e:
        _log("CACHE", f"save error: {e}", Fore.YELLOW)

sent_cache = load_sent_cache()

def cache_try_add(uid: str) -> bool:
    global _cache_dirty, _last_cache_save
    with _sent_cache_lock:
        if uid in sent_cache:
            return False
        sent_cache.add(uid)
    _cache_dirty = True
    if time.time() - _last_cache_save >= 5:
        with _sent_cache_lock:
            save_sent_cache_now(sent_cache)
        _last_cache_save = time.time()
        _cache_dirty = False
    return True

# GROUP TARGETS
_targets_lock    = threading.Lock()
_forward_targets: set = {DEFAULT_TARGET}

def _load_groups():
    if not os.path.exists(GROUPS_FILE):
        return
    try:
        with open(GROUPS_FILE, "r", encoding="utf-8") as f:
            ids = json.load(f)
        if isinstance(ids, list):
            with _targets_lock:
                for gid in ids:
                    _forward_targets.add(int(gid))
        _log("GROUP", f"{len(ids)} grup dimuat dari {GROUPS_FILE}", Fore.CYAN)
    except Exception as e:
        _log("GROUP", f"load error: {e}", Fore.YELLOW)

def _save_groups():
    try:
        os.makedirs("file", exist_ok=True)
        with _targets_lock:
            ids = list(_forward_targets)
        with open(GROUPS_FILE, "w", encoding="utf-8") as f:
            json.dump(ids, f)
    except Exception as e:
        _log("GROUP", f"save error: {e}", Fore.YELLOW)

def add_group(chat_id: int) -> bool:
    with _targets_lock:
        if chat_id in _forward_targets:
            return False
        _forward_targets.add(chat_id)
    _save_groups()
    return True

def remove_group(chat_id: int) -> bool:
    with _targets_lock:
        if chat_id not in _forward_targets or chat_id == DEFAULT_TARGET:
            return False
        _forward_targets.discard(chat_id)
    _save_groups()
    return True

def list_groups() -> list:
    with _targets_lock:
        return list(_forward_targets)

# TELEGRAM SEND
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=10, max_retries=0,
))

def _tg_post(chat_id, text, reply_markup=None, retries=3):
    payload = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    for attempt in range(retries):
        try:
            r    = _tg_session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                return True
            if r.status_code == 429:
                wait = data.get("parameters", {}).get("retry_after", 5)
                time.sleep(wait + 1)
                continue
            _log("TG-ERR", f"chat {chat_id}: {data.get('description', '?')}", Fore.RED)
            return False
        except Exception as e:
            if attempt == retries - 1:
                _log("TG-ERR", f"chat {chat_id}: {e}", Fore.RED)
            else:
                time.sleep(1.5 ** (attempt + 1))
    return False

def tg_send_msg(chat_id: int, text: str):
    _tg_post(chat_id, text)

def tg_send_otp(otp: str, msg_text: str):
    kb      = build_otp_keyboard(otp)
    targets = list_groups()

    def _send_one(cid):
        _tg_post(cid, msg_text, reply_markup=kb)

    if len(targets) == 1:
        _send_one(targets[0])
    else:
        with ThreadPoolExecutor(max_workers=min(8, len(targets)), thread_name_prefix="tgsend") as pool:
            list(pool.map(_send_one, targets))

# COMMAND HANDLER
def handle_command(update: dict):
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return

    chat      = msg.get("chat", {})
    chat_id   = chat.get("id")
    chat_type = chat.get("type", "")
    chat_name = chat.get("title") or chat.get("username") or str(chat_id)
    text      = (msg.get("text") or "").strip()
    cmd       = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""

    if cmd == "/addbot":
        if chat_type not in ("group", "supergroup"):
            tg_send_msg(chat_id, "⚠️ <b>Perintah ini hanya bisa digunakan di dalam grup.</b>")
            return
        if add_group(chat_id):
            _log("GROUP", f"✅ ditambahkan: {chat_name} ({chat_id})", Fore.GREEN)
            tg_send_msg(chat_id, f"✅ <b>Bot Aktif di {chat_name}</b>")
        else:
            tg_send_msg(chat_id, f"ℹ️ <b>{chat_name}</b> sudah terdaftar.")

    elif cmd == "/removebot":
        if chat_type not in ("group", "supergroup"):
            return
        if chat_id == DEFAULT_TARGET:
            tg_send_msg(chat_id, "⛔ Grup utama tidak bisa dihapus.")
            return
        if remove_group(chat_id):
            _log("GROUP", f"🗑️ dihapus: {chat_name} ({chat_id})", Fore.YELLOW)
            tg_send_msg(chat_id, f"🗑️ <b>{chat_name}</b> telah dihapus.")

    elif cmd == "/listbot":
        groups = list_groups()
        lines  = [f"  {i+1}. <code>{gid}</code>" for i, gid in enumerate(groups)]
        tg_send_msg(chat_id, f"👥 <b>DAFTAR GRUP AKTIF</b>\n\n" + "\n".join(lines))

def tg_update_listener():
    offset  = 0
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    _log("CMD", "update listener aktif", Fore.CYAN)
    while True:
        try:
            resp = _tg_session.post(
                api_url,
                json={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                timeout=40,
            )
            data = resp.json()
            if not data.get("ok"):
                time.sleep(5)
                continue
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    handle_command(upd)
                except Exception as e:
                    _log("CMD", f"handle error: {e}", Fore.YELLOW)
        except requests.exceptions.Timeout:
            pass
        except Exception as e:
            _log("CMD", f"listener error: {e}", Fore.YELLOW)
            time.sleep(5)

# POLL ONE ACCOUNT
_OTP_RE = re.compile(r"\b\d{3}[- ]?\d{3}\b")

def poll_one(acc) -> bool:
    found  = False
    ranges = []
    try:
        ranges = get_ranges_cached(acc)
    except Exception as e:
        _log("RANGE", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
        return False

    def process_number(rng, num, fallback_country, code):
        full_num = normalize_number(num, code)
        if not full_num.isdigit():
            return False
        try:
            sms_list = get_sms(acc, rng, num)
        except Exception as e:
            _log("SMS", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
            return False

        local_found = False
        for sms in sms_list:
            clean = re.sub(r"\s+", " ", sms.replace("<#>", "")).strip()
            uid   = hashlib.md5(f"{num}-{clean}".encode()).hexdigest()

            matches = _OTP_RE.findall(sms)
            if not matches:
                continue

            if not cache_try_add(uid):
                continue

            otp                        = re.sub(r"[^0-9]", "", matches[0])
            svc                        = detect_service(sms)
            country, flag, region_code = detect_country_and_flag(full_num, fallback_country)

            msg = build_otp_message(otp, svc, flag, country, region_code, full_num, sms)
            tg_send_otp(otp, msg)

            _, last4 = garage_mask_phone(full_num)
            lang     = detect_sms_language(sms)
            _log(
                "OTP",
                f"{svc['icon']} {svc['code']:<3}  {flag} #{region_code}  "
                f"+{full_num[:4]}🗿{last4}  →  {otp}  #{lang}",
                Fore.GREEN,
            )
            local_found = True

        return local_found

    for rng in ranges:
        fallback_country, code = parse_range(rng)
        try:
            numbers = get_numbers(acc, rng)
        except Exception as e:
            _log("NUM", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
            continue
        if not numbers:
            continue

        for n in numbers:
            try:
                if process_number(rng, n, fallback_country, code):
                    found = True
            except Exception as e:
                _log("NUM", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
            time.sleep(0.3)

    return found

# ACCOUNT WORKER
def account_worker(acc):
    sleep_time = MIN_IDLE_SLEEP
    while True:
        try:
            found = poll_one(acc)
            if found:
                sleep_time = MIN_IDLE_SLEEP
            else:
                sleep_time = min(sleep_time + 0.5, POLL_INTERVAL_MAX)
        except Exception as e:
            _log("WORKER", f"akun #{acc['idx']}: {e}", Fore.RED)
            sleep_time = min(sleep_time * 2, POLL_INTERVAL_MAX)

        time.sleep(max(sleep_time, MIN_IDLE_SLEEP))

# KEEPALIVE
_last_keepalive       = {}
_session_expired_sent = {}

def keepalive_worker(accounts):
    _log("KEEPALIVE", f"aktif — ping tiap {KEEPALIVE_INTERVAL}s per akun", Fore.CYAN)
    while True:
        now = time.time()
        for acc in accounts:
            idx = acc["idx"]
            if now - _last_keepalive.get(idx, 0) < KEEPALIVE_INTERVAL:
                continue

            session_ok = False
            for _ in range(len(WORKER_POOL)):
                base = get_base()
                try:
                    r = acc["session"].get(f"{base}/portal", timeout=15)
                    if is_worker_blocked(r):
                        mark_worker_limited(base)
                        continue
                    if r.status_code == 200 and "/login" not in str(r.url):
                        _recv_csrf_cache.pop(idx, None)
                        _log("KA-OK", f"akun #{idx} — session aktif ✓", Fore.GREEN)
                        _session_expired_sent[idx] = False
                        session_ok = True
                        break
                    if "/login" in str(r.url):
                        _log("KEEPALIVE", f"akun #{idx}: session expired, coba auto-login...", Fore.YELLOW)
                        login_ok = auto_login_ivas(acc)
                        if login_ok:
                            _recv_csrf_cache.pop(idx, None)
                            _log("KA-OK", f"akun #{idx}: auto-login berhasil ✓", Fore.GREEN)
                            _session_expired_sent[idx] = False
                            session_ok = True
                        break
                except Exception as e:
                    _log("KA-ERR", f"{base}: {e}", Fore.YELLOW)
                    mark_worker_limited(base)

            if not session_ok:
                _log("KA-WARN", f"akun #{idx} — session tidak bisa dipulihkan.", Fore.YELLOW)
                already_sent = _session_expired_sent.get(idx, False)
                if not already_sent and OWNER_ID and OWNER_ID != DEFAULT_TARGET:
                    try:
                        _tg_post(OWNER_ID, f"⚠️ <b>SESSION EXPIRED — Auto-Login Gagal Akun #{idx}</b>")
                        _session_expired_sent[idx] = True
                    except Exception as e:
                        _log("KA-ERR", f"gagal kirim notif Telegram: {e}", Fore.RED)

            _last_keepalive[idx] = now
            time.sleep(2)
        time.sleep(60)

# HTTP HEALTH SERVER
_bot_start_time = time.time()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path in ("", "/", "/health"):
            self._respond(200, "text/plain", b"OK")
        elif path == "/status":
            up   = int(time.time() - _bot_start_time)
            body = json.dumps({
                "status":         "running",
                "uptime_seconds": up,
                "targets":        list_groups(),
            }).encode()
            self._respond(200, "application/json", body)
        else:
            self._respond(404, "text/plain", b"Not found")

    def _respond(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    _log("SERVER", f"port {port}  |  /health  /status", Fore.CYAN)
    server.serve_forever()

# GRACEFUL SHUTDOWN
def _shutdown(signum, frame):
    _log("SHUTDOWN", "menyimpan cache & keluar...", Fore.YELLOW)
    with _sent_cache_lock:
        save_sent_cache_now(sent_cache)
    _save_groups()
    sys.exit(0)

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)

# MAIN
def main():
    global _cache_dirty, _last_cache_save

    print(Fore.CYAN + Style.BRIGHT, end="")
    print("  ╔══════════════════════════════════════╗")
    print("  ║   🕷  SPIDERMAT OTP BOT              ║")
    print("  ║        FORWARD MODE  v2.0            ║")
    print("  ╚══════════════════════════════════════╝")
    print(Style.RESET_ALL)

    hs_thread = threading.Thread(target=run_health_server, daemon=True, name="health")
    hs_thread.start()
    time.sleep(0.3)
    _log("SERVER", "Health server aktif — Railway healthcheck siap", Fore.GREEN)

    if not BOT_TOKEN:
        _log("FATAL", "BOT_TOKEN belum diset!", Fore.RED)
        while True:
            time.sleep(60)

    if IVAS_USERNAME and IVAS_PASSWORD:
        _log("LOGIN", f"Auto-login aktif: {IVAS_USERNAME}", Fore.GREEN)
    else:
        _log("LOGIN", "IVAS_USERNAME/IVAS_PASSWORD belum diset", Fore.YELLOW)

    _load_groups()

    cookies_list = load_cookies()
    if not cookies_list:
        _log("FATAL", f"Tidak ada cookie valid di {COOKIE_FILE}.", Fore.RED)
        while True:
            time.sleep(60)

    accounts = []
    for idx, ck in enumerate(cookies_list):
        acc = {
            "idx":        idx,
            "cookies":    ck,
            "session":    make_session(ck),
            "csrf_token": "",
        }
        accounts.append(acc)
        _log("COOKIE", f"Akun #{idx} — {len(ck)} cookie dimuat", Fore.GREEN)

    print()
    _log("CONFIG", f"Default target   →  {DEFAULT_TARGET}",              Fore.CYAN)
    _log("CONFIG", f"Total target     →  {len(list_groups())} grup",     Fore.CYAN)
    _log("CONFIG", f"Poll interval    →  max {POLL_INTERVAL_MAX}s",      Fore.CYAN)
    _log("CONFIG", f"Min idle sleep   →  {MIN_IDLE_SLEEP}s",             Fore.CYAN)
    print()

    threading.Thread(target=tg_update_listener,                 daemon=True, name="cmd-listener").start()
    threading.Thread(target=keepalive_worker, args=(accounts,), daemon=True, name="keepalive").start()

    for acc in accounts:
        threading.Thread(
            target=account_worker, args=(acc,),
            daemon=True, name=f"poll-{acc['idx']}",
        ).start()
        _log("THREAD+", f"Akun #{acc['idx']} — polling aktif", Fore.GREEN)

    print()
    _log("CONFIG", "Bot berjalan.", Fore.CYAN)

    while True:
        if _cache_dirty and time.time() - _last_cache_save >= 5:
            with _sent_cache_lock:
                save_sent_cache_now(sent_cache)
            _last_cache_save = time.time()
            _cache_dirty     = False
        time.sleep(5)

main()
