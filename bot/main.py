"""
SPIDERMAT OTP BOT — FORWARD MODE v3.0
Baca cookie dari cookie.json → poll IVAS live endpoint → forward OTP ke Telegram.
Command: /addbot /removebot /listbot

Changelog v3.0:
  - Live endpoint: GET /portal/live/my_sms (1 request/siklus, bukan range→number→SMS)
  - Speed: found→sleep 3s, idle backoff mulai 5s max 20s
  - Time filter: skip OTP > 30 menit (UTC-aware, cegah resend pasca restart)
  - Pesan baru: card style + custom emoji WhatsApp Telegram
  - Session stabil: keepalive, auto-login, rate-limit cooldown 15 menit
"""

import httpx
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone
import time
import threading
import json
import os
import hashlib
import signal
import sys
import queue
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import phonenumbers
from phonenumbers import geocoder
from colorama import init, Fore, Style

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN  = os.getenv("BOT_TOKEN", "")
OWNER_ID   = int(os.getenv("OWNER_ID", "0"))

IVAS_USERNAME = os.getenv("IVAS_USERNAME", "")
IVAS_PASSWORD = os.getenv("IVAS_PASSWORD", "")

DEFAULT_TARGET = -1003686221386

CHANNEL_LINK = "https://t.me/matchaappp"
NUMBER_LINK  = "https://t.me/matchaappp"

COOKIE_FILE  = "cookie.json"
CACHE_FILE   = "file/sent_cache.json"
GROUPS_FILE  = "file/groups.json"
MAX_CACHE    = 2000

# ── Timing ────────────────────────────────────────────────────────────────────
POLL_FOUND_SLEEP   = 3.0    # detik — jeda setelah OTP ditemukan (jangan 0 biar session aman)
MIN_IDLE_SLEEP     = 5.0    # detik — jeda minimum saat tidak ada SMS baru
POLL_INTERVAL_MAX  = 20.0   # detik — jeda maksimum idle

MAX_SMS_AGE_MINUTES = 30    # OTP lebih tua dari ini langsung di-skip
KEEPALIVE_INTERVAL  = 480   # detik — ping /portal tiap 8 menit

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    "POLL":     "🔍",
}

def _log(tag, msg, color=Fore.CYAN):
    icon  = _LOG_ICONS.get(tag, "•")
    ts    = datetime.now().strftime("%H:%M:%S")
    label = f"{icon} {tag:<9}"
    print(color + f"  {ts}  {label}  {msg}" + Style.RESET_ALL, flush=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WORKER POOL  (proxy fallback jika kena rate-limit)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WORKER_POOL = [
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

_worker_lock          = threading.Lock()
_active_worker_idx    = 0
_worker_limited_until = {}
WORKER_LIMIT_COOLDOWN = 900   # 15 menit

def get_base():
    with _worker_lock:
        return WORKER_POOL[_active_worker_idx % len(WORKER_POOL)]

def _all_workers_limited() -> bool:
    now = time.time()
    with _worker_lock:
        return all(_worker_limited_until.get(w, 0) >= now for w in WORKER_POOL)

def mark_worker_limited(url):
    global _active_worker_idx
    now     = time.time()
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
    "has been rate limited",    "error 1015", "you have been blocked",
    "attention required",       "error 1020", "checking your browser", "just a moment",
)

def is_worker_blocked(resp) -> bool:
    if resp is None:
        return False
    try:
        if resp.status_code == 429:
            return True
        sample = resp.text[:2000].lower()
        return any(m in sample for m in _RATE_LIMIT_MARKERS)
    except Exception:
        return False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COOKIE LOADING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTPX SESSION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AUTO-LOGIN IVAS  (anti session expired)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_login_lock   = {}
_login_result = {}

def _get_login_csrf(session, base) -> str:
    try:
        r    = session.get(f"{base}/login", timeout=15)
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
        _log("LOGIN", f"akun #{idx}: IVAS_USERNAME/IVAS_PASSWORD belum diset!", Fore.YELLOW)
        return False

    if idx not in _login_lock:
        _login_lock[idx] = threading.Lock()
    if not _login_lock[idx].acquire(blocking=False):
        _login_lock[idx].acquire()
        _login_lock[idx].release()
        return _login_result.get(idx, False)

    try:
        _log("LOGIN", f"akun #{idx}: auto-login sebagai {IVAS_USERNAME}...", Fore.YELLOW)
        base = get_base()
        csrf = _get_login_csrf(acc["session"], base)
        if not csrf:
            _log("LOGIN", f"akun #{idx}: gagal dapat CSRF login", Fore.RED)
            _login_result[idx] = False
            return False

        r = acc["session"].post(
            f"{base}/login",
            data={"_token": csrf, "email": IVAS_USERNAME, "password": IVAS_PASSWORD},
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
        _log("LOGIN", f"akun #{idx}: exception: {e}", Fore.RED)
        _login_result[idx] = False
        return False
    finally:
        _login_lock[idx].release()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CSRF CACHE  (per-akun, TTL 15 menit)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_recv_csrf_cache = {}
RECV_CSRF_TTL    = 900

def _recv_headers(base):
    return {
        "Accept":           "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          f"{base}/portal/sms/received",
        "Origin":           "https://ivasms.com",
    }

def get_recv_csrf(acc, _retry=0) -> str:
    idx    = acc["idx"]
    now    = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < RECV_CSRF_TTL:
        return cached["csrf"]
    base     = get_base()
    recv_url = f"{base}/portal/sms/received"
    try:
        r = acc["session"].get(recv_url, timeout=15)
        if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
            mark_worker_limited(base)
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
            acc["csrf_token"]     = csrf
            _recv_csrf_cache[idx] = {"csrf": csrf, "ts": now}
            return csrf
    except Exception as e:
        _log("CSRF", f"akun #{idx}: {e}", Fore.YELLOW)
    return acc.get("csrf_token", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TIME FILTER  — skip OTP lama (UTC-aware)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _is_sms_recent(time_str: str) -> bool:
    """
    Return True jika SMS timestamp (HH:MM:SS UTC dari IVAS) <= MAX_SMS_AGE_MINUTES menit lalu.
    Pakai UTC agar tidak salah karena timezone WIB vs UTC.
    Jika tidak bisa parse → anggap baru (True) supaya tidak skip valid.
    """
    try:
        now_utc = datetime.now(timezone.utc)
        h, m, s = map(int, time_str.strip().split(":"))
        sms_utc = now_utc.replace(hour=h, minute=m, second=s, microsecond=0)
        # Kalau timestamp di masa depan (overflow hari) → geser mundur 1 hari
        if (sms_utc - now_utc).total_seconds() > 60:
            from datetime import timedelta
            sms_utc -= timedelta(days=1)
        age_minutes = (now_utc - sms_utc).total_seconds() / 60
        return age_minutes <= MAX_SMS_AGE_MINUTES
    except Exception:
        return True   # tidak bisa parse → anggap valid

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LIVE SMS ENDPOINT  — /portal/live/my_sms (1 request, semua nomor)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _parse_live_response(r, idx: int) -> list:
    """
    Parse response GET /portal/live/my_sms.
    Return: list of (full_number_str, sms_text, time_str_utc)
    Handles: JSON array, JSON {data:[...]}, HTML table.
    """
    results = []

    # ── Coba JSON ──────────────────────────────────────────────────────────────
    try:
        data  = r.json()
        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for k in ("data", "sms", "messages", "result", "items"):
                if k in data and isinstance(data[k], list):
                    items = data[k]
                    break

        for item in items:
            if not isinstance(item, dict):
                continue
            num = str(
                item.get("number") or item.get("phone") or
                item.get("msisdn") or item.get("sender_number") or ""
            ).strip().replace("+", "").replace(" ", "")
            msg = str(
                item.get("message") or item.get("sms") or
                item.get("text")    or item.get("body") or ""
            ).strip()
            raw_ts   = str(item.get("time") or item.get("created_at") or item.get("received_at") or "")
            ts_match = re.search(r"\d{2}:\d{2}:\d{2}", raw_ts)
            ts       = ts_match.group(0) if ts_match else ""
            if msg:
                results.append((num, msg, ts))

        if results:
            return results
    except Exception:
        pass

    # ── HTML table fallback ────────────────────────────────────────────────────
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        SKIP_WORDS = {"sender", "message", "number", "time", "range",
                      "count", "paid", "unpaid", "revenue", "sms found"}
        for row in soup.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue

            number   = ""
            sms_text = ""
            time_str = ""

            for cell in cells:
                c = cell.replace("<#>", "").strip()
                if not c:
                    continue
                if re.fullmatch(r"\d{2}:\d{2}:\d{2}", c):
                    time_str = c
                elif re.fullmatch(r"\+?\d{8,15}", c.replace(" ", "")):
                    number = c.replace(" ", "").lstrip("+")
                elif (len(c.split()) >= 2 or re.search(r"\d{4,}", c)) and \
                     not any(x in c.lower() for x in SKIP_WORDS):
                    sms_text = c

            if sms_text:
                results.append((number, sms_text, time_str))
    except Exception as e:
        _log("SMS", f"akun #{idx}: parse live HTML error: {e}", Fore.RED)

    return results


def get_live_sms(acc) -> list:
    """
    GET /portal/live/my_sms — satu request, semua SMS terbaru.
    Return: list of (full_number, sms_text, time_str_utc)
    Jauh lebih efisien dari pipeline range→number→sms.
    """
    idx  = acc["idx"]
    base = get_base()
    url  = f"{base}/portal/live/my_sms"

    try:
        r = acc["session"].get(url, headers=_recv_headers(base), timeout=15)
    except Exception as e:
        _log("SMS", f"akun #{idx}: get_live_sms error: {e}", Fore.YELLOW)
        return []

    if is_worker_blocked(r):
        mark_worker_limited(base)
        return []
    if r.status_code == 429:
        mark_worker_limited(base)
        return []
    if "/login" in str(r.url):
        _log("WORKER", f"akun #{idx}: live/my_sms → /login, coba auto-login", Fore.YELLOW)
        auto_login_ivas(acc)
        return []

    items = _parse_live_response(r, idx)
    if items:
        _log("POLL", f"akun #{idx}: live endpoint → {len(items)} SMS", Fore.CYAN)
    return items

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PLATFORM / SERVICE DETECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# tg_emoji_id: custom emoji ID Telegram (LV6 group support)
# Fallback ke icon jika ID tidak valid di client.
SERVICE_INFO = {
    "WHATSAPP":  {"icon": "💬",  "name": "WhatsApp",  "code": "WS",  "tg_emoji_id": "5373123633519420602"},
    "TELEGRAM":  {"icon": "✈️",  "name": "Telegram",  "code": "TG",  "tg_emoji_id": ""},
    "GOOGLE":    {"icon": "🔍",  "name": "Google",    "code": "G",   "tg_emoji_id": ""},
    "FACEBOOK":  {"icon": "📘",  "name": "Facebook",  "code": "FB",  "tg_emoji_id": ""},
    "INSTAGRAM": {"icon": "📸",  "name": "Instagram", "code": "IG",  "tg_emoji_id": ""},
    "TIKTOK":    {"icon": "🎵",  "name": "TikTok",    "code": "TT",  "tg_emoji_id": ""},
    "GRAB":      {"icon": "🚗",  "name": "Grab",      "code": "GR",  "tg_emoji_id": ""},
    "GOJEK":     {"icon": "🛵",  "name": "Gojek",     "code": "GJ",  "tg_emoji_id": ""},
    "SHOPEE":    {"icon": "🟠",  "name": "Shopee",    "code": "SP",  "tg_emoji_id": ""},
    "TOKOPEDIA": {"icon": "🛍️", "name": "Tokopedia", "code": "TP",  "tg_emoji_id": ""},
    "PAYPAL":    {"icon": "🅿️",  "name": "PayPal",   "code": "PP",  "tg_emoji_id": ""},
    "TWITTER":   {"icon": "🐦",  "name": "Twitter/X", "code": "TW",  "tg_emoji_id": ""},
    "AMAZON":    {"icon": "📦",  "name": "Amazon",    "code": "AMZ", "tg_emoji_id": ""},
    "NETFLIX":   {"icon": "🎬",  "name": "Netflix",   "code": "NF",  "tg_emoji_id": ""},
    "APPLE":     {"icon": "🍎",  "name": "Apple",     "code": "APL", "tg_emoji_id": ""},
    "MICROSOFT": {"icon": "🪟",  "name": "Microsoft", "code": "MS",  "tg_emoji_id": ""},
    "DISCORD":   {"icon": "🎮",  "name": "Discord",   "code": "DC",  "tg_emoji_id": ""},
    "SNAPCHAT":  {"icon": "👻",  "name": "Snapchat",  "code": "SC",  "tg_emoji_id": ""},
    "LINKEDIN":  {"icon": "💼",  "name": "LinkedIn",  "code": "LI",  "tg_emoji_id": ""},
    "BINANCE":   {"icon": "🪙",  "name": "Binance",   "code": "BNB", "tg_emoji_id": ""},
    "BYBIT":     {"icon": "📊",  "name": "Bybit",     "code": "BB",  "tg_emoji_id": ""},
    "OKX":       {"icon": "💹",  "name": "OKX",       "code": "OKX", "tg_emoji_id": ""},
}
_SVC_DEFAULT = {"icon": "💌", "name": "OTP", "code": "OT", "tg_emoji_id": ""}

_SVC_PATTERN = re.compile(
    r"(WhatsApp|Telegram|Google|Facebook|Instagram|TikTok|Grab|Gojek|Shopee|Tokopedia"
    r"|PayPal|Twitter|Amazon|Netflix|Apple|Microsoft|Discord|Snapchat|LinkedIn"
    r"|Binance|Bybit|OKX)",
    re.IGNORECASE,
)

def detect_service(text: str) -> dict:
    m = _SVC_PATTERN.search(text)
    if m:
        return SERVICE_INFO.get(m.group(1).upper(), _SVC_DEFAULT)
    return _SVC_DEFAULT

def _svc_emoji_tag(svc: dict) -> str:
    """
    Return <tg-emoji> tag kalau ada emoji_id, fallback ke icon biasa.
    Telegram custom emoji — bisa dipakai di grup LV6+.
    """
    eid  = svc.get("tg_emoji_id", "")
    icon = svc.get("icon", "💌")
    if eid:
        return f'<tg-emoji emoji-id="{eid}">{icon}</tg-emoji>'
    return icon

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LANGUAGE DETECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PHONE / COUNTRY HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def code_to_flag(code: str) -> str:
    try:
        return "".join(chr(127397 + ord(c)) for c in code.upper())
    except Exception:
        return "🏳"

def detect_country_and_flag(full_num: str, fallback_country="UNKNOWN"):
    try:
        parsed  = phonenumbers.parse("+" + full_num, None)
        region  = phonenumbers.region_code_for_number(parsed)
        if region:
            flag         = code_to_flag(region)
            country_name = geocoder.description_for_number(parsed, "en")
            return (country_name.upper() if country_name else fallback_country), flag, region
    except Exception:
        pass
    return fallback_country, "🏳", "??"

def normalize_number(num: str, country_code: str) -> str:
    num = str(num).strip().replace(" ", "").replace("-", "").replace("+", "")
    if country_code and num.startswith(country_code):
        return num
    if num.startswith("0") and country_code:
        return country_code + num[1:]
    return num

def garage_mask_phone(full_num: str) -> tuple:
    """'628812340303' → ('+6288', '0303')"""
    n = str(full_num).replace("+", "").replace(" ", "")
    if len(n) >= 8:
        return "+" + n[:4], n[-4:]
    return "+" + n, ""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MESSAGE BUILDER  — Card style dengan custom emoji
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def build_otp_message(
    otp:         str,
    svc:         dict,
    flag:        str,
    country:     str,
    region_code: str,
    full_num:    str,
    sms_text:    str = "",
) -> str:
    """
    Format card OTP — bersih dan cepat dibaca:

        💬 WhatsApp  ·  🇧🇯 Benin
        📱 +2290🗿4704  ·  #BJ  #FR

    OTP muncul di tombol inline keyboard (copy_text hijau).
    Custom emoji WhatsApp via <tg-emoji> untuk grup LV6.
    """
    prefix, last4 = garage_mask_phone(full_num)
    masked        = f"{prefix}🗿{last4}" if last4 else prefix
    lang          = detect_sms_language(sms_text) if sms_text else "EN"
    svc_em        = _svc_emoji_tag(svc)
    svc_name      = svc.get("name", "OTP")

    line1 = f"{svc_em} <b>{svc_name}</b>  ·  {flag} {country.title()}"
    line2 = f"📱 <code>{masked}</code>  ·  #{region_code}  #{lang}"

    return f"{line1}\n{line2}"


def build_otp_keyboard(otp: str, sms_text: str = "") -> dict:
    """
    Keyboard 3-baris:
      Baris 1 (HIJAU, full-width): [📋 OTP] — copy_text
      Baris 2 (BIRU, setengah):    [📱 NUMBER] [🔔 CHANNEL]

    OTP 6 digit: tampil sebagai 738-146
    OTP 8 digit: tampil sebagai 7381-4690
    OTP lain: tampil apa adanya
    """
    n = len(otp)
    if n == 6:
        display = f"{otp[:3]}-{otp[3:]}"
    elif n == 8:
        display = f"{otp[:4]}-{otp[4:]}"
    else:
        display = otp

    return {
        "inline_keyboard": [
            [
                {
                    "text":      f"📋  {display}",
                    "copy_text": {"text": otp},
                }
            ],
            [
                {"text": "📱 NUMBER",  "url": NUMBER_LINK},
                {"text": "🔔 CHANNEL", "url": CHANNEL_LINK},
            ],
        ]
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SENT CACHE  (dedup — OTP tidak dikirim dua kali)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    except Exception:
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
    """Atomic check-and-add. True = baru (boleh kirim), False = duplikat (skip)."""
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GROUP TARGETS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_targets_lock:    threading.Lock = threading.Lock()
_forward_targets: set             = {DEFAULT_TARGET}

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM SEND  (queue-based, non-blocking poll)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=10, max_retries=0,
))
_tg_send_queue: queue.Queue = queue.Queue(maxsize=200)

def _tg_post(chat_id, text, reply_markup=None, retries=3) -> bool:
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
                json=payload, timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                return True
            if r.status_code == 429:
                wait = data.get("parameters", {}).get("retry_after", 5)
                time.sleep(wait + 1)
                continue
            _log("TG-ERR", f"chat {chat_id}: {data.get('description','?')}", Fore.RED)
            return False
        except Exception as e:
            if attempt == retries - 1:
                _log("TG-ERR", f"chat {chat_id}: {e}", Fore.RED)
            else:
                time.sleep(1.5 ** (attempt + 1))
    return False

def tg_send_msg(chat_id: int, text: str):
    _tg_post(chat_id, text)

def tg_send_otp(otp: str, msg_text: str, sms_text: str = ""):
    """Kirim OTP ke semua grup secara paralel."""
    kb      = build_otp_keyboard(otp, sms_text)
    targets = list_groups()

    def _send_one(cid):
        _tg_post(cid, msg_text, reply_markup=kb)

    if len(targets) == 1:
        _send_one(targets[0])
    else:
        with ThreadPoolExecutor(max_workers=min(8, len(targets)), thread_name_prefix="tgsend") as pool:
            list(pool.map(_send_one, targets))

def tg_sender_worker():
    """Thread dedicated yang drain _tg_send_queue → kirim ke Telegram."""
    while True:
        try:
            uid, otp, msg_text, sms_text = _tg_send_queue.get(timeout=2)
        except queue.Empty:
            continue
        try:
            tg_send_otp(otp, msg_text, sms_text)
        except Exception as e:
            _log("TG-ERR", f"sender worker: {e}", Fore.RED)
        finally:
            _tg_send_queue.task_done()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMMAND HANDLER  (/addbot /removebot /listbot)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
            tg_send_msg(chat_id,
                "⚠️ <b>Perintah ini hanya bisa digunakan di dalam grup.</b>\n"
                "Tambahkan bot ke grup, lalu ketik <code>/addbot</code> di grup tersebut.")
            return
        if add_group(chat_id):
            _log("GROUP", f"✅ ditambahkan: {chat_name} ({chat_id})", Fore.GREEN)
            tg_send_msg(chat_id,
                f"╔{'═' * 26}╗\n"
                f"  ✅  <b>BOT AKTIF</b>\n"
                f"╚{'═' * 26}╝\n\n"
                f"🏠  <b>{chat_name}</b>\n"
                f"🆔  <code>{chat_id}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✦  Grup ini sudah terdaftar.\n"
                f"✦  OTP akan diteruskan ke sini secara otomatis.\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<i>Gunakan /removebot untuk menonaktifkan.</i>")
        else:
            tg_send_msg(chat_id,
                f"ℹ️  <b>{chat_name}</b> sudah terdaftar sebelumnya.\n"
                f"Bot sudah aktif di grup ini.")

    elif cmd == "/removebot":
        if chat_type not in ("group", "supergroup"):
            return
        if chat_id == DEFAULT_TARGET:
            tg_send_msg(chat_id, "⛔  Grup utama tidak bisa dihapus dari daftar target.")
            return
        if remove_group(chat_id):
            _log("GROUP", f"🗑️  dihapus: {chat_name} ({chat_id})", Fore.YELLOW)
            tg_send_msg(chat_id,
                f"🗑️  <b>{chat_name}</b> telah dikeluarkan dari daftar penerima OTP.\n"
                f"Ketik /addbot untuk mendaftarkan kembali.")
        else:
            tg_send_msg(chat_id, "ℹ️  Grup ini tidak ada dalam daftar terdaftar.")

    elif cmd == "/listbot":
        groups = list_groups()
        lines  = [f"  {i+1}.  <code>{gid}</code>" for i, gid in enumerate(groups)]
        tg_send_msg(chat_id,
            f"╔{'═' * 26}╗\n"
            f"  👥  <b>DAFTAR GRUP AKTIF</b>\n"
            f"╚{'═' * 26}╝\n\n"
            + "\n".join(lines) +
            f"\n\n<i>Total: {len(groups)} grup terdaftar</i>")

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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FALLBACK PIPELINE  — range → number → SMS  (terbukti kerja, dipakai jika
# live endpoint tidak return data)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_ranges_cache     = {}
_ranges_429_until = {}
RANGES_CACHE_TTL  = 180   # 3 menit (lebih pendek = lebih responsif)

def _get_today():
    return datetime.now().strftime("%Y-%m-%d")

def get_ranges(acc, _retry=0) -> list:
    idx = acc["idx"]
    now = time.time()
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    base  = get_base()
    today = _get_today()
    csrf  = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms",
            data={"_token": csrf, "from": today, "to": today},
            headers=_recv_headers(base),
            timeout=15,
        )
    except Exception as e:
        _log("RANGE", f"akun #{idx}: {e}", Fore.YELLOW)
        return []
    if is_worker_blocked(r):
        mark_worker_limited(base)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            entry = _ranges_cache.get(idx)
            return entry[1] if entry else []
        time.sleep(3 * (_retry + 1))
        return get_ranges(acc, _retry + 1)
    if r.status_code == 429:
        _ranges_429_until[idx] = now + 120
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    if "/login" in str(r.url):
        auto_login_ivas(acc)
        return []
    soup   = BeautifulSoup(r.text, "html.parser")
    ranges = []
    for div in soup.find_all("div", onclick=True):
        if "toggleRange" in div["onclick"]:
            try:
                ranges.append(div["onclick"].split("'")[1])
            except Exception:
                pass
    result = list(set(ranges))
    _ranges_429_until.pop(idx, None)
    if result:
        _ranges_cache[idx] = (now, result)
    return result

def get_ranges_cached(acc) -> list:
    idx  = acc["idx"]
    now  = time.time()
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    entry = _ranges_cache.get(idx)
    if entry and (now - entry[0]) < RANGES_CACHE_TTL:
        return entry[1]
    return get_ranges(acc)

def get_numbers(acc, rng, _retry=0) -> list:
    base  = get_base()
    today = _get_today()
    csrf  = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number",
            data={"_token": csrf, "start": today, "end": today, "range": rng},
            headers=_recv_headers(base),
            timeout=15,
        )
    except Exception as e:
        _log("NUM", f"get_numbers: {e}", Fore.YELLOW)
        return []
    if is_worker_blocked(r):
        mark_worker_limited(base)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            return []
        time.sleep(3 * (_retry + 1))
        return get_numbers(acc, rng, _retry + 1)
    if r.status_code == 429:
        return []
    if "/login" in str(r.url):
        auto_login_ivas(acc)
        return []
    soup    = BeautifulSoup(r.text, "html.parser")
    numbers = []
    for div in soup.find_all("div", onclick=True):
        try:
            val = div["onclick"].split("'")[1]
            if val and val != rng:
                numbers.append(val)
        except Exception:
            pass
    return list(set(numbers))

def get_sms(acc, rng, number, _retry=0) -> list:
    base  = get_base()
    today = _get_today()
    csrf  = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number/sms",
            data={"_token": csrf, "start": today, "end": today, "Number": number, "Range": rng},
            headers=_recv_headers(base),
            timeout=15,
        )
    except Exception as e:
        _log("SMS", f"get_sms: {e}", Fore.YELLOW)
        return []
    if is_worker_blocked(r):
        mark_worker_limited(base)
        if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
            return []
        time.sleep(3 * (_retry + 1))
        return get_sms(acc, rng, number, _retry + 1)
    if r.status_code == 429:
        return []
    if "/login" in str(r.url):
        auto_login_ivas(acc)
        return []
    soup      = BeautifulSoup(r.text, "html.parser")
    sms_texts = []
    try:
        for t in soup.stripped_strings:
            t     = t.strip().replace("<#>", "").strip()
            t_low = t.lower()
            if re.fullmatch(r"[A-Za-z0-9]{10,}", t):
                continue
            if any(x in t_low for x in ["sender", "revenue", "time"]):
                continue
            if re.search(r"\b\d{2}:\d{2}:\d{2}\b", t):
                continue
            if "$" in t:
                continue
            if t and "No SMS Found" not in t:
                sms_texts.append(t)
    except Exception as e:
        _log("SMS", f"parse_sms error: {e}", Fore.RED)
    return list(dict.fromkeys(sms_texts))

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

def _pipeline_to_sms_items(acc) -> list:
    """
    Fallback: range → number → SMS pipeline.
    Return: list of (full_number, sms_text, "")  (timestamp kosong = skip time filter)
    """
    idx    = acc["idx"]
    result = []
    try:
        ranges = get_ranges_cached(acc)
    except Exception as e:
        _log("RANGE", f"akun #{idx}: {e}", Fore.YELLOW)
        return []
    if not ranges:
        return []
    _log("POLL", f"akun #{idx}: fallback pipeline — {len(ranges)} range", Fore.WHITE)
    for rng in ranges:
        if _all_workers_limited():
            break
        fallback_country, code = parse_range(rng)
        try:
            numbers = get_numbers(acc, rng)
        except Exception:
            continue
        if not numbers:
            continue
        for num in numbers:
            if _all_workers_limited():
                break
            full_num = normalize_number(num, code)
            if not full_num.isdigit():
                continue
            try:
                sms_list = get_sms(acc, rng, num)
            except Exception:
                continue
            for sms in sms_list:
                result.append((full_num, sms, ""))  # ts="" → bypass time filter
    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POLL ONE ACCOUNT  — live dulu, fallback pipeline jika perlu
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_OTP_RE = re.compile(
    r"\b\d{3}[- ]?\d{3,5}\b"   # 6–8 digit dengan/tanpa pemisah (WA: 123-456)
    r"|\b\d{8}\b"               # 8 digit persis
)

def poll_one(acc) -> bool:
    """
    Coba live endpoint dulu (1 request, cepat).
    Kalau kosong → fallback ke range→number→SMS pipeline (terbukti kerja).
    Return True jika ada OTP baru di-enqueue.
    """
    idx   = acc["idx"]
    found = False

    # ── Path 1: Live endpoint ─────────────────────────────────────────────────
    try:
        sms_items = get_live_sms(acc)
    except Exception:
        sms_items = []

    # ── Path 2: Fallback pipeline jika live kosong ────────────────────────────
    if not sms_items:
        try:
            sms_items = _pipeline_to_sms_items(acc)
        except Exception as e:
            _log("POLL", f"akun #{idx}: pipeline exception: {e}", Fore.YELLOW)
            return False

    if not sms_items:
        return False

    for full_num, sms, ts in sms_items:
        # Time filter hanya kalau ada timestamp (live endpoint)
        if ts and not _is_sms_recent(ts):
            continue

        clean = re.sub(r"\s+", " ", sms.replace("<#>", "")).strip()
        uid   = hashlib.md5(f"{full_num}:{clean}".encode()).hexdigest()

        matches = _OTP_RE.findall(sms)
        if not matches:
            continue

        if not cache_try_add(uid):
            continue

        otp                        = re.sub(r"[^0-9]", "", matches[0])
        svc                        = detect_service(sms)
        country, flag, region_code = detect_country_and_flag(full_num, "")

        msg = build_otp_message(otp, svc, flag, country, region_code, full_num, sms)

        try:
            _tg_send_queue.put_nowait((uid, otp, msg, sms))
        except queue.Full:
            _log("TG-ERR", f"akun #{idx}: queue penuh — retry nanti", Fore.YELLOW)
            with _sent_cache_lock:
                sent_cache.discard(uid)
            continue

        _, last4 = garage_mask_phone(full_num)
        lang     = detect_sms_language(sms)
        _log(
            "OTP",
            f"{svc['icon']} {svc['code']:<3}  {flag} #{region_code}  "
            f"+{full_num[:4]}🗿{last4}  →  {otp}  #{lang}",
            Fore.GREEN,
        )
        found = True

    return found

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ACCOUNT WORKER  — cepat saat ada OTP, backoff saat idle
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def account_worker(acc):
    sleep_time = MIN_IDLE_SLEEP
    while True:
        try:
            found = poll_one(acc)
            if found:
                # OTP ditemukan → poll cepat lagi setelah jeda kecil
                sleep_time = POLL_FOUND_SLEEP
            else:
                # Idle → backoff bertahap agar tidak spam IVAS
                sleep_time = min(sleep_time + 1.0, POLL_INTERVAL_MAX)
        except Exception as e:
            _log("WORKER", f"akun #{acc['idx']}: {e}", Fore.RED)
            sleep_time = min(sleep_time * 2, POLL_INTERVAL_MAX)

        time.sleep(max(sleep_time, POLL_FOUND_SLEEP))

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KEEPALIVE  — auto-login + notif Telegram 1x jika session expired
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
                        _log("KEEPALIVE", f"akun #{idx}: session expired, auto-login...", Fore.YELLOW)
                        if auto_login_ivas(acc):
                            _recv_csrf_cache.pop(idx, None)
                            _log("KA-OK", f"akun #{idx}: auto-login berhasil ✓", Fore.GREEN)
                            _session_expired_sent[idx] = False
                            session_ok = True
                        break
                except Exception as e:
                    _log("KA-ERR", f"{base}: {e}", Fore.YELLOW)
                    mark_worker_limited(base)

            if not session_ok:
                _log("KA-WARN",
                     f"akun #{idx}: session tidak bisa dipulihkan. "
                     f"Cek IVAS_USERNAME/IVAS_PASSWORD atau perbarui cookie.json.",
                     Fore.YELLOW)
                if not _session_expired_sent.get(idx, False) and OWNER_ID and OWNER_ID != DEFAULT_TARGET:
                    try:
                        _tg_post(OWNER_ID,
                            f"⚠️ <b>SESSION EXPIRED — Auto-Login Gagal</b>\n\n"
                            f"Akun #{idx} tidak bisa akses portal IVAS.\n"
                            f"Solusi:\n"
                            f"• Cek <code>IVAS_USERNAME</code> / <code>IVAS_PASSWORD</code> di env\n"
                            f"• Atau perbarui <code>cookie.json</code> dengan cookie fresh.\n"
                            f"  Lalu restart bot.")
                        _session_expired_sent[idx] = True
                    except Exception as e:
                        _log("KA-ERR", f"gagal kirim notif: {e}", Fore.RED)

            _last_keepalive[idx] = now
            time.sleep(2)
        time.sleep(60)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTP HEALTH SERVER  (Railway healthcheck)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
                "uptime":         f"{up // 3600}h {(up % 3600) // 60}m {up % 60}s",
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GRACEFUL SHUTDOWN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _shutdown(signum, frame):
    _log("SHUTDOWN", "menyimpan cache & keluar...", Fore.YELLOW)
    with _sent_cache_lock:
        save_sent_cache_now(sent_cache)
    _save_groups()
    _log("SHUTDOWN", "selesai.", Fore.YELLOW)
    sys.exit(0)

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    global _cache_dirty, _last_cache_save

    print(Fore.CYAN + Style.BRIGHT, end="")
    print("  ╔══════════════════════════════════════╗")
    print("  ║   🕷  SPIDERMAT OTP BOT  v3.0        ║")
    print("  ║        LIVE ENDPOINT MODE            ║")
    print("  ╚══════════════════════════════════════╝")
    print(Style.RESET_ALL)

    # ── Health server selalu start duluan (Railway healthcheck) ───────────────
    hs_thread = threading.Thread(target=run_health_server, daemon=True, name="health")
    hs_thread.start()
    time.sleep(0.3)
    _log("SERVER", "Health server aktif", Fore.GREEN)

    # ── Validasi env vars ─────────────────────────────────────────────────────
    if not BOT_TOKEN:
        _log("FATAL", "BOT_TOKEN belum diset! Set via environment variable.", Fore.RED)
        while True:
            time.sleep(60)

    if IVAS_USERNAME and IVAS_PASSWORD:
        _log("LOGIN", f"Auto-login aktif: {IVAS_USERNAME}", Fore.GREEN)
    else:
        _log("LOGIN", "IVAS_USERNAME/IVAS_PASSWORD belum diset — auto-login nonaktif", Fore.YELLOW)

    _load_groups()

    # ── Load cookies ──────────────────────────────────────────────────────────
    cookies_list = load_cookies()
    if not cookies_list:
        _log("FATAL", f"Tidak ada cookie valid di {COOKIE_FILE}. Isi dulu!", Fore.RED)
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
    _log("CONFIG", f"Default target   →  {DEFAULT_TARGET}",             Fore.CYAN)
    _log("CONFIG", f"Total target     →  {len(list_groups())} grup",    Fore.CYAN)
    _log("CONFIG", f"Channel link     →  {CHANNEL_LINK}",               Fore.CYAN)
    _log("CONFIG", f"Worker pool      →  {len(WORKER_POOL)} proxy",     Fore.CYAN)
    _log("CONFIG", f"Poll found sleep →  {POLL_FOUND_SLEEP}s",          Fore.CYAN)
    _log("CONFIG", f"Idle sleep range →  {MIN_IDLE_SLEEP}–{POLL_INTERVAL_MAX}s", Fore.CYAN)
    _log("CONFIG", f"Max SMS age      →  {MAX_SMS_AGE_MINUTES} menit",  Fore.CYAN)
    _log("CONFIG", f"Keepalive        →  tiap {KEEPALIVE_INTERVAL}s",   Fore.CYAN)
    print()

    # ── Start threads ─────────────────────────────────────────────────────────
    threading.Thread(target=tg_update_listener,                 daemon=True, name="cmd-listener").start()
    threading.Thread(target=keepalive_worker, args=(accounts,), daemon=True, name="keepalive").start()
    threading.Thread(target=tg_sender_worker,                   daemon=True, name="tg-sender").start()

    for acc in accounts:
        threading.Thread(
            target=account_worker, args=(acc,),
            daemon=True, name=f"poll-{acc['idx']}",
        ).start()
        _log("THREAD+", f"Akun #{acc['idx']} — polling aktif", Fore.GREEN)
        time.sleep(2)   # stagger antar akun

    print()
    _log("CONFIG", "Bot berjalan. Ketik /addbot di grup untuk mendaftarkan.", Fore.CYAN)

    # ── Main thread — flush cache periodik ───────────────────────────────────
    while True:
        if _cache_dirty and time.time() - _last_cache_save >= 5:
            with _sent_cache_lock:
                save_sent_cache_now(sent_cache)
            _last_cache_save = time.time()
            _cache_dirty     = False
        time.sleep(5)

main()
