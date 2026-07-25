"""
SPIDERMAT OTP BOT — FORWARD MODE
Baca cookie dari cookie.json → poll IVAS → forward OTP ke Telegram.
Tidak ada login, tidak ada session management, tidak ada command kompleks.
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

# ── CONFIG (dari environment variable) ──────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
OWNER_ID     = int(os.getenv("OWNER_ID", "0"))
# FORWARD_TO: chat_id tujuan forward OTP. Default ke OWNER_ID.
# Bisa diisi ID grup/channel, misalnya: -1001234567890
FORWARD_TO   = os.getenv("FORWARD_TO", str(OWNER_ID))
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "https://t.me/your_channel")
NUMBER_LINK  = os.getenv("NUMBER_LINK",  "https://t.me/your_channel")

COOKIE_FILE        = "cookie.json"
CACHE_FILE         = "file/sent_cache.json"
MAX_CACHE          = 2000
POLL_INTERVAL_MAX  = 3.0   # detik — jeda maks saat tidak ada OTP
KEEPALIVE_INTERVAL = 480   # detik — ping /portal tiap 8 menit agar session tidak mati

# ── LOGGING ──────────────────────────────────────────────────────────────────
def _log(tag, msg, color=Fore.CYAN):
    print(color + f"  [{tag:<10}] {msg}" + Style.RESET_ALL, flush=True)

# ── WORKER POOL (proxy fallback jika kena rate-limit) ────────────────────────
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

def mark_worker_limited(url):
    global _active_worker_idx
    now = time.time()
    with _worker_lock:
        _worker_limited_until[url] = now + WORKER_LIMIT_COOLDOWN
        for i in range(1, len(WORKER_POOL) + 1):
            idx = (_active_worker_idx + i) % len(WORKER_POOL)
            if _worker_limited_until.get(WORKER_POOL[idx], 0) < now:
                _active_worker_idx = idx
                break
    _log("WORKER", f"rate-limited → pindah ke {get_base()}", Fore.YELLOW)

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

# ── COOKIE LOADING ────────────────────────────────────────────────────────────
def load_cookies():
    """
    Baca cookie.json. Format yang didukung:
      1. {"email@x.com": {"laravel_session": "xxx", ...}}  — multi-akun per email
      2. {"laravel_session": "xxx", ...}                   — flat single akun
    Selalu return list of dict (cookies per akun).
    """
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

# ── HTTPX SESSION ─────────────────────────────────────────────────────────────
def make_session(cookies: dict, timeout=30):
    base = get_base()
    host = base.split("://", 1)[-1]
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
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )
    s.cookies.update(cookies)
    return s

# ── CSRF CACHE (per-akun) ─────────────────────────────────────────────────────
_recv_csrf_cache = {}   # idx -> {"csrf": str, "ts": float}
RECV_CSRF_TTL    = 900  # 15 menit

def get_recv_csrf(acc, _retry=0) -> str:
    """Ambil CSRF token dari /portal/sms/received. Di-cache 15 menit."""
    idx = acc["idx"]
    now = time.time()
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

# ── RANGES CACHE ──────────────────────────────────────────────────────────────
_ranges_cache     = {}   # idx -> (ts, list)
_ranges_429_until = {}   # idx -> ts (non-blocking 429 cooldown)
RANGES_CACHE_TTL  = 300  # 5 menit

def _recv_headers(base):
    return {
        "Accept":           "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          f"{base}/portal/sms/received",
        "Origin":           "https://ivasms.com",
    }

def get_ranges(acc, _retry=0):
    idx = acc["idx"]
    now = time.time()
    # Non-blocking 429 cooldown — gunakan cache lama tanpa hit server
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []
    base  = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf  = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms",
        data={"_token": csrf, "from": today, "to": today},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
        mark_worker_limited(worker_before)
        return get_ranges(acc, _retry + 1)
    if r.status_code == 429:
        _ranges_429_until[idx] = now + 180   # 3 menit cooldown, non-blocking
        entry = _ranges_cache.get(idx)
        _log("RANGE", f"akun #{idx} 429 — cooldown 3 menit, pakai cache", Fore.YELLOW)
        return entry[1] if entry else []
    if "/login" in str(r.url):
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
    base  = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf  = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms/number",
        data={"_token": csrf, "start": today, "end": today, "range": rng},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
        mark_worker_limited(worker_before)
        return get_numbers(acc, rng, _retry + 1)
    if r.status_code == 429 or "/login" in str(r.url):
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
    base  = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf  = get_recv_csrf(acc)
    worker_before = base
    r = acc["session"].post(
        f"{base}/portal/sms/received/getsms/number/sms",
        data={"_token": csrf, "start": today, "end": today, "Number": number, "Range": rng},
        headers=_recv_headers(base),
    )
    if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
        mark_worker_limited(worker_before)
        return get_sms(acc, rng, number, _retry + 1)
    if r.status_code == 429 or "/login" in str(r.url):
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

# ── HELPERS ───────────────────────────────────────────────────────────────────
SERVICE_SHORT = {
    "WHATSAPP": "#WS", "TELEGRAM": "#TG", "GOOGLE": "#G",  "FACEBOOK": "#FB",
    "INSTAGRAM": "#IG", "SHOPEE": "#SP",  "TOKOPEDIA": "#TP", "GRAB": "#GR",
    "GOJEK": "#GJ",  "TIKTOK": "#TT",
}
_SVC_EMOJI = {
    "#WS": "💬", "#TG": "✈️", "#G": "🔍", "#FB": "📘",
    "#IG": "📷", "#TT": "🎵", "#GR": "🚗", "#GJ": "🛵",
    "#SP": "🟠", "#TP": "🛍️",
}

def extract_service_short(text):
    m = re.search(
        r"(WhatsApp|Telegram|Google|Facebook|Instagram|Shopee|Tokopedia|Grab|Gojek|TikTok)",
        text, re.I
    )
    if m:
        return SERVICE_SHORT.get(m.group(1).upper(), "#OT")
    return "#OT"

def parse_range(rng):
    country = re.sub(r"\s*\(.*?\)", "", rng)
    country = re.sub(r"\d+", "", country)
    country = re.sub(r"\s+", " ", country).strip().upper()
    code_match = re.search(r"\((\d+)\)", rng)
    code = code_match.group(1) if code_match else ""
    return country, code

def code_to_flag(code):
    try:
        return "".join(chr(127397 + ord(c)) for c in code.upper())
    except:
        return "🏳"

def detect_country_and_flag(full_num, fallback_country="UNKNOWN"):
    try:
        parsed = phonenumbers.parse("+" + full_num, None)
        region = phonenumbers.region_code_for_number(parsed)
        if region:
            flag = code_to_flag(region)
            country_name = geocoder.description_for_number(parsed, "en")
            return (country_name.upper() if country_name else fallback_country), flag
    except:
        pass
    return fallback_country, "🏳"

def normalize_number(num, country_code):
    num = str(num).strip().replace(" ", "").replace("-", "").replace("+", "")
    if country_code and num.startswith(country_code):
        return num
    if num.startswith("0") and country_code:
        return country_code + num[1:]
    return num

def format_phone_number(number):
    n = str(number).replace("+", "").replace(" ", "")
    if len(n) >= 10:
        return f"{n[:4]}****{n[-4:]}"
    return n

# ── SENT CACHE ────────────────────────────────────────────────────────────────
_sent_cache_lock = threading.Lock()
_cache_dirty     = False
_last_cache_save = 0.0

def load_sent_cache():
    os.makedirs("file", exist_ok=True)
    if not os.path.exists(CACHE_FILE):
        return set()
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except:
        return set()

def save_sent_cache_now(cache):
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

def cache_add(uid):
    global _cache_dirty, _last_cache_save
    with _sent_cache_lock:
        sent_cache.add(uid)
    _cache_dirty = True
    if time.time() - _last_cache_save >= 5:
        with _sent_cache_lock:
            save_sent_cache_now(sent_cache)
        _last_cache_save = time.time()
        _cache_dirty = False

# ── TELEGRAM SEND ─────────────────────────────────────────────────────────────
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=2, pool_maxsize=5, max_retries=0
))

def tg_send_otp(otp, msg_text):
    kb = {
        "inline_keyboard": [
            [{"text": f"» 📋 {otp}", "copy_text": {"text": otp}}],
            [
                {"text": "🏆 Channel ↗", "url": CHANNEL_LINK},
                {"text": "✉️ Number ↗",  "url": NUMBER_LINK},
            ],
        ]
    }
    for attempt in range(3):
        try:
            r = _tg_session.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":      FORWARD_TO,
                    "text":         msg_text,
                    "parse_mode":   "HTML",
                    "reply_markup": kb,
                },
                timeout=10,
            )
            data = r.json()
            if data.get("ok"):
                return
            if r.status_code == 429:
                retry_after = data.get("parameters", {}).get("retry_after", 5)
                time.sleep(retry_after + 1)
                continue
            _log("TG-ERR", data.get("description", "?"), Fore.RED)
            return
        except Exception as e:
            if attempt == 2:
                _log("TG-ERR", str(e), Fore.RED)
            else:
                time.sleep(1.5 ** (attempt + 1))

# ── POLL ONE ACCOUNT ──────────────────────────────────────────────────────────
def poll_one(acc):
    """Ambil semua SMS baru dari satu akun. Return True jika ada OTP terkirim."""
    found  = False
    ranges = []
    try:
        ranges = get_ranges_cached(acc)
    except Exception as e:
        _log("RANGE", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
        return False

    def process_number(rng, num, fallback_country, code):
        local_found = False
        full_num = normalize_number(num, code)
        if not full_num.isdigit():
            return False
        try:
            sms_list = get_sms(acc, rng, num)
        except Exception as e:
            _log("SMS", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
            return False

        for sms in sms_list:
            clean = re.sub(r"\s+", " ", sms.replace("<#>", "")).strip()
            uid   = hashlib.md5(f"{num}-{clean}".encode()).hexdigest()
            with _sent_cache_lock:
                if uid in sent_cache:
                    continue

            # Deteksi OTP — 6 digit (XXXXXX / XXX XXX / XXX-XXX)
            matches = re.findall(r"\b\d{3}[- ]?\d{3}\b", sms)
            if not matches:
                continue

            otp          = re.sub(r"[^0-9]", "", matches[0])
            service      = extract_service_short(sms)
            country, flag = detect_country_and_flag(full_num, fallback_country)

            try:
                _parsed     = phonenumbers.parse("+" + full_num, None)
                region_code = phonenumbers.region_code_for_number(_parsed) or "??"
                last4       = full_num[-4:] if len(full_num) >= 4 else full_num
            except:
                region_code = fallback_country[:2] if fallback_country else "??"
                last4       = full_num[-4:] if len(full_num) >= 4 else full_num

            svc_icon = _SVC_EMOJI.get(service, "💬")
            msg      = f"{flag} <b>{region_code}</b>  {svc_icon} <code>{otp}MAT{last4}</code>"

            tg_send_otp(otp, msg)
            cache_add(uid)
            _log("OTP", f"{flag} {region_code} | {format_phone_number(full_num)} | {otp}", Fore.GREEN)
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
        n_workers = min(20, len(numbers))
        with ThreadPoolExecutor(max_workers=n_workers, thread_name_prefix="sms") as pool:
            futs = {pool.submit(process_number, rng, n, fallback_country, code): n for n in numbers}
            for fut in as_completed(futs):
                try:
                    if fut.result():
                        found = True
                except Exception as e:
                    _log("NUM", f"akun #{acc['idx']}: {e}", Fore.YELLOW)

    return found

# ── ACCOUNT WORKER (polling loop) ─────────────────────────────────────────────
def account_worker(acc):
    sleep_time = 1.0
    while True:
        try:
            found      = poll_one(acc)
            sleep_time = 0.0 if found else min(sleep_time + 0.3, POLL_INTERVAL_MAX)
        except Exception as e:
            _log("WORKER", f"akun #{acc['idx']}: {e}", Fore.RED)
            sleep_time = min(sleep_time * 2, 10.0)
        if sleep_time > 0:
            time.sleep(sleep_time)

# ── KEEPALIVE (ping /portal agar session tidak expire) ───────────────────────
_last_keepalive = {}   # idx -> ts

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
                        _log("KEEPALIVE", f"Worker rate limited ({base}), pindah worker...", Fore.YELLOW)
                        mark_worker_limited(base)
                        continue

                    if r.status_code == 200 and "/login" not in str(r.url):
                        _recv_csrf_cache.pop(idx, None)
                        _log("KA-OK", f"akun #{idx} session aktif", Fore.GREEN)
                        session_ok = True
                        break

                    if "/login" in str(r.url):
                        break

                except Exception as e:
                    _log("KA-ERR", f"{base}: {e}", Fore.YELLOW)
                    mark_worker_limited(base)

            if not session_ok:
                _log("KA-WARN", f"akun #{idx} tidak dapat diverifikasi. Session/login perlu dicek hanya jika semua worker normal namun tetap redirect ke login.", Fore.YELLOW)

            _last_keepalive[idx] = now
            time.sleep(2)
        time.sleep(60)

# ── HTTP HEALTH SERVER (Railway healthcheck) ──────────────────────────────────
_bot_start_time = time.time()

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path in ("", "/", "/health"):
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/status":
            up   = int(time.time() - _bot_start_time)
            body = json.dumps({
                "status":         "running",
                "uptime_seconds": up,
                "uptime":         f"{up // 3600}h {(up % 3600) // 60}m {up % 60}s",
                "forward_to":     FORWARD_TO,
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not found")

    def log_message(self, *args):
        pass  # Nonaktifkan log HTTP agar console bersih

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    HTTPServer.allow_reuse_address = True
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    _log("SERVER", f"port {port} | /health /status", Fore.CYAN)
    server.serve_forever()

# ── GRACEFUL SHUTDOWN ─────────────────────────────────────────────────────────
def _shutdown(signum, frame):
    _log("SHUTDOWN", "menyimpan cache...", Fore.YELLOW)
    with _sent_cache_lock:
        save_sent_cache_now(sent_cache)
    _log("SHUTDOWN", "selesai.", Fore.YELLOW)
    sys.exit(0)

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(Fore.CYAN + Style.BRIGHT)
    print("  ─────────────────────────────────────────────")
    print("   SPIDERMAT OTP BOT  —  FORWARD MODE")
    print("  ─────────────────────────────────────────────")
    print(Style.RESET_ALL)

    if not BOT_TOKEN:
        _log("FATAL", "BOT_TOKEN belum diset!", Fore.RED)
        sys.exit(1)

    cookies_list = load_cookies()
    if not cookies_list:
        _log("FATAL", f"Tidak ada cookie valid di {COOKIE_FILE}. Isi dulu!", Fore.RED)
        sys.exit(1)

    # Buat objek akun — satu session httpx per akun
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

    _log("CONFIG", f"Forward OTP → {FORWARD_TO}", Fore.CYAN)
    _log("CONFIG", f"Worker pool: {len(WORKER_POOL)} proxy", Fore.CYAN)
    _log("CONFIG", f"Keepalive  : tiap {KEEPALIVE_INTERVAL}s", Fore.CYAN)

    # Jalankan thread-thread background
    threading.Thread(target=run_health_server, daemon=True, name="health").start()
    threading.Thread(target=keepalive_worker, args=(accounts,), daemon=True, name="keepalive").start()

    for acc in accounts:
        threading.Thread(
            target=account_worker, args=(acc,),
            daemon=True, name=f"poll-{acc['idx']}"
        ).start()
        _log("THREAD+", f"Akun #{acc['idx']} — polling aktif", Fore.GREEN)

    # Main thread: flush cache secara periodik
    global _cache_dirty, _last_cache_save
    while True:
        if _cache_dirty and time.time() - _last_cache_save >= 5:
            with _sent_cache_lock:
                save_sent_cache_now(sent_cache)
            _last_cache_save = time.time()
            _cache_dirty = False
        time.sleep(5)

main()
