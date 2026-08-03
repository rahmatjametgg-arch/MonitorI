"""
SPIDERMAT OTP BOT — FORWARD MODE (CLEAN & FIXED)
Fitur: Clean UI Bot PG, Anti Log-Spam Rate Limit, Auto-Login IVAS.
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
    # PARSER & TELEGRAM FORWARDER
def _get_flag(cc_str):
    if not cc_str or not cc_str.isdigit():
        return "🌐"
    try:
        p = phonenumbers.parse("+" + cc_str, None)
        region = geocoder.region_code_for_number(p)
        if region and len(region) == 2:
            return chr(ord(region[0]) + 127397) + chr(ord(region[1]) + 127397)
    except:
        pass
    return "🌐"

def format_caption(phone, sms_text):
    # Ekstrak Kode OTP
    m = re.search(r"\b\d{3}[-\s]?\d{3}\b", sms_text)
    if not m:
        m = re.search(r"\b\d{4,8}\b", sms_text)
    otp = m.group(0) if m else "N/A"

    # Format Flag & Negara
    cleaned = re.sub(r"\D", "", phone)
    cc = cleaned[:3] if len(cleaned) >= 10 else "1"
    flag = _get_flag(cc)

    # UI Telegram Clean
    msg = (
        f"{flag} <b>#{cc} WS +{phone}</b> 🗿 <b>{otp}</b> <b>#ID</b>\n\n"
        f"<b>OTP:</b> <code>{otp}</code>"
    )
    
    keyboard = {
        "inline_keyboard": [
            [{"text": f"📋 {otp}", "copy_text": {"text": otp}}],
            [{"text": "All File ↗", "url": CHANNEL_LINK}]
        ]
    }
    return msg, keyboard

def send_telegram(phone, sms_text):
    if not BOT_TOKEN:
        return
    caption, reply_markup = format_caption(phone, sms_text)
    payload = {
        "chat_id": DEFAULT_TARGET,
        "text": caption,
        "parse_mode": "HTML",
        "reply_markup": json.dumps(reply_markup),
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json=payload,
            timeout=10,
        )
        if r.status_code == 200:
            _log("OTP", f"Terkirim ke Telegram: +{phone} -> {sms_text}", Fore.GREEN)
        else:
            _log("TG-ERR", f"Gagal kirim TG ({r.status_code}): {r.text}", Fore.RED)
    except Exception as e:
        _log("TG-ERR", f"Exception TG: {e}", Fore.RED)

# POLLING SYSTEM
_sent_cache = set()

def poll_one(acc):
    idx = acc["idx"]
    ranges = get_ranges_cached(acc)
    if not ranges:
        return False

    found_any = False
    for rng in ranges:
        numbers = get_numbers(acc, rng)
        for num in numbers:
            sms_list = get_sms(acc, rng, num)
            for sms in sms_list:
                cache_key = f"{num}:{sms}"
                if cache_key not in _sent_cache:
                    _sent_cache.add(cache_key)
                    send_telegram(num, sms)
                    found_any = True
            time.sleep(1.0) # Jeda aman anti 429
    return found_any
    # WORKER THREAD & MAIN LOOP
def account_worker(acc):
    sleep_time = MIN_IDLE_SLEEP
    while True:
        try:
            if _all_workers_limited():
                # Jika semua worker cooldown, pause 60 detik agar tidak spam log
                time.sleep(60)
                continue

            found = poll_one(acc)
            if found:
                sleep_time = MIN_IDLE_SLEEP
            else:
                sleep_time = min(sleep_time + 1.0, 10.0)
        except Exception as e:
            _log("WORKER", f"akun #{acc['idx']}: {e}", Fore.RED)
            sleep_time = 10.0

        time.sleep(sleep_time)

# SIMPLE HTTP SERVER UNTUK KEEP-ALIVE RAILWAY
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running active!")

    def log_message(self, format, *args):
        pass  # Matikan HTTP request log bawaan agar log terminal tetap bersih

def run_http_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), SimpleHTTPRequestHandler)
    _log("SERVER", f"HTTP Server aktif di port {port}", Fore.CYAN)
    server.serve_forever()

# MAIN RUNNER
def main():
    _log("SYSTEM", "Memulai Spidermat OTP Bot...", Fore.GREEN)
    
    # Jalankan HTTP server di background thread
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    # Inisialisasi Akun IVAS
    accounts = []
    for idx, (user, pwd) in enumerate(IVAS_ACCOUNTS, 1):
        sess = requests.Session()
        acc = {"idx": idx, "user": user, "pwd": pwd, "session": sess, "csrf_token": ""}
        if auto_login_ivas(acc):
            accounts.append(acc)

    if not accounts:
        _log("SYSTEM", "Tidak ada akun IVAS yang berhasil login. Bot berhenti.", Fore.RED)
        return

    # Jalankan Worker Polling Per Akun
    for acc in accounts:
        t = threading.Thread(target=account_worker, args=(acc,), daemon=True)
        t.start()
        _log("SYSTEM", f"Worker untuk Akun #{acc['idx']} berhasil dimuat.", Fore.CYAN)

    # Keep Main Thread Alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    
