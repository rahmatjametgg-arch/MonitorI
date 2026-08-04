"""
SPIDERMAT OTP BOT — FAST & ANTI-RATE LIMIT
Focus: Real-time OTP Forwarder without spamming endpoints
"""

import httpx
from bs4 import BeautifulSoup
import re
from datetime import datetime
import time
import threading
import json
import os
import sys
import requests
import phonenumbers
from phonenumbers import geocoder
from colorama import init, Fore, Style
from http.server import HTTPServer, BaseHTTPRequestHandler

# ----------------------------
# BASIC SETUP
# ----------------------------
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
IVAS_USERNAME = os.getenv("IVAS_USERNAME", "")
IVAS_PASSWORD = os.getenv("IVAS_PASSWORD", "")
DEFAULT_TARGET = -1003686221386

CHANNEL_LINK  = "https://t.me/matchaappp"
COOKIE_FILE   = "cookie.json"
WORKER_LIMIT_COOLDOWN = 300  # Cooldown disingkat jadi 5 menit

# ----------------------------
# WORKER POOL & LOGGING
# ----------------------------
WORKER_POOL = [
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

_worker_lock          = threading.Lock()
_active_worker_idx    = 0
_worker_limited_until = {}

def _log(tag, msg, color=Fore.CYAN):
    ts = datetime.now().strftime("%H:%M:%S")
    print(color + f"  {ts}  [{tag:<8}]  {msg}" + Style.RESET_ALL, flush=True)

def get_base():
    with _worker_lock:
        return WORKER_POOL[_active_worker_idx % len(WORKER_POOL)]

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
        _log("WORKER", f"Rate-limit! Pindah ke: {get_base()}", Fore.YELLOW)

_RATE_LIMIT_MARKERS = (
    "temporarily rate limited", "error 1027", "please check back later",
    "has been rate limited", "error 1015", "you have been blocked",
    "attention required", "error 1020", "checking your browser", "just a moment",
)

def is_worker_blocked(resp) -> bool:
    if resp is None:
        return False
    try:
        if resp.status_code in (429, 403, 503):
            return True
        sample = resp.text[:2000].lower()
        return any(m in sample for m in _RATE_LIMIT_MARKERS)
    except Exception:
        return False

# ----------------------------
# SESSION & IVAS AUTH
# ----------------------------
def load_cookies():
    if not os.path.exists(COOKIE_FILE):
        return []
    try:
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            if all(isinstance(x, dict) and "name" in x and "value" in x for x in data):
                return [{x["name"]: x["value"] for x in data}]
            return data
        if isinstance(data, dict):
            return [data]
    except Exception as e:
        _log("COOKIE", f"Error load cookie: {e}", Fore.RED)
    return []

def make_session(cookies: dict):
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://ivasms.com",
        "Referer": "https://ivasms.com/",
    }
    s = httpx.Client(follow_redirects=True, timeout=15, headers=hdrs)
    s.cookies.update(cookies)
    return s

def _recv_headers(base):
    return {
        "Accept": "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": f"{base}/portal/sms/received",
        "Origin": "https://ivasms.com",
    }

# ----------------------------
# IVAS FAST FETCHERS
# ----------------------------
_recv_csrf_cache = {}

def get_recv_csrf(acc) -> str:
    idx = acc["idx"]
    now = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < 600:
        return cached["csrf"]
    
    base = get_base()
    try:
        r = acc["session"].get(f"{base}/portal/sms/received", timeout=10)
        if is_worker_blocked(r):
            mark_worker_limited(base)
            return acc.get("csrf_token", "")
            
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        csrf = meta.get("content", "") if meta else ""
        
        if csrf:
            acc["csrf_token"] = csrf
            _recv_csrf_cache[idx] = {"csrf": csrf, "ts": now}
            return csrf
    except Exception:
        pass
    return acc.get("csrf_token", "")

def fetch_active_ranges(acc):
    """Ambil range aktif dengan penanganan Silent Block yang lebih agresif"""
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = get_recv_csrf(acc)
    
    if not csrf:
        # Jika CSRF gagal diambil, kemungkinan worker lama terblokir. Langsung rotasi!
        mark_worker_limited(base)
        return []
    
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms",
            data={"_token": csrf, "from": today, "to": today},
            headers=_recv_headers(base),
            timeout=7
        )
        
        # Cek apakah response diblokir secara tersembunyi (Cloudflare / Empty response)
        if is_worker_blocked(r) or "login" in r.url.path or len(r.text) < 500:
            mark_worker_limited(base)
            return []
            
        soup = BeautifulSoup(r.text, "html.parser")
        ranges = []
        for div in soup.find_all("div", onclick=True):
            if "toggleRange" in div.get("onclick", ""):
                try:
                    ranges.append(div["onclick"].split("'")[1])
                except Exception:
                    pass
                    
        # Jika respon 200 OK tapi range tidak ditemukan sama sekali padahal respon pendek -> paksa ganti worker
        if not ranges and "getsms" not in r.text:
            mark_worker_limited(base)
            
        return list(set(ranges))
    except Exception as e:
        mark_worker_limited(base)
        return []
        

def fetch_sms_for_range(acc, rng):
    """Langsung scan SMS untuk range aktif"""
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = get_recv_csrf(acc)
    
    # Ambil nomor di range ini
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number",
            data={"_token": csrf, "start": today, "end": today, "range": rng},
            headers=_recv_headers(base),
            timeout=10
        )
        if is_worker_blocked(r):
            mark_worker_limited(base)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        numbers = []
        for div in soup.find_all("div", onclick=True):
            try:
                val = div["onclick"].split("'")[1]
                if val and val != rng:
                    numbers.append(val)
            except Exception:
                pass

        results = []
        # Tarik SMS hanya dari nomor yang ada di range aktif
        for num in numbers:
            r_sms = acc["session"].post(
                f"{base}/portal/sms/received/getsms/number/sms",
                data={"_token": csrf, "start": today, "end": today, "Number": num, "Range": rng},
                headers=_recv_headers(base),
                timeout=10
            )
            if is_worker_blocked(r_sms):
                mark_worker_limited(base)
                continue

            soup_sms = BeautifulSoup(r_sms.text, "html.parser")
            for t in soup_sms.stripped_strings:
                t = t.strip().replace("<#>", "").strip()
                if re.fullmatch(r"[A-Za-z0-9]{10,}", t) or "No SMS Found" in t:
                    continue
                if t and not any(x in t.lower() for x in ["sender", "revenue", "time"]):
                    results.append((num, t))
            
            time.sleep(0.15) # Jeda mikro anti-limit per nomor

        return results
    except Exception:
        return []

# ----------------------------
# TELEGRAM FORWARDER
# ----------------------------
def _get_flag(phone_str):
    cleaned = re.sub(r"\D", "", phone_str)
    if not cleaned:
        return "🌐", "1"
    try:
        parsed = phonenumbers.parse("+" + cleaned, None)
        region = geocoder.region_code_for_number(parsed)
        cc = str(parsed.country_code)
        if region and len(region) == 2:
            flag = chr(ord(region[0]) + 127397) + chr(ord(region[1]) + 127397)
            return flag, cc
    except Exception:
        pass
    return "🌐", cleaned[:3]

def mask_phone(phone_str):
    cleaned = re.sub(r"\D", "", phone_str)
    if len(cleaned) <= 7:
        return cleaned
    return f"+{cleaned[:4]}🗿{cleaned[-4:]}"

def send_telegram(phone, sms_text):
    if not BOT_TOKEN:
        return

    sms_clean = sms_text.strip()
    m = re.search(r"\b\d{3}[-\s]?\d{3}\b", sms_clean)
    otp = m.group(0) if m else None

    if not otp:
        candidates = re.findall(r"\b\d{4,8}\b", sms_clean)
        for c in candidates:
            if c != "0120":
                otp = c
                break

    if not otp or otp == "0120":
        return

    flag, _ = _get_flag(phone)
    masked_num = mask_phone(phone)

    caption = f"{flag} WS {masked_num} #ID"
    keyboard = {
        "inline_keyboard": [
            [{"text": f"📋 {otp}", "copy_text": {"text": otp}}],
            [{"text": "All File ↗", "url": CHANNEL_LINK}]
        ]
    }

    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": DEFAULT_TARGET,
                "text": caption,
                "parse_mode": "HTML",
                "reply_markup": json.dumps(keyboard),
            },
            timeout=10,
        )
        _log("OTP", f"⚡ OTP KETEMU! {masked_num} -> {otp}", Fore.GREEN)
    except Exception as e:
        _log("TG-ERR", f"Gagal forward ke TG: {e}", Fore.RED)

# ----------------------------
# POLLING ENGINE (SMART FAST-LANE)
# ----------------------------
_sent_cache = set()

def account_worker(acc):
    _log("WORKER", "⚡ FAST-LANE Engine Aktif! Siap narik OTP...", Fore.GREEN)
    
    while True:
        try:
            # 1. Tarik HANYA range yang ada traffic SMS hari ini
            active_ranges = fetch_active_ranges(acc)
            
            if not active_ranges:
                _log("POLL", "Belum ada SMS baru masuk hari ini. Menunggu...", Fore.CYAN)
                time.sleep(2.5)  # Jeda aman kalau lagi sepi
                continue

            found_new = False
            for rng in active_ranges:
                sms_items = fetch_sms_for_range(acc, rng)
                for num, sms in sms_items:
                    cache_key = f"{num}:{sms.strip()}"
                    if cache_key not in _sent_cache:
                        _sent_cache.add(cache_key)
                        send_telegram(num, sms)
                        found_new = True

            # Jika dapet OTP baru, langsung hajar poll lagi tanpa delay.
            # Kalau sepi, kasih delay 1.5 - 2 detik biar bebas dari 429.
            sleep_time = 0.5 if found_new else 2.0
            time.sleep(sleep_time)

        except Exception as e:
            _log("WORKER-ERR", f"Error loop: {e}", Fore.RED)
            time.sleep(3.0)

# ----------------------------
# RAILWAY HEALTHCHECK DUMMY SERVER
# ----------------------------
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    server.serve_forever()

# ----------------------------
# MAIN ENTRY
# ----------------------------
def main():
    _log("SYSTEM", "Memulai Bot...", Fore.GREEN)
    threading.Thread(target=run_health_server, daemon=True).start()

    cookies_list = load_cookies()
    if not cookies_list:
        cookies_list = [{}]

    accounts = []
    for idx, c in enumerate(cookies_list, 1):
        s = make_session(c)
        acc = {"idx": idx, "session": s, "csrf_token": ""}
        accounts.append(acc)

    for acc in accounts:
        threading.Thread(target=account_worker, args=(acc,), daemon=True).start()

    _log("SYSTEM", f"{len(accounts)} Worker Berhasil Dijalankan!", Fore.GREEN)

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
    
