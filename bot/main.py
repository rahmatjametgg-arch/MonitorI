"""
SPIDERMAT OTP BOT — FORWARD MODE
Clean UI & Anti Rate-Limit Log Spam
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

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

# ----------------------------
# CONFIGURATION
# ----------------------------
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
IVAS_USERNAME = os.getenv("IVAS_USERNAME", "")
IVAS_PASSWORD = os.getenv("IVAS_PASSWORD", "")
DEFAULT_TARGET = -1003686221386

CHANNEL_LINK  = "https://t.me/matchaappp"
COOKIE_FILE   = "cookie.json"

MIN_IDLE_SLEEP = 2.0
WORKER_LIMIT_COOLDOWN = 900

# ----------------------------
# WORKER POOL
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
        _log("COOKIE", f"error load: {e}", Fore.RED)
    return []

def make_session(cookies: dict):
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://ivasms.com",
        "Referer": "https://ivasms.com/",
    }
    s = httpx.Client(follow_redirects=True, timeout=25, headers=hdrs)
    s.cookies.update(cookies)
    return s

_login_lock = {}
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
    except Exception as e:
        pass
    return ""

def auto_login_ivas(acc) -> bool:
    idx = acc["idx"]
    if not IVAS_USERNAME or not IVAS_PASSWORD:
        return False

    if idx not in _login_lock:
        _login_lock[idx] = threading.Lock()
    if not _login_lock[idx].acquire(blocking=False):
        _login_lock[idx].acquire()
        _login_lock[idx].release()
        return _login_result.get(idx, False)

    try:
        _log("LOGIN", f"Akun #{idx}: mencoba auto-login...", Fore.YELLOW)
        base = get_base()
        csrf = _get_login_csrf(acc["session"], base)
        if not csrf:
            _login_result[idx] = False
            return False

        payload = {"_token": csrf, "email": IVAS_USERNAME, "password": IVAS_PASSWORD}
        r = acc["session"].post(
            f"{base}/login",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
        success = r.status_code == 200 and "/login" not in str(r.url)
        if success:
            _log("LOGIN", f"Akun #{idx}: ✅ Auto-login BERHASIL", Fore.GREEN)
        _login_result[idx] = success
        return success
    except Exception as e:
        _login_result[idx] = False
        return False
    finally:
        _login_lock[idx].release()

# ----------------------------
# IVAS API FETCHERS
# ----------------------------
_recv_csrf_cache = {}

def get_recv_csrf(acc, _retry=0) -> str:
    idx = acc["idx"]
    now = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < 900:
        return cached["csrf"]
    base = get_base()
    try:
        r = acc["session"].get(f"{base}/portal/sms/received", timeout=15)
        if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
            mark_worker_limited(base)
            return get_recv_csrf(acc, _retry + 1)
        if "/login" in str(r.url):
            if auto_login_ivas(acc):
                return get_recv_csrf(acc, _retry)
            return acc.get("csrf_token", "")
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        csrf = meta.get("content", "") if meta else ""
        if csrf:
            acc["csrf_token"] = csrf
            _recv_csrf_cache[idx] = {"csrf": csrf, "ts": now}
            return csrf
    except Exception as e:
        pass
    return acc.get("csrf_token", "")

def _recv_headers(base):
    return {
        "Accept": "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": f"{base}/portal/sms/received",
        "Origin": "https://ivasms.com",
    }

def get_ranges(acc, _retry=0):
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms",
            data={"_token": csrf, "from": today, "to": today},
            headers=_recv_headers(base),
        )
        if is_worker_blocked(r):
            mark_worker_limited(base)
            if _all_workers_limited() or _retry >= len(WORKER_POOL) - 1:
                return []
            time.sleep(2)
            return get_ranges(acc, _retry + 1)
        
        soup = BeautifulSoup(r.text, "html.parser")
        ranges = []
        for div in soup.find_all("div", onclick=True):
            if "toggleRange" in div["onclick"]:
                try:
                    ranges.append(div["onclick"].split("'")[1])
                except:
                    pass
        return list(set(ranges))
    except:
        return []

def get_numbers(acc, rng):
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number",
            data={"_token": csrf, "start": today, "end": today, "range": rng},
            headers=_recv_headers(base),
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
            except:
                pass
        return list(set(numbers))
    except:
        return []

def get_sms(acc, rng, number):
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = get_recv_csrf(acc)
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number/sms",
            data={"_token": csrf, "start": today, "end": today, "Number": number, "Range": rng},
            headers=_recv_headers(base),
        )
        if is_worker_blocked(r):
            mark_worker_limited(base)
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        sms_texts = []
        for t in soup.stripped_strings:
            t = t.strip().replace("<#>", "").strip()
            if re.fullmatch(r"[A-Za-z0-9]{10,}", t) or "No SMS Found" in t:
                continue
            if t and not any(x in t.lower() for x in ["sender", "revenue", "time"]):
                sms_texts.append(t)
        return list(dict.fromkeys(sms_texts))
    except:
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
    except:
        pass

    return "🌐", cleaned[:3]

def mask_phone(phone_str):
    cleaned = re.sub(r"\D", "", phone_str)
    if len(cleaned) <= 7:
        return cleaned
    
    prefix = cleaned[:4]
    suffix = cleaned[-4:]
    return f"+{prefix}🗿{suffix}"

def send_telegram(phone, sms_text):
    if not BOT_TOKEN:
        return

    # Abaikan teks bawaan IVAS/header jam
    sms_clean = sms_text.strip()
    if any(x in sms_clean.lower() for x in ["sender", "revenue", "time"]):
        return

    # 1. Cari OTP format XXX-XXX atau 6 digit
    m = re.search(r"\b\d{3}[-\s]?\d{3}\b", sms_clean)
    otp = None
    
    if m:
        otp = m.group(0)
    else:
        # 2. Jika tidak ada, cari angka 4-8 digit TAPI BUKAN 0120
        candidates = re.findall(r"\b\d{4,8}\b", sms_clean)
        for c in candidates:
            if c != "0120":
                otp = c
                break

    # Kalo tetep bernilai 0120 / None -> STOP!
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
        _log("OTP", f"Terkirim Telegram: {masked_num} -> {otp}", Fore.GREEN)
    except Exception as e:
        _log("TG-ERR", f"Gagal kirim: {e}", Fore.RED)

# ----------------------------
# POLLING SYSTEM & WORKERS
# ----------------------------
_sent_cache = set()

def poll_one(acc):
    ranges = get_ranges(acc)
    if not ranges:
        return False

    found_any = False
    for rng in ranges:
        numbers = get_numbers(acc, rng)
        time.sleep(1.5)  # Delay aman agar worker gak kena rate-limit
        
        for num in numbers:
            sms_list = get_sms(acc, rng, num)
            for sms in sms_list:
                cache_key = f"{num}:{sms.strip()}"
                if cache_key not in _sent_cache:
                    _sent_cache.add(cache_key)
                    send_telegram(num, sms)
                    found_any = True
            time.sleep(1.5)  # Jeda ambil SMS per nomor
    return found_any

def account_worker(acc):
    sleep_time = 5.0  # Jeda awal polling dibuat 5 detik
    while True:
        try:
            if _all_workers_limited():
                _log("WORKER", "Semua worker kena rate-limit, istirahat 60s...", Fore.YELLOW)
                time.sleep(60)
                continue

            found = poll_one(acc)
            sleep_time = 3.0 if found else 8.0  # Jeda polling aman
        except Exception as e:
            sleep_time = 10.0

        time.sleep(sleep_time)
        

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
    
