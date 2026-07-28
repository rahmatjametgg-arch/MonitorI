"""
SPIDERMAT OTP BOT — FORWARD MODE (PATCHED v2.1)
Baca cookie dari cookie.json → poll IVAS → forward OTP ke Telegram.
Command: /addbot /removebot /listbot

Perbaikan v2.0:
  1. Rate-limit fix: POLL_INTERVAL_MAX=120s, min sleep 30s
  2. Auto-login IVAS menggunakan IVAS_USERNAME + IVAS_PASSWORD
  3. Notifikasi SESSION EXPIRED ke Telegram hanya 1x per kegagalan
  4. Format pesan Telegram baru: GARAGE OTP UI

Patch v2.1 (bugfix — lihat AUDIT.md untuk penjelasan lengkap):
  FIX-01: Stagger startup + jitter sleep antar akun
  FIX-02: Worker pool per-akun — _active_worker_idx tidak lagi global
  FIX-03: Recursion fix get_recv_csrf (_retry+1 setelah auto-login)
  FIX-04: TG sender queue — polling tidak diblok Telegram
  FIX-05: cache_try_add SETELAH send berhasil (via pending set + sender worker)
  FIX-06: UID poll include range prefix
  FIX-07: Race condition cache dirty-flag — semua update di dalam lock
  FIX-08: Session Telegram terpisah untuk cmd-listener vs OTP sender
  FIX-09: Timestamp filter _parse_sms_texts diperbaiki (fullmatch, bukan search)
  FIX-10: _OTP_RE diperluas — 4-digit, 6-digit, 8-digit
  FIX-11: _login_lock diinisialisasi di main() sebelum thread apapun start
  FIX-12: Watchdog thread — restart poll thread yang mati/macet
  FIX-13: Keepalive pakai acc["_wlock"] saat baca worker state
  FIX-14: Cookie di-persist ke disk setelah auto-login berhasil
  FIX-15: get_ranges iteratif — tidak ada blocking recursion dengan sleep
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
import queue      # FIX-04: TG sender queue
import random     # FIX-01: jitter sleep
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
BOT_TOKEN  = os.getenv("BOT_TOKEN", "")        # wajib diset via env var
OWNER_ID   = int(os.getenv("OWNER_ID", "0"))

# Kredensial IVAS untuk auto-login (anti session expired)
IVAS_USERNAME = os.getenv("IVAS_USERNAME", "")
IVAS_PASSWORD = os.getenv("IVAS_PASSWORD", "")

# Grup default yang SELALU menerima OTP
DEFAULT_TARGET = -1003686221386

CHANNEL_LINK = "https://t.me/matchaappp"
NUMBER_LINK  = "https://t.me/matchaappp"

COOKIE_FILE        = "cookie.json"
CACHE_FILE         = "file/sent_cache.json"
GROUPS_FILE        = "file/groups.json"
MAX_CACHE          = 2000

POLL_INTERVAL_MAX    = 120.0  # detik — jeda maks saat tidak ada OTP baru
MIN_IDLE_SLEEP       = 30.0   # detik — minimum sleep antar poll
KEEPALIVE_INTERVAL   = 480    # detik — ping /portal tiap 8 menit
WORKER_LIMIT_COOLDOWN = 120   # detik — cooldown per worker setelah kena rate-limit

# FIX-01: jeda antar akun saat startup agar tidak burst bersamaan
WORKER_STAGGER_DELAY = 15.0   # detik — 0s, 15s, 30s, 45s per akun

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
}

def _log(tag, msg, color=Fore.CYAN):
    icon  = _LOG_ICONS.get(tag, "•")
    ts    = datetime.now().strftime("%H:%M:%S")
    label = f"{icon} {tag:<9}"
    print(color + f"  {ts}  {label}  {msg}" + Style.RESET_ALL, flush=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WORKER POOL  (FIX-02: state per-akun, bukan global)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ivasms.com HARUS di index 0 — ini URL yang pasti jalan karena cookie user
# langsung valid di sana. Proxy di bawahnya sebagai fallback jika direct kena limit.
WORKER_POOL = [
    "https://ivasms.com",                                         # ← PRIMARY (direct)
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

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

# ── FIX-02: semua fungsi worker kini menerima acc dan memakai state per-akun ──

def get_base_for(acc) -> str:
    """
    FIX-02: Pilih worker URL terbaik untuk akun INI saja.
    Setiap akun memiliki _widx dan _wlimited sendiri → tidak ada interferensi
    antar akun saat satu akun kena rate-limit.
    """
    with acc["_wlock"]:
        widx = acc["_widx"]
        now  = time.time()
        for i in range(len(WORKER_POOL)):
            idx_try   = (widx + i) % len(WORKER_POOL)
            candidate = WORKER_POOL[idx_try]
            if acc["_wlimited"].get(candidate, 0) < now:
                acc["_widx"] = idx_try
                return candidate
        # semua limited — kembalikan yang paling cepat bebas tanpa update widx
        return min(WORKER_POOL, key=lambda w: acc["_wlimited"].get(w, 0))

def mark_worker_limited_for(acc, url: str):
    """FIX-02: Tandai URL rate-limited untuk akun ini, pindah ke worker berikutnya."""
    now     = time.time()
    switched = False
    with acc["_wlock"]:
        acc["_wlimited"][url] = now + WORKER_LIMIT_COOLDOWN
        for i in range(1, len(WORKER_POOL) + 1):
            idx_try   = (acc["_widx"] + i) % len(WORKER_POOL)
            candidate = WORKER_POOL[idx_try]
            if acc["_wlimited"].get(candidate, 0) < now:
                acc["_widx"] = idx_try
                switched = True
                break
    if switched:
        _log("WORKER", f"akun #{acc['idx']}: rate-limited → pindah ke {get_base_for(acc)}", Fore.YELLOW)
    else:
        _log("WORKER", f"akun #{acc['idx']}: semua worker kena rate-limit — tunggu cooldown", Fore.RED)

def _all_workers_limited_for(acc) -> bool:
    """FIX-02: True jika semua worker akun ini sedang cooldown."""
    now = time.time()
    with acc["_wlock"]:
        return all(acc["_wlimited"].get(w, 0) >= now for w in WORKER_POOL)

def _soonest_worker_free_in_for(acc) -> float:
    """
    FIX-02: Return detik hingga worker pertama akun ini keluar cooldown.
    Return 0.0 jika sudah ada worker yang bebas sekarang.
    """
    now = time.time()
    with acc["_wlock"]:
        free = [w for w in WORKER_POOL if acc["_wlimited"].get(w, 0) < now]
        if free:
            return 0.0
        earliest = min(acc["_wlimited"].get(w, 0) for w in WORKER_POOL)
        return max(0.0, earliest - now + 1.5)   # +1.5s buffer

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COOKIE LOADING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_cookies():
    """
    Format yang didukung:
      1. [{"name":"k","value":"v"}, ...]          — array browser export
      2. {"email": {"laravel_session": "x"}, ...} — multi-akun per email
      3. {"laravel_session": "x", ...}            — flat single akun
    Return: list of cookie-dict (satu dict per akun).
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
# AUTO-LOGIN IVAS  (FIX-11: _login_lock diinisialisasi di main(), FIX-14: persist cookie)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_login_lock   = {}   # idx -> threading.Lock()  — diinisialisasi di main()
_login_result = {}   # idx -> bool

def _get_login_csrf(session, base) -> str:
    """Ambil CSRF token dari halaman /login."""
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

def _persist_cookies(acc):
    """
    FIX-14: Simpan cookie session terbaru ke cookie.json setelah auto-login berhasil.
    Mencegah re-login di setiap restart bot.
    """
    try:
        fresh = dict(acc["session"].cookies)
        if not fresh:
            return
        if not os.path.exists(COOKIE_FILE):
            return
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        idx = acc["idx"]
        if isinstance(raw, list):
            if all(isinstance(x, dict) and "name" in x for x in raw):
                # format browser export: update value per nama cookie
                for item in raw:
                    if item.get("name") in fresh:
                        item["value"] = fresh[item["name"]]
            elif idx < len(raw):
                raw[idx].update(fresh)
        elif isinstance(raw, dict):
            emails = list(raw.keys())
            if all(isinstance(v, dict) for v in raw.values()) and idx < len(emails):
                raw[emails[idx]].update(fresh)
            else:
                raw.update(fresh)
        with open(COOKIE_FILE, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2)
        _log("COOKIE", f"akun #{idx}: cookie diperbarui ke disk", Fore.GREEN)
    except Exception as e:
        _log("COOKIE", f"akun #{acc['idx']}: gagal persist cookie: {e}", Fore.YELLOW)

def auto_login_ivas(acc) -> bool:
    """
    Lakukan HTTP POST ke endpoint login IVAS menggunakan IVAS_USERNAME & IVAS_PASSWORD.
    Perbarui cookie session di memori secara otomatis.
    Return True jika berhasil login, False jika gagal.
    """
    idx = acc["idx"]
    if not IVAS_USERNAME or not IVAS_PASSWORD:
        _log("LOGIN", f"akun #{idx}: IVAS_USERNAME/IVAS_PASSWORD belum diset di env!", Fore.YELLOW)
        return False

    # FIX-11: _login_lock sudah diinisialisasi di main() — tidak ada race di sini
    if not _login_lock[idx].acquire(blocking=False):
        # Thread lain sedang login, tunggu selesai lalu pakai hasilnya
        _login_lock[idx].acquire()
        _login_lock[idx].release()
        return _login_result.get(idx, False)

    try:
        _log("LOGIN", f"akun #{idx}: mencoba auto-login sebagai {IVAS_USERNAME}...", Fore.YELLOW)
        base = get_base_for(acc)   # FIX-02: per-akun
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
            # Invalidate CSRF cache agar diambil ulang dengan session baru
            _recv_csrf_cache.pop(idx, None)
            _persist_cookies(acc)   # FIX-14: simpan cookie ke disk
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CSRF CACHE  (per-akun, TTL 15 menit)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_recv_csrf_cache = {}
RECV_CSRF_TTL    = 900

def get_recv_csrf(acc, _retry=0) -> str:
    idx    = acc["idx"]
    now    = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < RECV_CSRF_TTL:
        return cached["csrf"]
    base     = get_base_for(acc)   # FIX-02: per-akun
    recv_url = f"{base}/portal/sms/received"
    try:
        worker_before = base
        r = acc["session"].get(recv_url, timeout=15)
        if is_worker_blocked(r) and _retry < len(WORKER_POOL) - 1:
            mark_worker_limited_for(acc, worker_before)   # FIX-02
            return get_recv_csrf(acc, _retry + 1)
        if "/login" in str(r.url):
            # FIX-03: increment _retry setelah auto-login agar tidak infinite recursion
            if _retry < 2 and auto_login_ivas(acc):
                return get_recv_csrf(acc, _retry + 1)   # FIX-03: +1 bukan _retry
            _log("CSRF", f"akun #{idx}: tetap di /login setelah auto-login, abort", Fore.RED)
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IVAS API  (ranges / numbers / sms)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_ranges_cache     = {}   # idx -> (ts, list)
_ranges_429_until = {}   # idx -> ts
RANGES_CACHE_TTL  = 300  # 5 menit

def _recv_headers(base):
    return {
        "Accept":           "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          f"{base}/portal/sms/received",
        "Origin":           "https://ivasms.com",
    }

def _check_and_handle_login_redirect(r, acc) -> bool:
    """Return True jika response adalah redirect ke /login (session expired)."""
    if "/login" in str(r.url):
        idx = acc["idx"]
        _log("WORKER", f"akun #{idx}: redirect ke /login, coba auto-login...", Fore.YELLOW)
        auto_login_ivas(acc)
        return True
    return False

def get_ranges(acc):
    """
    FIX-15: Konversi dari rekursi blocking ke iterative loop.
    Menghilangkan time.sleep(4*(attempt+1)) yang bisa memblok 40 detik.
    """
    idx = acc["idx"]
    now = time.time()
    if now < _ranges_429_until.get(idx, 0):
        entry = _ranges_cache.get(idx)
        return entry[1] if entry else []

    for attempt in range(len(WORKER_POOL)):
        base          = get_base_for(acc)   # FIX-02: per-akun
        today         = datetime.now().strftime("%Y-%m-%d")
        csrf          = get_recv_csrf(acc)
        worker_before = base

        try:
            r = acc["session"].post(
                f"{base}/portal/sms/received/getsms",
                data={"_token": csrf, "from": today, "to": today},
                headers=_recv_headers(base),
            )
        except Exception as e:
            _log("RANGE", f"akun #{idx} attempt {attempt}: {e}", Fore.YELLOW)
            break

        if is_worker_blocked(r):
            mark_worker_limited_for(acc, worker_before)   # FIX-02
            if _all_workers_limited_for(acc):             # FIX-02
                _log("RANGE", f"akun #{idx}: semua worker limited — skip poll ini", Fore.RED)
                entry = _ranges_cache.get(idx)
                return entry[1] if entry else []
            if attempt < len(WORKER_POOL) - 1:
                time.sleep(2)   # FIX-15: jeda singkat 2s, bukan 4*(attempt+1)
            continue

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

    # Semua attempt gagal — kembalikan cache lama
    entry = _ranges_cache.get(idx)
    return entry[1] if entry else []

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

def _parse_sms_texts(html_text: str) -> list:
    """
    Ekstrak teks SMS dari HTML response IVAS.
    FIX-09: filter timestamp diperbaiki — hanya skip baris yang SELURUHNYA adalah
    timestamp (fullmatch), bukan baris yang mengandung timestamp.
    SMS OTP sering menyertakan waktu expire di baris yang sama dengan kode.
    """
    soup      = BeautifulSoup(html_text, "html.parser")
    sms_texts = []
    try:
        for t in soup.stripped_strings:
            t = t.strip().replace("<#>", "").strip()
            if not t:
                continue
            if re.fullmatch(r"[A-Za-z0-9]{10,}", t):
                continue
            t_low = t.lower()
            if any(x in t_low for x in ["sender", "revenue", "time"]):
                continue
            # FIX-09: fullmatch bukan search — hanya skip jika SELURUH baris adalah timestamp
            if re.fullmatch(r"\d{2}:\d{2}:\d{2}", t.strip()):
                continue
            if "$" in t:
                continue
            if "No SMS Found" in t:
                continue
            sms_texts.append(t)
    except Exception as e:
        _log("SMS", f"parse error: {e}", Fore.RED)
    return list(dict.fromkeys(sms_texts))


def get_numbers_and_otp(acc, rng, _retry=0) -> dict:
    """
    Satu request ke /getsms/number — ambil daftar nomor DAN teks SMS sekaligus.
    Return: {number: [sms_text, ...]}
    Nomor tanpa SMS → value = []
    """
    base          = get_base_for(acc)   # FIX-02: per-akun
    today         = datetime.now().strftime("%Y-%m-%d")
    csrf          = get_recv_csrf(acc)
    worker_before = base
    try:
        r = acc["session"].post(
            f"{base}/portal/sms/received/getsms/number",
            data={"_token": csrf, "start": today, "end": today, "range": rng},
            headers=_recv_headers(base),
        )
    except Exception as e:
        _log("NUM", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
        return {}

    if is_worker_blocked(r):
        mark_worker_limited_for(acc, worker_before)   # FIX-02
        if _all_workers_limited_for(acc) or _retry >= len(WORKER_POOL) - 1:   # FIX-02
            return {}
        time.sleep(5 * (_retry + 1))
        return get_numbers_and_otp(acc, rng, _retry + 1)
    if r.status_code == 429:
        mark_worker_limited_for(acc, worker_before)   # FIX-02
        return {}
    if _check_and_handle_login_redirect(r, acc):
        return {}

    soup   = BeautifulSoup(r.text, "html.parser")
    result = {}

    for div in soup.find_all("div", onclick=True):
        try:
            val = div["onclick"].split("'")[1]
            if val and val != rng:
                result.setdefault(val, [])
        except:
            pass

    all_texts = _parse_sms_texts(r.text)

    for num in result:
        result[num] = all_texts

    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PLATFORM DETECTION  (kode pendek teks untuk header pesan)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
        key = key.replace("TWITTER", "TWITTER")
        return SERVICE_INFO.get(key, _SVC_DEFAULT)
    return _SVC_DEFAULT

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LANGUAGE DETECTION  (dari isi teks SMS OTP)
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
    """Deteksi bahasa teks SMS OTP. Return kode 2 huruf, default 'EN'."""
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
    """
    Kembalikan (prefix, last4) untuk format GARAGE OTP.
    Contoh: '628812340303' → ('+6288', '0303')
    """
    n = str(full_num).replace("+", "").replace(" ", "")
    if len(n) >= 8:
        prefix = "+" + n[:4]
        last4  = n[-4:]
        return prefix, last4
    return "+" + n, ""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MESSAGE BUILDER  — GARAGE OTP Format
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
    Format header GARAGE OTP:
        🇮🇩 #ID WS +6288🗿0303 #EN
    """
    prefix, last4 = garage_mask_phone(full_num)
    masked_phone  = f"{prefix}🗿{last4}" if last4 else prefix
    lang_code     = detect_sms_language(sms_text) if sms_text else "EN"
    svc_code      = svc.get("code", "OT")
    return f"{flag} #{region_code} {svc_code} {masked_phone} #{lang_code}"

def build_otp_keyboard(otp: str) -> dict:
    """
    Inline keyboard style Mail_PG:
      Baris 1 (HIJAU): [📋 OTP_CODE]       → copy_text
      Baris 2 (BIRU):  [📱 NUMBER] [🔔 CHANNEL] → url
    """
    otp_display = f"{otp[:3]}-{otp[3:]}" if len(otp) == 6 else otp
    return {
        "inline_keyboard": [
            [
                {
                    "text":      f"📋 {otp_display}",
                    "copy_text": {"text": otp},
                }
            ],
            [
                {"text": "📱 NUMBER ↗",  "url": NUMBER_LINK},
                {"text": "🔔 CHANNEL ↗", "url": CHANNEL_LINK},
            ],
        ]
    }

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SENT CACHE  (FIX-05 FIX-07: dedup + race condition fix)
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

def cache_add(uid: str):
    """Tambah uid ke cache tanpa pengecekan duplikat."""
    global _cache_dirty, _last_cache_save
    should_save = False
    # FIX-07: semua update flag di dalam lock yang sama
    with _sent_cache_lock:
        sent_cache.add(uid)
        _cache_dirty = True
        if time.time() - _last_cache_save >= 5:
            should_save      = True
            _last_cache_save = time.time()
            _cache_dirty     = False
    if should_save:
        with _sent_cache_lock:
            save_sent_cache_now(sent_cache)

def cache_try_add(uid: str) -> bool:
    """
    FIX-07: ATOMIC check-and-add dengan semua flag update di dalam satu lock.
    Return True  = uid baru, pesan boleh dikirim.
    Return False = uid sudah ada, SKIP (mencegah double-send).
    """
    global _cache_dirty, _last_cache_save
    should_save = False
    with _sent_cache_lock:
        if uid in sent_cache:
            return False
        sent_cache.add(uid)
        _cache_dirty = True
        # FIX-07: cek dan update _last_cache_save di dalam lock yang sama
        if time.time() - _last_cache_save >= 5:
            should_save      = True
            _last_cache_save = time.time()
            _cache_dirty     = False
    if should_save:
        # File write di luar lock agar tidak memblok thread lain terlalu lama
        with _sent_cache_lock:
            save_sent_cache_now(sent_cache)
    return True

# ── FIX-05: pending set untuk mencegah double-enqueue OTP antar thread ───────
_pending_uids      = set()
_pending_uids_lock = threading.Lock()

def _uid_reserve(uid: str) -> bool:
    """
    FIX-05: Atomic check: apakah uid sudah ada di cache atau sedang pending kirim?
    Return True  = OK, uid sekarang ditandai pending, boleh di-enqueue.
    Return False = sudah di-cache atau sudah pending → skip.
    """
    with _sent_cache_lock:
        if uid in sent_cache:
            return False
    with _pending_uids_lock:
        if uid in _pending_uids:
            return False
        _pending_uids.add(uid)
    return True

def _uid_release_pending(uid: str):
    """FIX-05: Lepas status pending (dipanggil saat kirim gagal total)."""
    with _pending_uids_lock:
        _pending_uids.discard(uid)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GROUP TARGETS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM SEND  (FIX-04 FIX-05 FIX-08: queue + dedicated sender thread)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# FIX-08: session terpisah untuk OTP send vs cmd listener
_tg_session = requests.Session()
_tg_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=10, max_retries=0,
))

# FIX-08: session khusus cmd listener — tidak berbagi pool dengan OTP sender
_tg_cmd_session = requests.Session()
_tg_cmd_session.mount("https://", requests.adapters.HTTPAdapter(
    pool_connections=2, pool_maxsize=2, max_retries=0,
))

# FIX-04: queue FIFO untuk OTP yang akan dikirim ke Telegram
# item: (uid, otp, msg_text)
_tg_send_queue: queue.Queue = queue.Queue(maxsize=500)

def _tg_post(chat_id, text, reply_markup=None, retries=3):
    """Kirim satu pesan ke satu chat_id. Return True jika sukses."""
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

def tg_sender_worker():
    """
    FIX-04 FIX-05 FIX-08: Thread terpisah khusus pengiriman OTP ke Telegram.

    - Polling IVAS tidak pernah menunggu Telegram (non-blocking enqueue).
    - cache_try_add dipanggil HANYA setelah minimal satu grup berhasil menerima.
    - Jika semua grup gagal: _uid_release_pending() agar poll berikutnya bisa retry.
    """
    _log("THREAD+", "TG sender worker aktif", Fore.CYAN)
    while True:
        try:
            item = _tg_send_queue.get(timeout=2)
        except queue.Empty:
            continue

        uid, otp, msg_text = item
        kb          = build_otp_keyboard(otp)
        targets     = list_groups()
        any_success = False

        for cid in targets:
            ok = _tg_post(cid, msg_text, reply_markup=kb)
            if ok:
                any_success = True

        if any_success:
            # FIX-05: cache SETELAH kirim berhasil — tidak ada OTP hilang jika TG error
            cache_try_add(uid)
        else:
            # Gagal total → lepas pending agar poll berikutnya otomatis retry
            _log("TG-ERR", "semua target gagal — OTP akan dicoba ulang di poll berikutnya", Fore.RED)
            _uid_release_pending(uid)

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
            # FIX-08: pakai _tg_cmd_session agar tidak menguras pool _tg_session
            resp = _tg_cmd_session.post(
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
# POLL ONE ACCOUNT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# FIX-10: diperluas — 4-digit, 6-digit (dengan/tanpa pemisah), 8-digit
_OTP_RE = re.compile(
    r"\b\d{3}[- ]?\d{3,5}\b"   # 6–8 digit dengan/tanpa pemisah
    r"|\b\d{4}\b"               # 4 digit persis
    r"|\b\d{8}\b"               # 8 digit persis
)

def poll_one(acc) -> bool:
    """Ambil semua SMS baru dari satu akun. Return True jika ada OTP di-enqueue."""
    found  = False
    ranges = []
    try:
        ranges = get_ranges_cached(acc)
    except Exception as e:
        _log("RANGE", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
        return False

    for rng in ranges:
        if _all_workers_limited_for(acc):   # FIX-02: per-akun
            _log("RANGE", f"akun #{acc['idx']}: semua worker limited — abort poll", Fore.YELLOW)
            break

        fallback_country, code = parse_range(rng)
        try:
            numbers_data = get_numbers_and_otp(acc, rng)
        except Exception as e:
            _log("NUM", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
            continue
        if not numbers_data:
            continue

        for num, sms_list in numbers_data.items():
            if _all_workers_limited_for(acc):   # FIX-02: per-akun
                _log("RANGE", f"akun #{acc['idx']}: semua worker limited di tengah poll — berhenti", Fore.YELLOW)
                break

            full_num = normalize_number(num, code)
            if not full_num.isdigit():
                continue

            for sms in sms_list:
                clean = re.sub(r"\s+", " ", sms.replace("<#>", "")).strip()

                # FIX-06: uid menyertakan rng agar OTP identik di range berbeda
                # tidak di-skip secara salah oleh dedup cache.
                uid = hashlib.md5(f"{rng}:{clean}".encode()).hexdigest()

                matches = _OTP_RE.findall(sms)   # FIX-10: regex diperluas
                if not matches:
                    continue

                # FIX-05: cek uid di cache + pending SEBELUM enqueue
                # (tidak langsung add ke cache — cache hanya di-update setelah TG send berhasil)
                if not _uid_reserve(uid):
                    continue

                otp                        = re.sub(r"[^0-9]", "", matches[0])
                svc                        = detect_service(sms)
                country, flag, region_code = detect_country_and_flag(full_num, fallback_country)

                msg = build_otp_message(otp, svc, flag, country, region_code, full_num, sms)

                # FIX-04: enqueue non-blocking — polling tidak menunggu Telegram
                try:
                    _tg_send_queue.put_nowait((uid, otp, msg))
                except queue.Full:
                    # Queue penuh — lepas pending, poll berikutnya akan retry
                    _uid_release_pending(uid)
                    _log("TG-ERR", "queue TG penuh — OTP akan dicoba di poll berikutnya", Fore.YELLOW)
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
# ACCOUNT WORKER  (FIX-01: jitter sleep, FIX-02: per-akun worker funcs, FIX-12: heartbeat)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# FIX-12: heartbeat per thread untuk watchdog
_thread_last_heartbeat = {}   # idx -> float timestamp

def _update_heartbeat(idx: int):
    """FIX-12: Update timestamp aktivitas thread — dipanggil di tiap iterasi."""
    _thread_last_heartbeat[idx] = time.time()

def account_worker(acc):
    idx         = acc["idx"]
    sleep_time  = MIN_IDLE_SLEEP
    _last_all_limited_log = 0.0

    while True:
        _update_heartbeat(idx)   # FIX-12: heartbeat di setiap iterasi

        # ── Cek cooldown SEBELUM masuk poll ─────────────────────────────────
        # FIX-02: pakai _soonest_worker_free_in_for (per-akun)
        wait = _soonest_worker_free_in_for(acc)
        if wait > 0:
            now = time.time()
            if now - _last_all_limited_log >= 60:
                _log("WORKER", f"akun #{idx}: semua worker cooldown — tidur {wait:.0f}s", Fore.YELLOW)
                _last_all_limited_log = now
            time.sleep(wait)
            sleep_time = MIN_IDLE_SLEEP
            continue
        # ────────────────────────────────────────────────────────────────────

        try:
            found = poll_one(acc)
            if found:
                sleep_time = MIN_IDLE_SLEEP
            else:
                sleep_time = min(sleep_time + 1.0, POLL_INTERVAL_MAX)
        except Exception as e:
            _log("WORKER", f"akun #{idx}: {e}", Fore.RED)
            sleep_time = min(sleep_time * 2, POLL_INTERVAL_MAX)

        # FIX-01: jitter ±5 detik agar pola periodik antar akun tidak terbentuk
        jitter     = random.uniform(-5.0, 5.0)
        actual_sleep = max(MIN_IDLE_SLEEP, sleep_time + jitter)
        time.sleep(actual_sleep)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# KEEPALIVE  (FIX-13: baca worker state dengan lock per-akun)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_last_keepalive       = {}
_session_expired_sent = {}   # idx -> bool

def keepalive_worker(accounts):
    _log("KEEPALIVE", f"aktif — ping tiap {KEEPALIVE_INTERVAL}s per akun", Fore.CYAN)
    while True:
        now = time.time()
        for acc in accounts:
            idx = acc["idx"]
            if now - _last_keepalive.get(idx, 0) < KEEPALIVE_INTERVAL:
                continue

            # FIX-13: baca free_workers dengan acc["_wlock"] untuk thread safety
            now_ts = time.time()
            with acc["_wlock"]:
                free_workers = [w for w in WORKER_POOL
                                if acc["_wlimited"].get(w, 0) < now_ts]

            if not free_workers:
                _log("KEEPALIVE", f"akun #{idx}: semua proxy busy — skip cek session", Fore.YELLOW)
                _last_keepalive[idx] = now
                time.sleep(2)
                continue

            # Pilih satu: utamakan ivasms.com direct, fallback ke proxy pertama yang bebas
            pick = WORKER_POOL[0] if WORKER_POOL[0] in free_workers else free_workers[0]

            session_ok    = False
            all_blocked   = True
            login_expired = False

            try:
                r = acc["session"].get(f"{pick}/portal", timeout=15)
                if is_worker_blocked(r):
                    # Blocked — keepalive tidak memanggil mark_worker_limited_for
                    # (tidak berhak menambah cooldown; biarkan polling yang handle)
                    all_blocked = True
                else:
                    all_blocked = False
                    if r.status_code == 200 and "/login" not in str(r.url):
                        _recv_csrf_cache.pop(idx, None)
                        _log("KA-OK", f"akun #{idx} — session aktif ✓", Fore.GREEN)
                        _session_expired_sent[idx] = False
                        session_ok = True
                    elif "/login" in str(r.url):
                        login_expired = True
                        _log("KEEPALIVE", f"akun #{idx}: redirect /login — coba auto-login...", Fore.YELLOW)
                        login_ok = auto_login_ivas(acc)
                        if login_ok:
                            _recv_csrf_cache.pop(idx, None)
                            _log("KA-OK", f"akun #{idx}: auto-login berhasil ✓", Fore.GREEN)
                            _session_expired_sent[idx] = False
                            session_ok = True
            except Exception as e:
                _log("KA-ERR", f"{pick}: {e}", Fore.YELLOW)

            if session_ok:
                pass
            elif all_blocked:
                _log("KEEPALIVE", f"akun #{idx}: semua proxy busy (rate-limit) — skip cek session", Fore.YELLOW)
            elif login_expired and not session_ok:
                _log(
                    "KA-WARN",
                    f"akun #{idx} — session expired & auto-login gagal. "
                    f"Periksa IVAS_USERNAME/IVAS_PASSWORD atau perbarui cookie.json.",
                    Fore.YELLOW,
                )
                already_sent = _session_expired_sent.get(idx, False)
                if not already_sent and OWNER_ID and OWNER_ID != DEFAULT_TARGET:
                    try:
                        _tg_post(OWNER_ID,
                            f"⚠️ <b>SESSION EXPIRED — Auto-Login Gagal</b>\n\n"
                            f"Akun #{idx} tidak bisa akses portal IVAS.\n"
                            f"Auto-login sudah dicoba namun tetap gagal.\n\n"
                            f"Solusi:\n"
                            f"• Periksa <code>IVAS_USERNAME</code> / <code>IVAS_PASSWORD</code> di env\n"
                            f"• Atau perbarui <code>cookie.json</code> dengan cookie fresh dari browser\n"
                            f"  lalu restart bot.")
                        _session_expired_sent[idx] = True
                        _log("KA-WARN", f"akun #{idx}: notif SESSION EXPIRED dikirim (1x)", Fore.YELLOW)
                    except Exception as e:
                        _log("KA-ERR", f"gagal kirim notif Telegram: {e}", Fore.RED)
                elif already_sent:
                    _log("KA-WARN", f"akun #{idx}: notif sudah dikirim, skip", Fore.YELLOW)

            _last_keepalive[idx] = now
            time.sleep(2)
        time.sleep(60)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WATCHDOG  (FIX-12: monitor dan restart thread polling yang mati/macet)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# FIX-12: referensi thread aktif per akun — diisi di main() dan di watchdog saat restart
_poll_threads: dict = {}   # idx -> threading.Thread

def watchdog_worker(accounts):
    """
    FIX-12: Monitor thread polling per akun.
    - Jika thread mati → restart otomatis.
    - Jika thread hidup tapi tidak ada heartbeat >5 menit → log peringatan.
    Bot tidak macet diam-diam lagi.
    """
    _log("THREAD+", "watchdog aktif — monitor thread polling setiap 60s", Fore.CYAN)
    while True:
        time.sleep(60)
        now = time.time()
        for acc in accounts:
            idx = acc["idx"]
            t   = _poll_threads.get(idx)

            if t is not None and t.is_alive():
                # Thread hidup — cek heartbeat
                last = _thread_last_heartbeat.get(idx, now)
                if now - last > 300:   # 5 menit tanpa aktivitas
                    _log(
                        "WORKER",
                        f"akun #{idx}: thread hidup tapi tidak ada aktivitas {int(now - last)}s",
                        Fore.YELLOW,
                    )
                # Thread masih hidup → tidak restart
                continue

            # Thread mati → restart
            _log("WORKER", f"akun #{idx}: thread polling mati — restart otomatis!", Fore.RED)
            new_t = threading.Thread(
                target=account_worker,
                args=(acc,),
                daemon=True,
                name=f"poll-{idx}",
            )
            new_t.start()
            _poll_threads[idx] = new_t
            _thread_last_heartbeat[idx] = time.time()
            _log("THREAD+", f"akun #{idx}: thread polling berhasil di-restart", Fore.GREEN)

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
    print("  ║   🕷  SPIDERMAT OTP BOT              ║")
    print("  ║        FORWARD MODE  v2.1            ║")
    print("  ╚══════════════════════════════════════╝")
    print(Style.RESET_ALL)

    # ── LANGKAH 1: Health server SELALU start duluan ─────────────────────────
    hs_thread = threading.Thread(target=run_health_server, daemon=True, name="health")
    hs_thread.start()
    time.sleep(0.3)
    _log("SERVER", "Health server aktif — Railway healthcheck siap", Fore.GREEN)

    # ── LANGKAH 2: Validasi env vars ─────────────────────────────────────────
    if not BOT_TOKEN:
        _log("FATAL", "BOT_TOKEN belum diset! Set via environment variable.", Fore.RED)
        while True:
            time.sleep(60)

    if IVAS_USERNAME and IVAS_PASSWORD:
        _log("LOGIN", f"Auto-login aktif: {IVAS_USERNAME}", Fore.GREEN)
    else:
        _log("LOGIN", "IVAS_USERNAME/IVAS_PASSWORD belum diset — auto-login nonaktif", Fore.YELLOW)

    _load_groups()

    # ── LANGKAH 3: Load cookies ───────────────────────────────────────────────
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
            # FIX-02: per-account worker state — tidak lagi global
            "_widx":    idx % len(WORKER_POOL),   # start di worker berbeda per akun
            "_wlimited": {},                        # url -> cooldown_until timestamp
            "_wlock":   threading.Lock(),           # lock khusus worker state akun ini
        }
        accounts.append(acc)
        _log("COOKIE", f"Akun #{idx} — {len(ck)} cookie dimuat, worker awal: {WORKER_POOL[idx % len(WORKER_POOL)]}", Fore.GREEN)

    # FIX-11: inisialisasi _login_lock SEBELUM thread apapun start
    # Mencegah race condition saat inisialisasi lazy di auto_login_ivas()
    for acc in accounts:
        _login_lock[acc["idx"]]   = threading.Lock()
        _login_result[acc["idx"]] = False
    _log("CONFIG", f"Login lock diinisialisasi untuk {len(accounts)} akun", Fore.CYAN)

    print()
    _log("CONFIG", f"Default target   →  {DEFAULT_TARGET}",               Fore.CYAN)
    _log("CONFIG", f"Total target     →  {len(list_groups())} grup",      Fore.CYAN)
    _log("CONFIG", f"Channel link     →  {CHANNEL_LINK}",                 Fore.CYAN)
    _log("CONFIG", f"Worker pool      →  {len(WORKER_POOL)} proxy",       Fore.CYAN)
    _log("CONFIG", f"Poll interval    →  max {POLL_INTERVAL_MAX}s",       Fore.CYAN)
    _log("CONFIG", f"Min idle sleep   →  {MIN_IDLE_SLEEP}s ± 5s jitter",  Fore.CYAN)
    _log("CONFIG", f"Stagger delay    →  {WORKER_STAGGER_DELAY}s per akun", Fore.CYAN)
    _log("CONFIG", f"Keepalive        →  tiap {KEEPALIVE_INTERVAL}s",     Fore.CYAN)
    _log("CONFIG", f"TG send queue    →  maxsize 500",                    Fore.CYAN)
    print()

    # ── LANGKAH 4: Init keepalive timestamp ──────────────────────────────────
    _now = time.time()
    for acc in accounts:
        _last_keepalive[acc["idx"]] = _now

    # ── LANGKAH 5: Start thread-thread background ─────────────────────────────
    threading.Thread(target=tg_update_listener,                  daemon=True, name="cmd-listener").start()
    threading.Thread(target=keepalive_worker, args=(accounts,),  daemon=True, name="keepalive").start()

    # FIX-04 FIX-05: dedicated TG sender worker thread
    threading.Thread(target=tg_sender_worker,                    daemon=True, name="tg-sender").start()
    _log("THREAD+", "TG sender worker dimulai", Fore.GREEN)

    # FIX-12: watchdog thread — restart poll thread yang mati
    threading.Thread(target=watchdog_worker, args=(accounts,),   daemon=True, name="watchdog").start()
    _log("THREAD+", "Watchdog thread dimulai", Fore.GREEN)

    # FIX-01 FIX-02: start poll thread per akun dengan stagger
    for i, acc in enumerate(accounts):
        # FIX-01: stagger — akun ke-i mulai polling setelah i*15 detik
        stagger = i * WORKER_STAGGER_DELAY
        if stagger > 0:
            _log("THREAD+", f"Akun #{acc['idx']} — polling mulai dalam {stagger:.0f}s (stagger)", Fore.CYAN)
            time.sleep(stagger)

        t = threading.Thread(
            target=account_worker,
            args=(acc,),
            daemon=True,
            name=f"poll-{acc['idx']}",
        )
        t.start()
        _poll_threads[acc["idx"]] = t   # FIX-12: daftar ke watchdog
        _thread_last_heartbeat[acc["idx"]] = time.time()
        _log("THREAD+", f"Akun #{acc['idx']} — polling aktif (worker: {get_base_for(acc)})", Fore.GREEN)

    print()
    _log("CONFIG", "Bot berjalan. Ketik /addbot di grup untuk mendaftarkan.", Fore.CYAN)

    # ── LANGKAH 6: Main thread — flush cache periodik ─────────────────────────
    while True:
        if _cache_dirty and time.time() - _last_cache_save >= 5:
            with _sent_cache_lock:
                save_sent_cache_now(sent_cache)
            _last_cache_save = time.time()
            _cache_dirty     = False
        time.sleep(5)

main()
