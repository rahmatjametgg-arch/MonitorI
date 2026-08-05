"""
SPIDERMAT OTP BOT — ASYNC ULTRA-FAST FORWARD MODE
"""

import asyncio
import hashlib
import json
import os
import random
import re
import signal
import sys
import time
from datetime import datetime

import httpx
import phonenumbers
from bs4 import BeautifulSoup
from colorama import Fore, Style, init
from phonenumbers import geocoder

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN      = os.getenv("BOT_TOKEN", "")
OWNER_ID       = int(os.getenv("OWNER_ID", "0"))

DEFAULT_TARGET = -1003686221386
CHANNEL_LINK   = "https://t.me/matttttcha"
ALL_FILES_LINK = "https://t.me/matchaappp"

COOKIE_FILE    = "cookie.json"
CACHE_FILE     = "file/sent_cache.json"
GROUPS_FILE    = "file/groups.json"
MAX_CACHE      = 2000
POLL_INTERVAL  = 2.0
KEEPALIVE_INT  = 480

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOGGING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_LOG_ICONS = {
    "OTP": "🟢", "COOKIE": "🍪", "CONFIG": "⚙️ ", "WORKER": "🔄",
    "RANGE": "📡", "CSRF": "🔑", "KA-OK": "💚", "KA-WARN": "🟡",
    "KA-ERR": "🔴", "KEEPALIVE": "🫀", "SERVER": "🌐", "CACHE": "💾",
    "TG-ERR": "❌", "NUM": "📟", "SMS": "📨", "TASK+": "⚡",
    "SHUTDOWN": "🛑", "FATAL": "💀", "CMD": "⌨️ ", "GROUP": "👥",
}

def _log(tag, msg, color=Fore.CYAN):
    icon = _LOG_ICONS.get(tag, "•")
    ts = datetime.now().strftime("%H:%M:%S")
    label = f"{icon} {tag:<9}"
    print(color + f"  {ts}  {label}  {msg}" + Style.RESET_ALL, flush=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# WORKER POOL MANAGER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WORKER_POOL = [
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

_worker_limited_until = {}
WORKER_LIMIT_COOLDOWN = 180

def get_base() -> str:
    now = time.time()
    available = [w for w in WORKER_POOL if _worker_limited_until.get(w, 0) < now]
    if not available:
        return min(WORKER_POOL, key=lambda w: _worker_limited_until.get(w, 0))
    return random.choice(available)

def mark_worker_limited(url: str):
    _worker_limited_until[url] = time.time() + WORKER_LIMIT_COOLDOWN
    _log("WORKER", f"rate-limited → cooling down {url}", Fore.YELLOW)

_RATE_LIMIT_MARKERS = (
    "temporarily rate limited", "error 1027", "please check back later",
    "has been rate limited", "error 1015", "you have been blocked",
    "attention required", "error 1020", "checking your browser", "just a moment",
)

def is_worker_blocked(resp: httpx.Response) -> bool:
    if resp is None:
        return False
    if resp.status_code == 429:
        return True
    sample = resp.text[:2000].lower()
    return any(m in sample for m in _RATE_LIMIT_MARKERS)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COOKIE & SESSION MANAGEMENT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_cookies() -> list:
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

def create_async_client(cookies: dict) -> httpx.AsyncClient:
    hdrs = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://ivasms.com",
        "Referer": "https://ivasms.com/",
    }
    return httpx.AsyncClient(
        cookies=cookies,
        headers=hdrs,
        follow_redirects=True,
        timeout=15.0,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=40)
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SENT CACHE & GROUPS (ASYNC SAFE)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
sent_cache = set()
_cache_dirty = False
_forward_targets = {DEFAULT_TARGET}

def load_sent_cache():
    global sent_cache
    os.makedirs("file", exist_ok=True)
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                sent_cache = set(data) if isinstance(data, list) else set()
        except Exception:
            sent_cache = set()

def save_sent_cache():
    global _cache_dirty
    if not _cache_dirty:
        return
    try:
        os.makedirs("file", exist_ok=True)
        lst = list(sent_cache)[-MAX_CACHE:]
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(lst, f)
        _cache_dirty = False
    except Exception as e:
        _log("CACHE", f"save error: {e}", Fore.YELLOW)

def load_groups():
    if os.path.exists(GROUPS_FILE):
        try:
            with open(GROUPS_FILE, "r", encoding="utf-8") as f:
                ids = json.load(f)
            if isinstance(ids, list):
                for gid in ids:
                    _forward_targets.add(int(gid))
            _log("GROUP", f"{len(ids)} grup dimuat dari {GROUPS_FILE}", Fore.CYAN)
        except Exception as e:
            _log("GROUP", f"load error: {e}", Fore.YELLOW)

def save_groups():
    try:
        os.makedirs("file", exist_ok=True)
        with open(GROUPS_FILE, "w", encoding="utf-8") as f:
            json.dump(list(_forward_targets), f)
    except Exception as e:
        _log("GROUP", f"save error: {e}", Fore.YELLOW)
        
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# IVAS API (ASYNC)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_recv_csrf_cache = {}
RECV_CSRF_TTL = 900

def _recv_headers(base):
    return {
        "Accept": "text/html,*/*;q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": f"{base}/portal/sms/received",
        "Origin": "https://ivasms.com",
    }

async def get_recv_csrf(acc: dict, client: httpx.AsyncClient, retry=0) -> str:
    idx = acc["idx"]
    now = time.time()
    cached = _recv_csrf_cache.get(idx)
    if cached and (now - cached["ts"]) < RECV_CSRF_TTL:
        return cached["csrf"]
        
    base = get_base()
    try:
        r = await client.get(f"{base}/portal/sms/received")
        if is_worker_blocked(r) and retry < len(WORKER_POOL) - 1:
            mark_worker_limited(base)
            return await get_recv_csrf(acc, client, retry + 1)
            
        soup = BeautifulSoup(r.text, "html.parser")
        meta = soup.find("meta", {"name": "csrf-token"})
        csrf = meta.get("content", "") if meta else ""
        if csrf:
            _recv_csrf_cache[idx] = {"csrf": csrf, "ts": now}
            return csrf
    except Exception as e:
        _log("CSRF", f"akun #{idx}: {e}", Fore.YELLOW)
    return ""

async def get_ranges(acc: dict, client: httpx.AsyncClient, retry=0) -> list:
    base = get_base()
    today = datetime.now().strftime("%Y-%m-%d")
    csrf = await get_recv_csrf(acc, client)
    
    try:
        r = await client.post(
            f"{base}/portal/sms/received/getsms",
            data={"_token": csrf, "from": today, "to": today},
            headers=_recv_headers(base)
        )
        if is_worker_blocked(r) and retry < len(WORKER_POOL) - 1:
            mark_worker_limited(base)
            return await get_ranges(acc, client, retry + 1)
            
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            ranges = []
            for div in soup.find_all("div", onclick=True):
                if "toggleRange" in div.get("onclick", ""):
                    try:
                        ranges.append(div["onclick"].split("'")[1])
                    except Exception:
                        pass
            return list(set(ranges))
    except Exception as e:
        _log("RANGE", f"akun #{acc['idx']}: {e}", Fore.YELLOW)
    return []

async def get_numbers(client: httpx.AsyncClient, rng: str, retry=0) -> list:
    base = get_base()
    try:
        r = await client.post(
            f"{base}/portal/sms/received/getsms/number",
            data={"start": "today", "end": "today", "range": rng},
            headers=_recv_headers(base)
        )
        if is_worker_blocked(r) and retry < len(WORKER_POOL) - 1:
            mark_worker_limited(base)
            return await get_numbers(client, rng, retry + 1)
            
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            numbers = []
            for div in soup.find_all("div", onclick=True):
                try:
                    val = div["onclick"].split("'")[1]
                    if val and val != rng:
                        numbers.append(val)
                except Exception:
                    pass
            return list(set(numbers))
    except Exception as e:
        _log("NUM-ERR", f"error get_numbers {rng}: {e}", Fore.RED)
    return []

async def get_sms(client: httpx.AsyncClient, rng: str, number: str) -> list:
    base = get_base()
    url = f"{base}/portal/sms/received/getsms/number"
    payload = {"range": rng, "number": number}
    
    try:
        r = await client.post(url, data=payload, headers=_recv_headers(base))
        if is_worker_blocked(r):
            mark_worker_limited(base)
            return []
            
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return data[:2]
            elif isinstance(data, dict):
                return data.get("sms", [])[:2]
    except Exception:
        pass
    return []

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PARSER & FORMATTER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SERVICE_INFO = {
    "WHATSAPP":  {"icon": "💬", "name": "WhatsApp",  "short": "#WS"},
    "TELEGRAM":  {"icon": "✈️", "name": "Telegram",  "short": "#TG"},
    "GOOGLE":    {"icon": "🔍", "name": "Google",    "short": "#G" },
    "FACEBOOK":  {"icon": "📘", "name": "Facebook",  "short": "#FB"},
    "INSTAGRAM": {"icon": "📷", "name": "Instagram", "short": "#IG"},
    "TIKTOK":    {"icon": "🎵", "name": "TikTok",    "short": "#TT"},
    "GRAB":      {"icon": "🚗", "name": "Grab",      "short": "#GR"},
    "GOJEK":     {"icon": "🛵", "name": "Gojek",     "short": "#GJ"},
    "SHOPEE":    {"icon": "🟠", "name": "Shopee",    "short": "#SP"},
    "TOKOPEDIA": {"icon": "🛍️", "name": "Tokopedia", "short": "#TP"},
}
_SVC_DEFAULT = {"icon": "💌", "name": "OTP", "short": "#OT"}
_SVC_PATTERN = re.compile(r"(WhatsApp|Telegram|Google|Facebook|Instagram|TikTok|Grab|Gojek|Shopee|Tokopedia)", re.IGNORECASE)

def detect_service(text: str) -> dict:
    m = _SVC_PATTERN.search(text)
    return SERVICE_INFO.get(m.group(1).upper(), _SVC_DEFAULT) if m else _SVC_DEFAULT

def code_to_flag(code: str) -> str:
    try:
        return "".join(chr(127397 + ord(c)) for c in code.upper())
    except Exception:
        return "🏳"

def detect_country_and_flag(full_num: str, fallback="UNKNOWN"):
    try:
        parsed = phonenumbers.parse("+" + full_num, None)
        region = phonenumbers.region_code_for_number(parsed)
        if region:
            flag = code_to_flag(region)
            desc = geocoder.description_for_number(parsed, "en")
            return (desc.upper() if desc else fallback), flag, region
    except Exception:
        pass
    return fallback, "🏳", "??"

def parse_range(rng: str):
    country = re.sub(r"\s*\(.*?\)", "", rng)
    country = re.sub(r"\d+", "", country)
    country = re.sub(r"\s+", " ", country).strip().upper()
    code_match = re.search(r"\((\d+)\)", rng)
    code = code_match.group(1) if code_match else ""
    return country, code

def normalize_number(num: str, country_code: str) -> str:
    num = str(num).strip().replace(" ", "").replace("-", "").replace("+", "")
    if country_code and num.startswith(country_code):
        return num
    if num.startswith("0") and country_code:
        return country_code + num[1:]
    return num

def mask_phone(number: str) -> str:
    n = str(number).replace("+", "").replace(" ", "")
    if len(n) >= 10:
        return f"+{n[:4]}{'·' * 4}{n[-4:]}"
    return f"+{n}"

def build_otp_message(svc: dict, flag: str, masked_num: str, full_number: str) -> str:
    svc_tag = svc.get('short', '#OTP')
    svc_icon = svc.get('icon', '💬')
    clean_num = ''.join(filter(str.isdigit, full_number)) if full_number else ''.join(filter(str.isdigit, masked_num))
    prefix = clean_num[:6] if clean_num else "123456"

    return (
        f"{flag} <b>{svc_tag}</b> {svc_icon} <code>{masked_num}</code> 🔴\n"
        f"Prefix: <tg-spoiler>{prefix}</tg-spoiler>"
    )

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM BOT SENDER (ASYNC)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def tg_post(tg_client: httpx.AsyncClient, chat_id: int, text: str, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
        
    try:
        r = await tg_client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload)
        data = r.json()
        if r.status_code == 429:
            wait = data.get("parameters", {}).get("retry_after", 3)
            await asyncio.sleep(wait)
            return await tg_post(tg_client, chat_id, text, reply_markup)
        return data.get("ok", False)
    except Exception as e:
        _log("TG-ERR", f"chat {chat_id}: {e}", Fore.RED)
        return False

async def tg_send_otp(tg_client: httpx.AsyncClient, otp: str, msg_text: str):
    kb = {
        "inline_keyboard": [
            [{"text": f"🔑 {otp}", "copy_text": {"text": otp}}],
            [{"text": "📁 All Files", "url": ALL_FILES_LINK}],
        ]
    }
    targets = list(_forward_targets)
    tasks = [tg_post(tg_client, cid, msg_text, reply_markup=kb) for cid in targets]
    await asyncio.gather(*tasks)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASYNC POLLING WORKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_OTP_RE = re.compile(r"\b\d{3}[- ]?\d{3}\b|\b\d{4,8}\b")

async def process_number_async(client: httpx.AsyncClient, tg_client: httpx.AsyncClient, acc: dict, rng: str, num: str, fallback_country: str, code: str):
    global _cache_dirty
    full_num = normalize_number(num, code)
    if not full_num.isdigit():
        return False

    sms_list = await get_sms(client, rng, num)
    found = False

    for sms in sms_list:
        clean = re.sub(r"\s+", " ", str(sms).replace("<#>", "")).strip()
        uid = hashlib.md5(f"{num}-{clean}".encode()).hexdigest()

        if uid in sent_cache:
            continue

        matches = _OTP_RE.findall(clean)
        if not matches:
            continue

        otp = re.sub(r"[^0-9]", "", matches[0])
        svc = detect_service(clean)
        country, flag, region_code = detect_country_and_flag(full_num, fallback_country)
        masked = mask_phone(full_num)

        msg = build_otp_message(svc, flag, masked, full_num)
        await tg_send_otp(tg_client, otp, msg)
        
        sent_cache.add(uid)
        _cache_dirty = True

        _log("OTP", f"{svc['icon']} {svc['name']:<10} {flag} {region_code} {masked} -> {otp}", Fore.GREEN)
        found = True

    return found

async def poll_one_account(client: httpx.AsyncClient, tg_client: httpx.AsyncClient, acc: dict):
    ranges = await get_ranges(acc, client)
    if not ranges:
        return False

    found_any = False
    for rng in ranges:
        fallback_country, code = parse_range(rng)
        numbers = await get_numbers(client, rng)
        if not numbers:
            continue

        tasks = [process_number_async(client, tg_client, acc, rng, num, fallback_country, code) for num in numbers]
        results = await asyncio.gather(*tasks)
        if any(results):
            found_any = True

    return found_any

async def account_worker_loop(acc: dict, tg_client: httpx.AsyncClient):
    async with create_async_client(acc["cookies"]) as client:
        _log("TASK+", f"Akun #{acc['idx']} — Async Polling aktif", Fore.GREEN)
        while True:
            try:
                found = await poll_one_account(client, tg_client, acc)
                await asyncio.sleep(0.5 if found else POLL_INTERVAL)
            except Exception as e:
                _log("WORKER", f"error akun #{acc['idx']}: {e}", Fore.RED)
                await asyncio.sleep(5.0)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TELEGRAM COMMAND LISTENER (ASYNC)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def handle_command(tg_client: httpx.AsyncClient, update: dict):
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return

    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    chat_type = chat.get("type", "")
    chat_name = chat.get("title") or chat.get("username") or str(chat_id)
    text = (msg.get("text") or "").strip()

    cmd = text.split()[0].split("@")[0].lower() if text.startswith("/") else ""

    if cmd == "/addbot":
        if chat_type not in ("group", "supergroup"):
            await tg_post(tg_client, chat_id, "⚠️ <b>Gunakan perintah ini di dalam grup.</b>")
            return

        if chat_id not in _forward_targets:
            _forward_targets.add(chat_id)
            save_groups()
            _log("GROUP", f"✅ ditambahkan: {chat_name} ({chat_id})", Fore.GREEN)
            await tg_post(tg_client, chat_id, f"✅ <b>Grup {chat_name} berhasil terdaftar untuk menerima OTP.</b>")
        else:
            await tg_post(tg_client, chat_id, f"ℹ️ Grup {chat_name} sudah terdaftar sebelumnya.")

    elif cmd == "/removebot":
        if chat_id == DEFAULT_TARGET:
            await tg_post(tg_client, chat_id, "⛔ Grup utama tidak bisa dihapus.")
            return

        if chat_id in _forward_targets:
            _forward_targets.remove(chat_id)
            save_groups()
            _log("GROUP", f"🗑️ dihapus: {chat_name} ({chat_id})", Fore.YELLOW)
            await tg_post(tg_client, chat_id, f"🗑️ <b>Grup {chat_name} telah dihapus dari daftar OTP.</b>")

    elif cmd == "/listbot":
        groups = list(_forward_targets)
        lines = [f"  {i+1}. <code>{gid}</code>" for i, gid in enumerate(groups)]
        await tg_post(tg_client, chat_id, f"👥 <b>DAFTAR GRUP AKTIF ({len(groups)})</b>\n\n" + "\n".join(lines))

async def tg_update_listener(tg_client: httpx.AsyncClient):
    offset = 0
    _log("CMD", "Telegram Update Listener Aktif", Fore.CYAN)
    while True:
        try:
            r = await tg_client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                json={"offset": offset, "timeout": 20, "allowed_updates": ["message"]},
                timeout=30.0
            )
            data = r.json()
            if data.get("ok"):
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    await handle_command(tg_client, upd)
        except Exception:
            await asyncio.sleep(3.0)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN ASYNC ENGINE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def cache_flush_loop():
    while True:
        save_sent_cache()
        await asyncio.sleep(15.0)

async def main():
    print(Fore.CYAN + Style.BRIGHT)
    print("  ╔══════════════════════════════════════╗")
    print("  ║   🕷  SPIDERMAT OTP BOT (ASYNC)     ║")
    print("  ║        FORWARD MODE — ULTRA FAST     ║")
    print("  ╚══════════════════════════════════════╝" + Style.RESET_ALL)

    if not BOT_TOKEN:
        _log("FATAL", "BOT_TOKEN belum diset!", Fore.RED)
        sys.exit(1)

    load_groups()
    load_sent_cache()

    cookies_list = load_cookies()
    if not cookies_list:
        _log("FATAL", f"Tidak ada cookie valid di {COOKIE_FILE}!", Fore.RED)
        sys.exit(1)

    accounts = [{"idx": i, "cookies": ck} for i, ck in enumerate(cookies_list)]

    async with httpx.AsyncClient(timeout=15.0) as tg_client:
        tasks = [
            asyncio.create_task(cache_flush_loop()),
            asyncio.create_task(tg_update_listener(tg_client))
        ]

        for acc in accounts:
            tasks.append(asyncio.create_task(account_worker_loop(acc, tg_client)))

        _log("CONFIG", f"Bot berjalan dengan {len(accounts)} akun IVAS.", Fore.CYAN)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        save_sent_cache()
        save_groups()
        _log("SHUTDOWN", "Bot berhasil dihentikan.", Fore.YELLOW)
                               
