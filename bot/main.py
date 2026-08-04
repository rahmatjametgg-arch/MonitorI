import httpx
from bs4 import BeautifulSoup
import re
import time
import json
import os
import sys
import threading
import requests
from colorama import init, Fore, Style

# Direct stdout log tanpa buffer (Real-time)
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)
init(autoreset=True)

# ================= KONFIGURASI =================
# Isi Token Bot & ID Group Telegram Tujuan di sini (atau via Env Variables)
BOT_TOKEN     = os.getenv("BOT_TOKEN", "8889061301:AAHeS_1vWRvEngCvCch9Hw7YhqGHh2QZp6I")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID", "-1003937740976")  # ID Group / Channel tempat OTP dikirim

# File cookie / akun IVAS
COOKIES_FILE  = "accounts.json"

# List Worker Proxy IVAS
WORKER_POOL = [
    "https://plain-butterfly-d9e9.kicenivas.workers.dev",
    "https://ivasmunchen.serverprivate1.web.id",
    "https://ivasmsbykicenv2.kikixrakaofficial.biz.id",
    "https://ivasbykiven.alwayskixyzshop.web.id",
]

# ================= HELPER TELEGRAM =================
def send_telegram_msg(text):
    """Fungsi simpel khusus kirim OTP ke Group Telegram"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TARGET_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        return r.status_code == 200
    except Exception as e:
        print(Fore.RED + f"[TELEGRAM ERROR] Gagal kirim pesan: {e}")
        return False

# ================= LOGIC PARSING SMS / OTP =================
def format_otp_message(sender, phone_number, sms_text):
    """Format tampilan pesan OTP yang bakal dikirim ke Group Telegram"""
    # Mencoba ekstrak 3-8 digit angka sebagai OTP
    otp_code = "Tidak terdeteksi"
    match = re.search(r'\b\d{3,8}\b', sms_text)
    if match:
        otp_code = match.group(0)

    msg = (
        f"📩 <b>OTP IVAS INCOMING</b>\n"
        f"────────────────────\n"
        f"📱 <b>Nomor:</b> <code>{phone_number}</code>\n"
        f"👤 <b>Pengirim:</b> <code>{sender}</code>\n"
        f"🔑 <b>Kode OTP:</b> <code>{otp_code}</code>\n"
        f"────────────────────\n"
        f"💬 <b>Pesan Lengkap:</b>\n<i>{sms_text}</i>"
    )
    return msg

# ================= IVAS SMS MONITOR =================
def fetch_latest_sms(session, base_url):
    """Mengambil SMS masuk dari dashboard IVAS"""
    recv_url = f"{base_url}/portal/sms/received"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": recv_url,
    }
    
    try:
        # Request data SMS terbaru dari IVAS
        resp = session.get(f"{base_url}/portal/sms/received/getsms", headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])
    except Exception as e:
        print(Fore.YELLOW + f"[IVAS ERROR] Fetch SMS gagal: {e}")
    return []

def start_otp_forwarder():
    """Main loop yang standby 24/7 nge-check & forward OTP"""
    print(Fore.CYAN + Style.BRIGHT + "🚀 IVAS AUTO-FORWARD OTP BOT STARTED...")
    
    # Load akun / cookie
    if not os.path.exists(COOKIES_FILE):
        print(Fore.RED + f"❌ File {COOKIES_FILE} tidak ditemukan! Buat file tersebut dan masukkan cookie/session IVAS.")
        return

    try:
        with open(COOKIES_FILE, "r") as f:
            accounts = json.load(f)
    except Exception as e:
        print(Fore.RED + f"❌ Gagal membaca {COOKIES_FILE}: {e}")
        return

    if not accounts:
        print(Fore.RED + "❌ Tidak ada akun/cookie di dalam file.")
        return

    # Cache untuk mencatat SMS ID yang sudah pernah dikirim biar ga spam/duplikat
    seen_sms_ids = set()
    worker_idx = 0

    while True:
        current_worker = WORKER_POOL[worker_idx % len(WORKER_POOL)]
        
        for acc in accounts:
            # Menggunakan session requests dari cookie akun
            session = requests.Session()
            if "cookies" in acc:
                session.cookies.update(acc["cookies"])
                
            sms_list = fetch_latest_sms(session, current_worker)
            
            for sms in sms_list:
                # Mengambil unique ID dari SMS (atau gabungan timestamp + nomor)
                sms_id = sms.get("id") or f"{sms.get('number')}_{sms.get('created_at')}"
                
                if sms_id not in seen_sms_ids:
                    seen_sms_ids.add(sms_id)
                    
                    phone = sms.get("number", "Unknown")
                    sender = sms.get("sender", "System")
                    text = sms.get("sms", "")
                    
                    print(Fore.GREEN + f"✅ SMS Baru Ditemukan dari {phone}! Memproses forward...")
                    
                    # Format dan kirim langsung ke Telegram
                    formatted_msg = format_otp_message(sender, phone, text)
                    send_telegram_msg(formatted_msg)
        
        # Batasi memori cache ID SMS agar tidak membengkak
        if len(seen_sms_ids) > 5000:
            seen_sms_ids.clear()

        # Cooldown check interval (misal setiap 3-5 detik)
        time.sleep(4)

if __name__ == "__main__":
    start_otp_forwarder()
            
