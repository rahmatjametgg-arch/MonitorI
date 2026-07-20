# 🚀 Deploy SPIDERMAT OTP BOT ke Railway

> Bot jalan **24/7 non-stop**, gratis $5 kredit/bulan dari Railway (cukup untuk bot kecil).

---

## Langkah 1 — Push ke GitHub

> Kamu butuh akun GitHub. Kalau belum punya, daftar di https://github.com

### Di Replit:
1. Buka tab **Git** di sidebar kiri Replit
2. Klik **Create a GitHub repository**
3. Kasih nama repo, misal: `spidermat-otp-bot`
4. Pilih **Private** (biar aman)
5. Klik **Create repository**
6. Tunggu sampai selesai upload

---

## Langkah 2 — Buat Akun Railway

1. Buka https://railway.app
2. Klik **Login** → pilih **Login with GitHub**
3. Izinkan Railway akses GitHub kamu
4. Verifikasi nomor HP (dibutuhkan untuk dapat kredit gratis)

---

## Langkah 3 — Buat Project Baru di Railway

1. Di dashboard Railway, klik **New Project**
2. Pilih **Deploy from GitHub repo**
3. Cari repo `spidermat-otp-bot` yang tadi dibuat
4. Klik repo-nya → Railway langsung mulai build (tunggu ~3-5 menit)

---

## Langkah 4 — Tambahkan PostgreSQL (Database)

1. Di project Railway, klik **+ New** (tombol di pojok kanan atas)
2. Pilih **Database** → **Add PostgreSQL**
3. Tunggu database dibuat (sekitar 30 detik)
4. Railway otomatis isi `DATABASE_URL` ke bot kamu — **tidak perlu copy-paste manual**

---

## Langkah 5 — Isi Environment Variables

1. Klik service bot kamu (bukan database) di dashboard Railway
2. Buka tab **Variables**
3. Klik **+ New Variable** untuk setiap baris di bawah ini:

---

### 📋 TEMPEL INI SATU-SATU (isi nilainya):

| Variable | Nilai |
|----------|-------|
| `BOT_TOKEN` | Token dari @BotFather |
| `OWNER_ID` | User ID kamu (cek via @userinfobot) |
| `LOG_CHANNEL_ID` | ID channel log (awali `-100...`) — kosongkan jika tidak perlu |
| `LINK_OWNER` | `t.me/usernameKamu` |
| `LINK_CHANNEL` | `t.me/channelKamu` |
| `FORCE_JOIN_CHANNELS` | Username channel tanpa @, pisah koma. Kosongkan jika tidak perlu |

> `DATABASE_URL` sudah otomatis terisi dari step 4 — **jangan diubah**.

---

### Cara cepat isi banyak variable sekaligus:

1. Di tab **Variables** Railway, klik **RAW Editor**
2. Tempel teks ini, isi nilainya:

```
BOT_TOKEN=ISI_DI_SINI
OWNER_ID=ISI_DI_SINI
LOG_CHANNEL_ID=ISI_DI_SINI
LINK_OWNER=t.me/usernameKamu
LINK_CHANNEL=t.me/channelKamu
FORCE_JOIN_CHANNELS=
```

3. Klik **Update Variables**
4. Railway otomatis restart bot dengan settings baru

---

## Langkah 6 — Cek Bot Hidup

1. Di Railway, buka tab **Deployments** → lihat log
2. Tunggu sampai muncul teks seperti:
   ```
   SPIDERMAT OTP BOT  @NamaBotKamu
   [SERVER    ] port XXXX | /health /status
   [BOT-MGR   ] per-account threading aktif
   ```
3. Buka Telegram → kirim `/start` ke bot kamu
4. ✅ Bot harus balas!

---

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| Build gagal | Cek tab **Build Logs** di Railway, cari error merah |
| Bot tidak balas | Pastikan `BOT_TOKEN` benar, cek **Deploy Logs** |
| Error database | Pastikan PostgreSQL plugin sudah ditambahkan (step 4) |
| Bot mati sendiri | Cek tab **Deployments** → lihat apakah ada crash loop |

---

## Estimasi Biaya Railway

| Penggunaan | Biaya |
|-----------|-------|
| Bot Telegram (low traffic) | ~$0.50-1.00/bulan |
| PostgreSQL | ~$0.50/bulan |
| **Total** | **~$1-2/bulan** |
| Kredit gratis Railway | **$5/bulan** ✅ |

> **Artinya: GRATIS** selama bot tidak terlalu berat.

---

## File yang sudah siap di repo ini

```
Dockerfile      ← Railway pakai ini untuk build
railway.toml    ← Konfigurasi deploy Railway
bot/main.py     ← Kode utama bot
bot/qris.jpg    ← Foto QRIS payment kamu
bot/thumbnail.png ← Foto banner /start (Spiderman)
bot/.env.template ← Template semua env vars
```

---

*Bot akan restart otomatis jika crash. Railway juga monitor health check tiap 30 detik.*
