FROM python:3.11-slim

WORKDIR /app

# ── System deps ──────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libfreetype6 \
    libjpeg62-turbo \
    libpng-dev \
    libwebp-dev \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────────────────────
COPY bot/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ── Bot source ────────────────────────────────────────────────────────────────
# Salin seluruh folder bot/ → /app/bot/
COPY bot/ /app/bot/

# Buat folder data yang dibutuhkan
RUN mkdir -p /app/bot/file /app/bot/voice

# ── Runtime ───────────────────────────────────────────────────────────────────
WORKDIR /app/bot

# Port keepalive / health check (Railway assign via $PORT, default 5000)
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=5 \
    CMD curl -sf http://localhost:${PORT:-5000}/health || exit 1

CMD ["python", "-u", "main.py"]
