FROM python:3.11-slim

WORKDIR /app

# System deps for Pillow, audio, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libfreetype6 libjpeg62-turbo libpng-dev libwebp-dev \
    ffmpeg curl && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot files
COPY bot/ ./bot/

# Expose keepalive port
EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT:-5000}/health || exit 1

WORKDIR /app/bot
CMD ["python", "main.py"]
