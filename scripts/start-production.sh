#!/usr/bin/env bash
# ============================================================
# SPIDERMAT OTP BOT — Production Startup Script
# Starts Telegram bot (background) + API server (foreground)
# ============================================================
set -e

echo "[BOOT] Starting SPIDERMAT OTP BOT..."
python bot/main.py &
BOT_PID=$!
echo "[BOOT] Bot PID: $BOT_PID"

echo "[BOOT] Starting API Server..."
exec node --enable-source-maps artifacts/api-server/dist/index.mjs
