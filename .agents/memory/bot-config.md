---
name: SPIDERMAT OTP BOT config
description: Key decisions and quirks for the Telegram OTP bot in bot/main.py
---

## Pricing / Tiers (as of July 2026)
- Free tier: 3 tokens/day, 1 IVAS email account
- Starter ("PREMIUM"): 7d=25k, 15d=50k, 30d=100k — 100 tokens/day, 5 email accounts
- /beli shows only starter (simplified UI); pro/elite/ultra exist in code but hidden from /beli menu

## Payment system
- Uses PAKASIR (env: PAKASIR_PROJECT, PAKASIR_API_KEY) for auto QRIS generation
- When Pakasir NOT configured → shows static QRIS image from bot/qris.jpg
- Static QRIS = Toko Rahmat NMID ID1025433619836 (owner's personal QRIS)
- Owner manually activates via /addtoken after receiving proof

## Force join fix
- `check_force_join`: if getChatMember returns ok=false (channel not found / bot not admin) → skip, do NOT block user
- Only block user if status == "left" (confirmed not joined)
- "kicked"/"restricted" → also skipped

## Images
- bot/thumbnail.png = Spiderman image (sent as /start banner photo)
- bot/qris.jpg = QRIS payment image

## Railway deployment
- Dockerfile at repo root (copies bot/ dir, runs python bot/main.py)
- railway.toml at repo root
- Bot health check: GET /health on PORT env var (default 5000)
- All secrets must be set in Railway environment variables (same names as Replit secrets)

## Replit VM deployment
- .replit: deploymentTarget = "vm"
- scripts/start-production.sh: starts bot (background) + api-server (foreground)
- api-server artifact.toml production.run uses start-production.sh

## DB layer
- bot/db.py: PostgreSQL key-value store (bot_store table)
- All save_* functions call db_save_async() in addition to writing JSON files
- All load_* functions fall back to db_load() if JSON file is empty
- db_init() called at startup
