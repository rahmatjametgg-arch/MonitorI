"""
db.py — PostgreSQL persistence layer (key-value store)
Semua data bot disimpan ke DB sebagai JSON untuk bertahan meski deploy ulang.
"""
import os, json, threading
try:
    import psycopg2
    import psycopg2.extras
    _HAS_PG = True
except ImportError:
    _HAS_PG = False

_db_lock = threading.Lock()
_conn = None


def _get_conn():
    global _conn
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url or not _HAS_PG:
        return None
    try:
        if _conn is None or _conn.closed:
            _conn = psycopg2.connect(db_url)
            _conn.autocommit = True
        return _conn
    except Exception as e:
        print(f"[DB] koneksi gagal: {e}")
        _conn = None
        return None


def db_init():
    """Pastikan tabel bot_store ada (idempotent, safe untuk dipanggil tiap startup)."""
    conn = _get_conn()
    if not conn:
        return
    try:
        with _db_lock:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_store (
                        key TEXT PRIMARY KEY,
                        value JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
        print("[DB] tabel bot_store siap")
    except Exception as e:
        print(f"[DB] init error: {e}")


def db_save(key: str, value) -> bool:
    """Simpan value (apapun yang bisa di-JSON) ke database. Return True jika berhasil."""
    conn = _get_conn()
    if not conn:
        return False
    try:
        with _db_lock:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bot_store(key, value, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (key) DO UPDATE
                        SET value = EXCLUDED.value,
                            updated_at = NOW()
                    """,
                    (key, json.dumps(value))
                )
        return True
    except Exception as e:
        print(f"[DB] save '{key}' error: {e}")
        return False


def db_load(key: str, default=None):
    """Load value dari database. Return default jika key tidak ada atau error."""
    conn = _get_conn()
    if not conn:
        return default
    try:
        with _db_lock:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM bot_store WHERE key = %s", (key,))
                row = cur.fetchone()
        if row:
            return row[0]   # psycopg2 otomatis parse JSONB → Python object
        return default
    except Exception as e:
        print(f"[DB] load '{key}' error: {e}")
        return default


def db_save_async(key: str, value):
    """Fire-and-forget: simpan ke DB di background thread agar tidak blocking."""
    import threading
    threading.Thread(target=db_save, args=(key, value), daemon=True).start()
