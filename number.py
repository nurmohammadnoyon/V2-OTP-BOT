import sys
import requests
import time
import json
import os
from localdb import LocalDB, Increment, SERVER_TIMESTAMP

import uuid
import threading
import random
import re
import html
import pyotp
from collections import Counter 
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ==========================================
# Configuration (Token & Owner ID)
# ==========================================
DATA_DIR = os.environ.get("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)

def _load_token():
    t = os.environ.get("BOT_TOKEN", "").strip()
    if not t and os.path.exists("token.txt"):
        t = open("token.txt", encoding="utf-8").read().strip()
    if not t:
        raise SystemExit("BOT_TOKEN missing: set env BOT_TOKEN or put the token in token.txt")
    return t

TOKEN = _load_token()

import logging, logging.handlers, math, shutil, hashlib, traceback, csv, io, collections

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
_log_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_fh = logging.handlers.RotatingFileHandler(os.path.join(DATA_DIR, "bot_errors.log"),
                                           maxBytes=2_000_000, backupCount=3, encoding="utf-8")
_fh.setFormatter(_log_fmt)
log.addHandler(_fh)
_sh = logging.StreamHandler()
_sh.setFormatter(_log_fmt)
log.addHandler(_sh)

number_lock = threading.RLock()     # protects number stock assignment
withdraw_lock = threading.Lock()    # protects pending_withdrawals approve/reject


class BoundedSet:
    """Thread-safe 'seen' set that forgets only the OLDEST items (never clear-all)."""
    def __init__(self, limit=20000, items=None):
        self._d = collections.OrderedDict()
        self._limit = limit
        self._lock = threading.Lock()
        for x in (items or []):
            self._d[x] = None
        self._trim()

    def _trim(self):
        while len(self._d) > self._limit:
            self._d.popitem(last=False)

    def add(self, x):
        with self._lock:
            self._d[x] = None
            self._trim()

    def add_if_new(self, x):
        with self._lock:
            if x in self._d:
                return False
            self._d[x] = None
            self._trim()
            return True

    def __contains__(self, x):
        return x in self._d

    def __len__(self):
        return len(self._d)

    def snapshot(self):
        with self._lock:
            return list(self._d)

    def __iter__(self):
        return iter(self.snapshot())


def _read_json_with_fallback():
    """Read bot_data.json; if it is corrupt use the .bak copy and keep the bad file for inspection."""
    last_err = None
    for path in (DB_FILE, DB_FILE + ".bak"):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if path != DB_FILE:
                log.warning("bot_data.json unreadable - restored from .bak copy")
            return data
        except Exception as e:
            last_err = e
            log.error("Cannot read %s: %s", path, e)
            try:
                shutil.copyfile(path, f"{path}.corrupt-{int(time.time())}")
            except OSError:
                pass
    raise last_err or FileNotFoundError(DB_FILE)
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"
FILE_URL = f"https://api.telegram.org/file/bot{TOKEN}/"

_owner_env = os.environ.get("OWNER_ID", "").strip()
if not _owner_env.isdigit():
    raise SystemExit("OWNER_ID missing: set env OWNER_ID to your numeric Telegram user ID")
OWNER_ID = int(_owner_env)
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").strip().lstrip("@")  # auto-filled from getMe at startup
DB_FILE = os.path.join(DATA_DIR, "bot_data.json")

# ==========================================
# Premium Emoji Database
# ==========================================
PEM = {
    "ok": '<tg-emoji emoji-id="5352694861990501856">✅</tg-emoji>',
    "no": '<tg-emoji emoji-id="5420130255174145507">❌</tg-emoji>',
    "warn": '<tg-emoji emoji-id="5336944168944047463">⚠️</tg-emoji>',
    "admin": '<tg-emoji emoji-id="5353032893096567467">📊</tg-emoji>',
    "user": '<tg-emoji emoji-id="5352861489541714456">👤</tg-emoji>',
    "file": '<tg-emoji emoji-id="5352721946054268944">📁</tg-emoji>',
    "rocket": '<tg-emoji emoji-id="5352597830089347330">🚀</tg-emoji>',
    "graph": '<tg-emoji emoji-id="5352877703043258544">📊</tg-emoji>',
    "money": '<tg-emoji emoji-id="5348469219761626211">💸</tg-emoji>',
    "gift": '<tg-emoji emoji-id="5420396762189831222">🎁</tg-emoji>',
    "msg": '<tg-emoji emoji-id="5337302974806922068">💬</tg-emoji>',
    "gear": '<tg-emoji emoji-id="5420155432272438703">⚙️</tg-emoji>',
    "link": '<tg-emoji emoji-id="5420517437885943844">🔗</tg-emoji>',
    "trash": '<tg-emoji emoji-id="5422557736330106570">🗑</tg-emoji>',
    "upload": '<tg-emoji emoji-id="5353001161878182134">📤</tg-emoji>',
    "world": '<tg-emoji emoji-id="5336972142066047577">🌐</tg-emoji>',
    "lock": '<tg-emoji emoji-id="5353022963132174959">🔐</tg-emoji>',
    "phone": '<tg-emoji emoji-id="5337132498965010628">📱</tg-emoji>',
    "num": '<tg-emoji emoji-id="5352862640592949843">🔢</tg-emoji>',
    "pin": '<tg-emoji emoji-id="5352922460897452503">📍</tg-emoji>',
    "star": '<tg-emoji emoji-id="5352552689983067014">✨</tg-emoji>',
    "hi": '<tg-emoji emoji-id="5353027129250453493">👋</tg-emoji>'
}

GLOBAL_BODY_EMOJIS = {
    "➖": "5870818207383686839", "🚫": "5334807341109908955", "😒": "5334763399299506604",
    "🖥": "5334880948259427772", "🌐": "5334590977837403844", "🌟": "5337102391244263212",
    "🕓": "5336983442125001376", "⌛": "5337172996211648018", "💬": "5337302974806922068",
    "🔐": "5337255927735163754", "🍏": "5337132498965010628", "❔": "5336850036145823599",
    "⚠️": "5336944168944047463", "🔥": "5337267511261960341", "💸": "5348469219761626211",
    "🥚": "5348390922507817684", "👨‍⚖": "5334763399299506604", "🐁": "5348494358205207761",
    "🧻": "5348486915026884464", "⚗": "5346311574221000149", "🛴": "5348075478634766440",
    "📊": "5353032893096567467", "🔢": "5352862640592949843", "👤": "5352861489541714456",
    "📁": "5352721946054268944", "🚀": "5352597830089347330", "💎": "5352838545826420397",
    "📍": "5352922460897452503", "👋": "5353027129250453493", "✅": "5352694861990501856",
    "1️⃣": "5352651766288652742", "2️⃣": "5355186458418257716", "3️⃣": "5352867219028091093",
    "4️⃣": "5352566657216714037", "5️⃣": "5353086880835474989", "6️⃣": "5354859211975071385",
    "7️⃣": "5352859127309707652", "8️⃣": "5352957533600389988", "9️⃣": "5353060913463204207",
    "🔤": "5352727417842606016", "📣": "5352980533150259581", "📤": "5353001161878182134",
    "✨": "5352552689983067014", "🔹": "5352638632278660622", "🎙": "5355102594886833928",
    "💴": "5352985330628730418", "📅": "5352585194295564660", "📴": "5352974971167611327",
    "✏️": "5395444784611480792", "📱": "5337132498965010628", "🔗": "5420517437885943844",
    "❌": "5420130255174145507", "⚙️": "5420155432272438703", "🫂": "5420145051336485498",
    "➕": "5420323438508155202", "🗑": "5422557736330106570", "🎁": "5420396762189831222",
    "➤": "5420618897898381296", "🏢": "5420156334215565595", "💳": "5190899075968441286",
    "📝": "5192739271886282680", "🛡": "5190447043545438788", "🤝": "5192805934073685937",
    "💰": "5190576863226933563", "👀": "5190645917711114179", "🕹": "5193100774988617665",
    "🟢": "5192812028632274956", "🧪": "5190781475468915802", "🎨": "5190751148704833975",
    "📂": "5257969839313526622", "🌍": "5780471598922337683", "📌": "5318986077455795572",
    "📢": "5789428375261023681", "🆔": "5352862640592949843", "📈": "5352877703043258544",
    "🔔": "5280945826993054795", "🏦": "5348469219761626211", "🧾": "5192739271886282680",
    "👨‍⚖️": "5334763399299506604", "🔍": "5463352748751753567",
    "🔑": "5197288647275071607"
}

DEFAULT_CUSTOM_MESSAGES = {
    "start": {"text": "╔═══════════╗\n       📊 NUMBER BOT\n╚═══════════╝\n🚀 Welcome to Number & OTP Service\n━━━━━━━━━━━━\n✅ Choose an option below\nto continue using the bot.\n━━━━━━━━━━━━\n💎 Premium OTP Service", "buttons": []},
    "get_number": {"text": f"{PEM['pin']} Select a service:", "buttons": []},
    "select_country": {"text": f"📌 Select a country for {{service}}:", "buttons": []}, 
    "search_number": {"text": "╔═══════════╗\n     🔍 <b>SEARCH NUMBER</b>\n╚═══════════╝\n✅ Enter 3 to 9 digits  \nto search for a number.\n━━━━━━━━━━━━━\n📝 Example:\n➥ 880\n➥ 9227373\n━━━━━━━━━━━━━\n🔍 Fast Number Lookup System", "buttons": []},
    "traffic": {"text": f"{PEM['graph']} <b>Traffic Overview</b>\n\n{PEM['ok']} Available Numbers: {{avail}}\n{PEM['rocket']} Assigned Numbers: {{assigned}}", "buttons": []},
    "refer": {"text": f"➖➖➖➖➖➖➖\n« {PEM['gift']} REFER & EARN »\n➖➖➖➖➖➖➖\n{PEM['link']} YOUR LINK:\n<code>{{ref_link}}</code>\n➖➖➖➖➖➖➖\n{PEM['user']} TOTAL REFERS: <b>{{total_ref}}</b>\n➖➖➖➖➖➖➖\n{PEM['money']} PER REFER: <b>{{ref_reward}} TK</b>\n➖➖➖➖➖➖➖", "buttons": []},
    "withdrawal": {"text": "➖➖➖➖➖➖➖\n《 😒 WITHDRAWAL 》\n➖➖➖➖➖➖➖\n👋 Total Otp: {total_otp}\n➖➖➖➖➖➖➖\n🫂 Total Reffer :{total_ref}\n➖➖➖➖➖➖➖\n📅 BALANCE: {bal}৳\n➖➖➖➖➖➖➖\n🔐 MINIMUM: {min_w} ৳\n➖➖➖➖➖➖➖\nSELECT METHOD:", "buttons": []},
    "support": {"text": f"{PEM['msg']} Contact us for any help:", "buttons": []}
}

# ==========================================
# Firebase Setup
# ==========================================
db = LocalDB(os.path.join(DATA_DIR, "bot_users.db"))
print("Local SQLite DB ready (no Firebase needed)")

bot_settings = {
    "admins": [OWNER_ID],
    "panels": [], 
    "fw_groups": [], 
    "otp_link": "https://t.me/your_otp_group",
    "withdraw_on": True,
    "min_withdraw": 30.0,
    "otp_reward": 0.1,
    "refer_reward": 0.2,
    "refer_min_otps": 1,
    "cooldown": 10,
    "num_req": 3,
    "num_share": 1, 
    "support_link": "https://t.me/your_support",
    "w_methods": ["bKash", "Nagad"],
    "w_group": "", 
    
    "fj_on": False,
    "fj_channels": [], 
    "stex_keys": [], 
    "voltx_keys": [],
    "zenex_keys": [],
    "range_group_id": "",
    "range_group_link": "",
    "search_countries": [],
    "stex_services": {},
    "voltx_services": {},
    "zenex_services": {},
    "premium_flags": {
        "1": {"char": "🇺🇸", "iso": "US", "name": "United States", "id": "5913463998522592692"},
        "880": {"char": "🇧🇩", "iso": "BD", "name": "Bangladesh", "id": "5911365056594973179"},
        "91": {"char": "🇮🇳", "iso": "IN", "name": "India", "id": "5913754823643107921"},
        "92": {"char": "🇵🇰", "iso": "PK", "name": "Pakistan", "id": "5913705895375672082"},
        "44": {"char": "🇬🇧", "iso": "GB", "name": "United Kingdom", "id": "5913443365499703513"}
    },
    "premium_apps": {
        "FACEBOOK": {"char": "🚫", "id": "5334807341109908955", "name": "Facebook"},
        "WHATSAPP": {"char": "🚫", "id": "5334759662677957452", "name": "WhatsApp"}
    },
    "custom_messages": DEFAULT_CUSTOM_MESSAGES.copy()
}

FS_KEYS = [
    "admins", "panels", "fw_groups", "otp_link", "withdraw_on", 
    "min_withdraw", "otp_reward", "refer_reward", "refer_min_otps", "cooldown", 
    "num_req", "num_share", "support_link", "w_methods", "w_group", "stex_keys", "voltx_keys", "search_countries", "stex_services", "voltx_services",
    "fj_on", "fj_channels", "voltx_auto", "stex_auto", "zenex_auto", "zenex_keys", "zenex_services", "zenex_search_countries",
    "range_group_id", "range_group_link",
    "refer_mode", "refer_otp_reward", "daily_report_on", "last_daily_report", "low_stock_on", "low_stock_limit", "low_stock_last", "otp_daily_backfilled"
]

number_batches = {}
used_numbers_list = []
stex_assigned_numbers = {} 
voltx_assigned_numbers = {}
zenex_assigned_numbers = {}
STEX_BASE_URL = "https://api.2oo9.cloud/MXS47FLFX0U/tness/@public/api"
VOLTX_BASE_URL = "https://api.2oo9.cloud/MXS47FLFX0U/tnevs/@public/api"
ZENEX_BASE_URL = "https://api.zenexnetwork.com"
total_uploaded_stats = 0
total_assigned_stats = 0
processed_otps = BoundedSet(20000)
recent_traffic = []
user_banned_cache = {}

# Active HTTP sessions for Auto Captcha Panels
panel_sessions = {}

# 🌟 sAjaxSource (AJAX/DataTable) এবং Fallback HTML Parser Helper Function
def fetch_cpt_panel_cdrs(p, session, check_url):
    res = session.get(check_url, timeout=15)
    html_text = res.text
    
    # সেশন শেষ হয়েছে বা লগইন পেজে রিডাইরেক্ট করেছে কি না তা চেক করা
    if "login" in html_text.lower() or "signin" in html_text.lower() or any(x in html_text for x in ["Sign in to your account", "Please sign in", "Welcome back!"]):
        raise Exception("Session expired")
        
    soup = BeautifulSoup(html_text, 'html.parser')
    s_ajax_source = ""
    for script in soup.find_all("script"):
        script_text = script.string or ""
        match = re.search(r'sAjaxSource":\s*"([^"]+)"', script_text)
        if match:
            s_ajax_source = match.group(1)
            break
            
    results = []
    
    n_col_name = p.get("num_col_name", "number").lower()
    m_col_name = p.get("msg_col_name", "message").lower()
    n_idx = int(p.get("num_col_idx", 1)) - 1 if p.get("num_col_idx") else 1
    m_idx = int(p.get("msg_col_idx", 2)) - 1 if p.get("msg_col_idx") else 2

    # ৫.১ যদি sAjaxSource AJAX লিংক পাওয়া যায়
    if s_ajax_source:
        baseUrl = p.get("login_url", "").split("/client")[0].split("/login")[0].strip()
        if not baseUrl.startswith("http"):
            baseUrl = "http://" + baseUrl
            
        full_ajax_url = ""
        if s_ajax_source.startswith("http"):
            full_ajax_url = s_ajax_source
        elif s_ajax_source.startswith("/"):
            full_ajax_url = f"{baseUrl}{s_ajax_source}"
        else:
            last_slash_idx = check_url.rfind("/")
            current_dir = check_url[:last_slash_idx]
            full_ajax_url = f"{current_dir}/{s_ajax_source}"

        if "iDisplayLength" not in full_ajax_url:
            # 250 এর জায়গায় 10000 করে দেওয়া হলো যাতে সব মেসেজ ফেচ করতে পারে
            query_params = "sEcho=1&iColumns=7&iDisplayStart=0&iDisplayLength=10000&sSearch=&iSortingCols=1&iSortCol_0=0&sSortDir_0=desc"
            divider = "&" if "?" in full_ajax_url else "?"
            full_ajax_url += f"{divider}{query_params}"

        ajax_headers = {
            "Referer": check_url,
            "X-Requested-With": "XMLHttpRequest"
        }
        
        ajax_res = session.get(full_ajax_url, headers=ajax_headers, timeout=15)
        data_dict = ajax_res.json()
        rows = data_dict.get("aaData", [])
        for row_val in rows:
            if not isinstance(row_val, list):
                continue
                
            if len(row_val) < max(n_idx, m_idx) + 1:
                continue
                
            num_val = row_val[n_idx] if (0 <= n_idx < len(row_val)) else row_val[2]
            msg_val = row_val[m_idx] if (0 <= m_idx < len(row_val)) else row_val[4]
            
            clean_num = re.sub(r'\D', '', str(num_val))
            if clean_num and 5 <= len(clean_num) <= 18:
                otp = extract_otp_code(msg_val)
                if otp and len(msg_val) > 4:
                    results.append({"number": clean_num, "message": msg_val, "otp": otp})
                    
    else:
        # ৫.২ ডাইরেক্ট HTML টেবিল থেকে রিড করার ব্যাকআপ লজিক
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if not rows: continue
            
            final_n_idx = n_idx
            final_m_idx = m_idx
            
            header_cells = rows[0].find_all(['th', 'td'])
            for i, cell in enumerate(header_cells):
                c_text = cell.get_text(strip=True).lower()
                if n_col_name in c_text: final_n_idx = i
                if m_col_name in c_text: final_m_idx = i

            for row in rows:
                cols = row.find_all(['td', 'th'])
                if all(c.name == 'th' for c in cols): continue
                
                if len(cols) > max(final_n_idx, final_m_idx):
                    num_text = cols[final_n_idx].get_text(separator=" ", strip=True)
                    msg_text = cols[final_m_idx].get_text(separator=" ", strip=True)
                    
                    clean_num = re.sub(r'\D', '', num_text)
                    if clean_num and 5 <= len(clean_num) <= 18:
                        otp = extract_otp_code(msg_text)
                        if otp and len(msg_text) > 4:
                            results.append({"number": clean_num, "message": msg_text, "otp": otp})
                            
    return results, html_text

# Track active number sessions to expire them automatically
user_active_sessions = {}

assigned_number_rates = {}

# Live range showcase tokens (callback_data must stay <= 64 bytes)
_RANGE_TOKENS = {}
_RANGE_TOKEN_SEQ = 0
_RANGE_POSTED = {}  # key -> last post monotonic time (anti-spam)

def _range_token(service, range_str, source="stex"):
    global _RANGE_TOKEN_SEQ
    full = (str(service or "OTHER").upper(), str(range_str or "").strip(), str(source or "stex"))
    for t, v in list(_RANGE_TOKENS.items())[-200:]:
        if v == full:
            return t
    _RANGE_TOKEN_SEQ = (_RANGE_TOKEN_SEQ + 1) % 1000000
    tok = f"rg{_RANGE_TOKEN_SEQ}"
    _RANGE_TOKENS[tok] = full
    if len(_RANGE_TOKENS) > 4000:
        for k in list(_RANGE_TOKENS.keys())[:1500]:
            _RANGE_TOKENS.pop(k, None)
    return tok

def _range_resolve(tok):
    return _RANGE_TOKENS.get(str(tok or "").strip())

def post_live_range_card(service, range_str, source="stex", hits=0, sms_body=""):
    """Forward a live range card to Range GROUP with Get number button."""
    gid = str(bot_settings.get("range_group_id") or "").strip()
    if not gid or not range_str:
        return False
    rng = str(range_str).replace("X", "").replace("x", "").replace(" ", "").strip()
    if not rng:
        return False
    # anti-spam: same range not more than once per 45s
    key = f"{source}:{str(service).upper()}:{rng}"
    now = time.time()
    if now - _RANGE_POSTED.get(key, 0) < 45:
        return False
    _RANGE_POSTED[key] = now

    digits = re.sub(r"\D", "", rng)
    prefix5 = digits[:5] if len(digits) >= 5 else digits
    char, iso = get_flag_and_code(digits or rng)
    country_name = None
    try:
        country_name = get_flag_info_html(iso, return_full_name=True)
        if isinstance(country_name, str):
            country_name = country_name.title()
    except Exception:
        country_name = None
    if not country_name:
        country_name = iso or "Unknown"
    if sms_body and "instagram" in sms_body.lower():
        service = "INSTAGRAM"

    app_full, prem = get_service_info_html(service, "")
    flag_html = get_flag_info_html(digits or rng) if digits else char
    hits_s = f" | 🔥 {hits}" if hits else ""
    text_msg = render_body_text(
        f"{flag_html} <b>{country_name}</b> | {prem} <b>{app_full}</b>\n"
        f"📊 <code>{prefix5}XXX</code>\n\n"
        f'<b>FULL MESSAGE 💬</b>\n'
        f'<blockquote><code>{sms_body}</code></blockquote>'
    )

    tok = _range_token(service, rng, source)
    src_flag = {"zenex": "znx", "voltx": "vtx", "stex": "stx"}.get(str(source).lower(), "stx")
    # Prefer embedding range digits in callback (works after restart). Token as fallback.
    digits_cb = re.sub(r"\D", "", rng)[:14]
    if digits_cb and len(f"rng_get_{src_flag}_{digits_cb}") <= 64:
        cb = f"rng_get_{src_flag}_{digits_cb}"
    else:
        cb = f"rng_get_{src_flag}_{tok}"[:64]
    # also keep service in token map under digits key
    try:
        _RANGE_TOKENS[f"{src_flag}:{digits_cb}"] = (str(service or "OTHER").upper(), rng, str(source or "stex"))
    except Exception:
        pass
    # Deep link: Get number button opens bot private chat + /start with range payload
    # অ্যাডমিন প্যানেল থেকে লিংক আনা হচ্ছে
    otp_channel_url = bot_settings.get("otp_channel_link") or ""

    if BOT_USERNAME:
        first_row = [
            {"text": "GET NUMBER", "icon_custom_emoji_id": "5303449763406954093",
             "url": f"https://t.me/{BOT_USERNAME}?start={cb}", "style": "success"}
        ]
    else:
        first_row = [
            {"text": "GET NUMBER", "icon_custom_emoji_id": "5303449763406954093",
             "callback_data": cb, "style": "success"}
        ]

    # কিবোর্ড স্ট্রাকচার তৈরি
    keyboard_rows = [first_row]

    # নতুন চ্যানেল বাটন (সবুজ কালার ও প্রিমিয়াম ইমোজি সহ)
    if otp_channel_url:
        keyboard_rows.append([
            {
                "text": "CHANNEL", 
                "icon_custom_emoji_id": "6206508629286196237", # এখানে আপনার প্রিমিয়াম ইমোজি আইডিটি বসান
                "url": otp_channel_url, 
                "style": "primary" # এটি বাটনটিকে সবুজ (success) কালারে দেখাবে
            }
        ])

    kb = {"inline_keyboard": keyboard_rows}

    try:
        send_message(gid, text_msg, reply_markup=kb)
        return True
    except Exception as e:
        print(f"range post err: {e}")
        return False




def load_db():
    global bot_settings, number_batches, used_numbers_list, total_uploaded_stats, total_assigned_stats, recent_traffic, stex_assigned_numbers, voltx_assigned_numbers, zenex_assigned_numbers, processed_otps, assigned_number_rates
    
    loaded_from_local = False
    
    # ১. প্রথমে Local DB লোড করবে (এটি সবচেয়ে আপডেট থাকে, তাই Key হারাবে না)
    if os.path.exists(DB_FILE) or os.path.exists(DB_FILE + ".bak"):
        try:
            if True:
                data = _read_json_with_fallback()
                saved_settings = data.get("bot_settings", {})
                for key, val in saved_settings.items():
                    if key == "custom_messages":
                        for m_key, m_val in val.items():
                            bot_settings["custom_messages"][m_key] = m_val
                    else:
                        bot_settings[key] = val
                        
                for m_key, m_val in DEFAULT_CUSTOM_MESSAGES.items():
                    if m_key not in bot_settings["custom_messages"]:
                        bot_settings["custom_messages"][m_key] = m_val
                        
                number_batches = data.get("number_batches", {})
                used_numbers_list = data.get("used_numbers_list", [])
                total_uploaded_stats = data.get("total_uploaded_stats", 0)
                total_assigned_stats = data.get("total_assigned_stats", 0)
                recent_traffic = data.get("recent_traffic", [])
                stex_assigned_numbers = data.get("stex_assigned_numbers", {})
                voltx_assigned_numbers = data.get("voltx_assigned_numbers", {})
                zenex_assigned_numbers = data.get("zenex_assigned_numbers", {})
                processed_otps = BoundedSet(20000, data.get("processed_otps", []))
                assigned_number_rates = data.get("assigned_number_rates", {})
                loaded_from_local = True
            print("✅ Local Stock/UI DB Loaded Successfully!")
        except Exception as e:
            print(f"❌ Error loading local DB: {e}")

    # ২. এরপর Firestore থেকে শুধু মিসিং ডেটা লোড করবে (Local ডেটা ওভাররাইট করবে না)
    if db:
        try:
            doc = db.collection('settings').document('bot_config').get()
            if doc.exists:
                fs_data = doc.to_dict()
                for k in FS_KEYS:
                    # যদি Local-এ ডেটা না থাকে (ফাঁকা থাকে), তবেই শুধুমাত্র Firestore থেকে আনবে
                    if k in fs_data and (not loaded_from_local or bot_settings.get(k) in [None, [], ""]):
                        bot_settings[k] = fs_data[k]
                print("✅ Config Synced with Firestore!")
            else:
                fs_data = {k: bot_settings[k] for k in FS_KEYS if k in bot_settings}
                db.collection('settings').document('bot_config').set(fs_data)
                print("✅ Firestore Config Initialized!")
        except Exception as e:
            print(f"❌ Error loading from Firestore: {e}")

_save_lock = threading.Lock()
_last_bak = [0.0]

def _save_local_db_now():
    local_data = {
        "bot_settings": bot_settings,
        "number_batches": number_batches,
        "used_numbers_list": used_numbers_list,
        "total_uploaded_stats": total_uploaded_stats,
        "total_assigned_stats": total_assigned_stats,
        "recent_traffic": recent_traffic,
        "stex_assigned_numbers": stex_assigned_numbers,
        "voltx_assigned_numbers": voltx_assigned_numbers,
        "zenex_assigned_numbers": zenex_assigned_numbers,
        "processed_otps": processed_otps.snapshot(),
        "assigned_number_rates": assigned_number_rates
    }
    with _save_lock:
        for attempt in range(3):
            try:
                payload = json.dumps(local_data, separators=(",", ":"))
                break
            except RuntimeError:          # dict changed size while another thread edited it
                time.sleep(0.05)
        else:
            log.error("save_local_db: could not serialise data")
            return
        try:
            tmp = DB_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            if os.path.exists(DB_FILE) and time.time() - _last_bak[0] > 60:
                try:
                    shutil.copyfile(DB_FILE, DB_FILE + ".bak")
                    _last_bak[0] = time.time()
                except OSError:
                    pass
            os.replace(tmp, DB_FILE)      # atomic: file is never half-written
        except Exception:
            log.exception("save_local_db failed")

def _sync_fs():
    if not db: return
    fs_data = {k: bot_settings[k] for k in FS_KEYS if k in bot_settings}
    try:
        db.collection('settings').document('bot_config').set(fs_data)
    except Exception: pass

# ---- Debounced saving: many quick changes => ONE disk write (keeps the bot fast) ----
_save_event = threading.Event()
_flush_lock = threading.Lock()

def _flush_now():
    with _flush_lock:
        _save_local_db_now()
        _sync_fs()

def _save_writer():
    while True:
        _save_event.wait()
        time.sleep(1.0)          # collect everything that changes in the next second
        _save_event.clear()
        try:
            _flush_now()
        except Exception:
            log.exception("background save failed")

def save_local_db():
    _save_event.set()

def save_db():
    _save_event.set()

threading.Thread(target=_save_writer, daemon=True).start()

import atexit, signal

def _graceful_exit(signum=None, frame=None):
    # Render sends SIGTERM on every deploy/restart: write pending data first
    try:
        _flush_now()
    except Exception:
        pass
    os._exit(0)

atexit.register(lambda: _flush_now())
for _sig in (signal.SIGTERM, signal.SIGINT):
    try:
        signal.signal(_sig, _graceful_exit)
    except Exception:
        pass

load_db()

user_states = {}
temp_data = {}
user_cooldowns = {}
pending_withdrawals = {}
try:
    for _w in db.collection('withdrawals').stream():
        _d = _w.to_dict()
        if _d.get("status") == "pending" and _d.get("number"):
            pending_withdrawals[_w.id] = {"user_id": int(_d["user_id"]), "amount": _d["amount"],
                                          "method": _d.get("method", ""), "number": _d["number"],
                                          "full_name": _d.get("full_name", _d["user_id"])}
except Exception as _e:
    print(f"Could not restore pending withdrawals: {_e}")

# ==========================================
# Telegram API & Helpers
# ==========================================
tg_session = requests.Session() # 🌟 Keep-Alive Connection (Makes bot 10x faster)
from requests.adapters import HTTPAdapter
tg_session.mount("https://", HTTPAdapter(pool_connections=20, pool_maxsize=100))

def api_call(method, payload=None):
    url = f"{BASE_URL}/{method}"
    try:
        # 🌟 Added timeout to prevent hanging!
        res = tg_session.post(url, json=payload, timeout=15)
        return res.json()
    except Exception as e:
        return {}

def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    if reply_markup: payload["reply_markup"] = reply_markup
    return api_call("sendMessage", payload)

def send_photo(chat_id, photo_url_or_file_id, caption="", reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "photo": photo_url_or_file_id, "caption": caption, "parse_mode": parse_mode}
    if reply_markup: payload["reply_markup"] = reply_markup
    return api_call("sendPhoto", payload)

def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
    if reply_markup: payload["reply_markup"] = reply_markup
    return api_call("editMessageText", payload)

def delete_message(chat_id, message_id):
    return api_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

def answer_callback(callback_id, text="", show_alert=False):
    api_call("answerCallbackQuery", {"callback_query_id": callback_id, "text": text, "show_alert": show_alert})

def send_document(chat_id, filename, text_content):
    url = f"{BASE_URL}/sendDocument"
    files = {'document': (filename, text_content)}
    data = {'chat_id': chat_id}
    try: requests.post(url, data=data, files=files, timeout=60)
    except Exception: pass

# 🌟 Local User List to completely remove Firebase Read Costs!
all_known_users = set()

def sync_users_list():
    global all_known_users
    try:
        if os.path.exists("users_list.json"):
            with open("users_list.json", "r") as f:
                all_known_users = set(json.load(f))
        if not all_known_users and db:
            for doc in db.collection('users').select([]).stream():
                all_known_users.add(doc.id)
            with open("users_list.json", "w") as f:
                json.dump(list(all_known_users), f)
    except Exception: pass

threading.Thread(target=sync_users_list, daemon=True).start()

def _save_users_list():
    try:
        with open("users_list.json", "w") as f:
            json.dump(list(all_known_users), f)
    except Exception: pass

def register_user_local(uid):
    uid_str = str(uid)
    if uid_str not in all_known_users:
        all_known_users.add(uid_str)
        # 🌟 Non-blocking background save (Prevents lag)
        threading.Thread(target=_save_users_list, daemon=True).start()

def broadcast_copymessage(from_chat_id, msg_id):
    success = 0
    failed = 0
    users = list(all_known_users)
    
    # 🌟 Dedicated Connection Pool for Broadcast (Fixes Port Exhaustion & Network Lag)
    b_session = requests.Session()
    url = f"{BASE_URL}/copyMessage"
    
    for user_id in users:
        payload = {"chat_id": user_id, "from_chat_id": from_chat_id, "message_id": msg_id}
        try:
            res = b_session.post(url, json=payload, timeout=5).json()
            if res.get("ok"): success += 1
            else: failed += 1
        except Exception:
            failed += 1
        time.sleep(0.035) # Safe speed (28 msgs/sec) to prevent Telegram Ban
        
    send_message(from_chat_id, render_body_text(f"📢 <b>Broadcast Completed!</b>\n✅ Success: {success}\n❌ Failed: {failed}\n👥 Total Sent: {len(users)}"))

def render_body_text(text):
    if not text: return str(text)
    parts = re.split(r'(<tg-emoji.*?</tg-emoji>)', str(text))
    for i in range(len(parts)):
        if not parts[i].startswith('<tg-emoji'):
            for normal_emj, prem_id in GLOBAL_BODY_EMOJIS.items():
                if normal_emj in parts[i]:
                    parts[i] = parts[i].replace(normal_emj, f'<tg-emoji emoji-id="{prem_id}">{normal_emj}</tg-emoji>')
    return "".join(parts)

def extract_premium_html(msg):
    text = msg.get("text", msg.get("caption", ""))
    entities = msg.get("entities", msg.get("caption_entities", []))
    if not entities: return text
    try:
        b_text = text.encode('utf-16-le')
        c_entities = [e for e in entities if e.get("type") == "custom_emoji"]
        c_entities.sort(key=lambda x: x["offset"], reverse=True)
        for ent in c_entities:
            offset = ent["offset"] * 2
            length = ent["length"] * 2
            eid = ent["custom_emoji_id"]
            emoji_char = b_text[offset:offset+length].decode('utf-16-le')
            html_tag = f'<tg-emoji emoji-id="{eid}">{emoji_char}</tg-emoji>'
            replacement = html_tag.encode('utf-16-le')
            b_text = b_text[:offset] + replacement + b_text[offset+length:]
        return b_text.decode('utf-16-le')
    except Exception as e:
        return text 

# ==========================================
# Built-in country table (fallback when a country is missing in premium_flags)
# ==========================================
_CC_RAW = """
1:US:United States;7:RU:Russia;76:KZ:Kazakhstan;77:KZ:Kazakhstan;20:EG:Egypt;27:ZA:South Africa;30:GR:Greece;31:NL:Netherlands;
32:BE:Belgium;33:FR:France;34:ES:Spain;36:HU:Hungary;39:IT:Italy;40:RO:Romania;41:CH:Switzerland;43:AT:Austria;44:GB:United Kingdom;
45:DK:Denmark;46:SE:Sweden;47:NO:Norway;48:PL:Poland;49:DE:Germany;51:PE:Peru;52:MX:Mexico;53:CU:Cuba;54:AR:Argentina;55:BR:Brazil;
56:CL:Chile;57:CO:Colombia;58:VE:Venezuela;60:MY:Malaysia;61:AU:Australia;62:ID:Indonesia;63:PH:Philippines;64:NZ:New Zealand;
65:SG:Singapore;66:TH:Thailand;81:JP:Japan;82:KR:South Korea;84:VN:Vietnam;86:CN:China;90:TR:Turkey;91:IN:India;92:PK:Pakistan;
93:AF:Afghanistan;94:LK:Sri Lanka;95:MM:Myanmar;98:IR:Iran;211:SS:South Sudan;212:MA:Morocco;213:DZ:Algeria;216:TN:Tunisia;
218:LY:Libya;220:GM:Gambia;221:SN:Senegal;222:MR:Mauritania;223:ML:Mali;224:GN:Guinea;225:CI:Ivory Coast;226:BF:Burkina Faso;
227:NE:Niger;228:TG:Togo;229:BJ:Benin;230:MU:Mauritius;231:LR:Liberia;232:SL:Sierra Leone;233:GH:Ghana;234:NG:Nigeria;235:TD:Chad;
236:CF:Central African Republic;237:CM:Cameroon;238:CV:Cape Verde;239:ST:Sao Tome;240:GQ:Equatorial Guinea;241:GA:Gabon;
242:CG:Congo Brazzaville;243:CD:Congo Kinshasa;244:AO:Angola;245:GW:Guinea Bissau;246:IO:British Indian Ocean;248:SC:Seychelles;
249:SD:Sudan;250:RW:Rwanda;251:ET:Ethiopia;252:SO:Somalia;253:DJ:Djibouti;254:KE:Kenya;255:TZ:Tanzania;256:UG:Uganda;257:BI:Burundi;
258:MZ:Mozambique;260:ZM:Zambia;261:MG:Madagascar;262:RE:Reunion;263:ZW:Zimbabwe;264:NA:Namibia;265:MW:Malawi;266:LS:Lesotho;
267:BW:Botswana;268:SZ:Eswatini;269:KM:Comoros;290:SH:Saint Helena;291:ER:Eritrea;297:AW:Aruba;298:FO:Faroe Islands;299:GL:Greenland;
350:GI:Gibraltar;351:PT:Portugal;352:LU:Luxembourg;353:IE:Ireland;354:IS:Iceland;355:AL:Albania;356:MT:Malta;357:CY:Cyprus;
358:FI:Finland;359:BG:Bulgaria;370:LT:Lithuania;371:LV:Latvia;372:EE:Estonia;373:MD:Moldova;374:AM:Armenia;375:BY:Belarus;
376:AD:Andorra;377:MC:Monaco;378:SM:San Marino;380:UA:Ukraine;381:RS:Serbia;382:ME:Montenegro;383:XK:Kosovo;385:HR:Croatia;
386:SI:Slovenia;387:BA:Bosnia Herzegovina;389:MK:North Macedonia;420:CZ:Czech Republic;421:SK:Slovakia;423:LI:Liechtenstein;
500:FK:Falkland Islands;501:BZ:Belize;502:GT:Guatemala;503:SV:El Salvador;504:HN:Honduras;505:NI:Nicaragua;506:CR:Costa Rica;
507:PA:Panama;508:PM:Saint Pierre;509:HT:Haiti;590:GP:Guadeloupe;591:BO:Bolivia;592:GY:Guyana;593:EC:Ecuador;594:GF:French Guiana;
595:PY:Paraguay;596:MQ:Martinique;597:SR:Suriname;598:UY:Uruguay;599:CW:Curacao;670:TL:East Timor;672:NF:Norfolk Island;
673:BN:Brunei;674:NR:Nauru;675:PG:Papua New Guinea;676:TO:Tonga;677:SB:Solomon Islands;678:VU:Vanuatu;679:FJ:Fiji;680:PW:Palau;
681:WF:Wallis Futuna;682:CK:Cook Islands;683:NU:Niue;685:WS:Samoa;686:KI:Kiribati;687:NC:New Caledonia;688:TV:Tuvalu;
689:PF:French Polynesia;690:TK:Tokelau;691:FM:Micronesia;692:MH:Marshall Islands;850:KP:North Korea;852:HK:Hong Kong;853:MO:Macau;
855:KH:Cambodia;856:LA:Laos;880:BD:Bangladesh;886:TW:Taiwan;960:MV:Maldives;961:LB:Lebanon;962:JO:Jordan;963:SY:Syria;964:IQ:Iraq;
965:KW:Kuwait;966:SA:Saudi Arabia;967:YE:Yemen;968:OM:Oman;970:PS:Palestine;971:AE:United Arab Emirates;972:IL:Israel;973:BH:Bahrain;
974:QA:Qatar;975:BT:Bhutan;976:MN:Mongolia;977:NP:Nepal;992:TJ:Tajikistan;993:TM:Turkmenistan;994:AZ:Azerbaijan;995:GE:Georgia;
996:KG:Kyrgyzstan;998:UZ:Uzbekistan;
1242:BS:Bahamas;1246:BB:Barbados;1264:AI:Anguilla;1268:AG:Antigua Barbuda;1284:VG:British Virgin Islands;1340:VI:US Virgin Islands;
1345:KY:Cayman Islands;1441:BM:Bermuda;1473:GD:Grenada;1649:TC:Turks Caicos;1658:JM:Jamaica;1664:MS:Montserrat;1670:MP:Northern Mariana;
1671:GU:Guam;1684:AS:American Samoa;1721:SX:Sint Maarten;1758:LC:Saint Lucia;1767:DM:Dominica;1784:VC:Saint Vincent;1787:PR:Puerto Rico;
1809:DO:Dominican Republic;1829:DO:Dominican Republic;1849:DO:Dominican Republic;1868:TT:Trinidad Tobago;1869:KN:Saint Kitts Nevis;
1876:JM:Jamaica;1939:PR:Puerto Rico
"""
_CC_MAP = {}
_ISO_NAME = {}
for _it in _CC_RAW.replace("\n", "").split(";"):
    _p = _it.strip().split(":")
    if len(_p) == 3:
        _CC_MAP[_p[0]] = (_p[1], _p[2])
        _ISO_NAME.setdefault(_p[1], _p[2])
for _ac in "204 226 236 249 250 289 306 343 365 367 403 416 418 431 437 438 450 506 514 519 548 579 581 587 604 613 639 647 672 705 709 742 778 780 782 807 819 825 867 873 902 905".split():
    _CC_MAP["1" + _ac] = ("CA", "Canada")
_ISO_NAME["CA"] = "Canada"


def _iso_flag_char(iso):
    iso = str(iso or "").upper()
    if len(iso) == 2 and iso.isalpha():
        return "".join(chr(0x1F1E6 + ord(c) - 65) for c in iso)
    return "\U0001F30D"


def builtin_country_from_num(num):
    """(iso, name) for any international number (longest calling-code match) or None."""
    clean = re.sub(r"\D", "", str(num))
    for ln in (4, 3, 2, 1):
        hit = _CC_MAP.get(clean[:ln])
        if hit and len(clean) >= ln:
            return hit
    return None


def country_label(num_or_iso):
    """Country NAME for a number (or 2-letter ISO) - admin premium_flags first, built-in table as fallback."""
    try:
        s = str(num_or_iso or "")
        name = get_flag_info_html(s, return_full_name=True)
        if name and name != s and not re.fullmatch(r"\+?\d[\d ]*", str(name)):
            return html.escape(str(name).title() if len(str(name)) > 3 else str(name))
        if len(s) == 2:
            return html.escape(s.upper())
    except Exception:
        pass
    return "Unknown"


def get_flag_info_from_num(num):
    clean = num.replace("+", "").replace(" ", "")
    sorted_codes = sorted(bot_settings.get("premium_flags", {}).keys(), key=len, reverse=True)
    for code in sorted_codes:
        if clean.startswith(code):
            data = bot_settings["premium_flags"][code]
            return data["char"], data.get("iso", "XX"), data.get("id")
    b = builtin_country_from_num(clean)
    if b:
        return _iso_flag_char(b[0]), b[0], None
    return "🌍", "XX", None

def get_flag_and_code(num):
    char, iso, _ = get_flag_info_from_num(num)
    return char, iso

def get_flag_info_html(num_or_iso, return_full_name=False):
    if len(num_or_iso) == 2:
        for code, data in bot_settings.get("premium_flags", {}).items():
            if data.get("iso") == num_or_iso:
                eid = data.get("id")
                char = data.get("char")
                name = data.get("name", num_or_iso)
                if return_full_name: return name
                if eid: return f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>'
                return char
        if return_full_name: return _ISO_NAME.get(num_or_iso.upper(), num_or_iso)
        return _iso_flag_char(num_or_iso) if num_or_iso.upper() in _ISO_NAME else "🌍"
        
    char, _, eid = get_flag_info_from_num(num_or_iso)
    if return_full_name:
        for code, data in bot_settings.get("premium_flags", {}).items():
            clean = num_or_iso.replace("+", "").replace(" ", "")
            if clean.startswith(code): return data.get("name", num_or_iso)
        _b = builtin_country_from_num(num_or_iso)
        return _b[1] if _b else num_or_iso
        
    if eid:
        return f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>'
    return char

_ITU_CODES = set("""
1 7 20 27 30 31 32 33 34 36 39 40 41 43 44 45 46 47 48 49 51 52 53 54 55 56 57 58 60 61 62 63 64 65 66
81 82 84 86 90 91 92 93 94 95 98
211 212 213 216 218 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239
240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 260 261 262 263 264 265
266 267 268 269 290 291 297 298 299
350 351 352 353 354 355 356 357 358 359 370 371 372 373 374 375 376 377 378 379 380 381 382 383 385 386 387 389
420 421 423 500 501 502 503 504 505 506 507 508 509 590 591 592 593 594 595 596 597 598 599
670 672 673 674 675 676 677 678 679 680 681 682 683 685 686 687 688 689 690 691 692
800 808 850 852 853 855 856 870 878 880 881 882 883 886 888
960 961 962 963 964 965 966 967 968 970 971 972 973 974 975 976 977 979 991 992 993 994 995 996 998
""".split())

# +1 countries with their own area code (Caribbean / Pacific). US & Canada keep only "1" stripped.
_NANP_AREA = set("""
242 246 264 268 284 340 345 441 473 649 658 664 670 671 684 721 758 767 784 787 809 829 849 868 869 876 939
""".split())

def strip_cc(num):
    """Return the number without its country code (all world calling codes). Unknown -> unchanged."""
    raw = str(num)
    clean = raw.replace("+", "").replace(" ", "")
    if not clean.isdigit():
        return raw
    # 1) Caribbean/Pacific +1 countries: remove "1" + 3-digit area code (e.g. 1876 Jamaica)
    if clean.startswith("1") and clean[1:4] in _NANP_AREA and len(clean) > 8:
        return clean[4:]
    # 2) codes the admin added as flags
    for code in sorted(bot_settings.get("premium_flags", {}).keys(), key=len, reverse=True):
        if clean.startswith(code) and len(clean) > len(code):
            return clean[len(code):]
    # 3) built-in world list (calling codes are prefix-free, so the first match is right)
    for n in (1, 2, 3):
        if clean[:n] in _ITU_CODES and len(clean) > n + 4:
            return clean[n:]
    return raw

def mask_number(num):
    clean = num.replace("+", "").replace(" ", "")
    if len(clean) > 6: return f"{clean[:3]}ZIHAN{clean[-3:]}"
    elif len(clean) > 2: return f"{clean[:1]}ZIHAN{clean[-1:]}"
    return clean

def _norm_num(n):
    """Normalize phone to pure digits only."""
    return re.sub(r"\D", "", str(n or ""))

def numbers_match(a, b):
    """Robust match: exact or one ends with the other (handles country code / national format differences)."""
    a = _norm_num(a)
    b = _norm_num(b)
    if not a or not b:
        return False
    if a == b:
        return True
    # Common case: full intl vs national / no-plus (different length by 1-4 digits)
    if len(a) >= 7 and len(b) >= 7:
        for k in (10, 9, 8, 7):
            if a.endswith(b[-k:]) or b.endswith(a[-k:]):
                return True
    return False

def best_sources_for(range_str="", service=""):
    """Return [stex,voltx,zenex] ordered by recent traffic (high first). Only panels with keys."""
    counts = {"stex": 0, "voltx": 0, "zenex": 0}
    now = time.time()
    rng = _norm_num(range_str)[:8]
    srv = str(service or "").upper()
    for t in recent_traffic:
        if now - t.get("time", 0) > 180:
            continue
        src = str(t.get("source") or "stex").lower()
        if src not in counts:
            src = "stex"
        rr = _norm_num(t.get("real_range") or "")
        hit = False
        if rng and rr and (rr.startswith(rng[:5]) or rng.startswith(rr[:5])):
            hit = True
        elif srv and str(t.get("service") or "").upper() == srv:
            hit = True
        if hit:
            counts[src] += 1
    order = sorted(counts.keys(), key=lambda s: counts[s], reverse=True)
    # keep only panels that actually have API keys
    key_map = {"stex": "stex_keys", "voltx": "voltx_keys", "zenex": "zenex_keys"}
    return [s for s in order if bot_settings.get(key_map[s])]

LANG_MAP = {
    "#EN": "English", "#BN": "Bengali", "#AR": "Arabic", "#HI": "Hindi", 
    "#PA": "Punjabi", "#GU": "Gujarati", "#OR": "Odia", "#TA": "Tamil", 
    "#TE": "Telugu", "#KN": "Kannada", "#ML": "Malayalam", "#SI": "Sinhala", 
    "#TH": "Thai", "#LO": "Lao", "#BO": "Tibetan", "#MY": "Burmese", 
    "#AM": "Amharic", "#KM": "Khmer", "#KA": "Georgian", "#HY": "Armenian", 
    "#HE": "Hebrew", "#EL": "Greek", "#RU": "Russian", "#ZH": "Chinese", 
    "#JA": "Japanese", "#KO": "Korean", "#ID": "Indonesian", "#MS": "Malay", 
    "#VN": "Vietnamese", "#TL": "Filipino", "#ES": "Spanish", "#PT": "Portuguese", 
    "#FR": "French", "#DE": "German", "#IT": "Italian", "#PL": "Polish", 
    "#TR": "Turkish", "#NL": "Dutch", "#SV": "Swedish", "#DA": "Danish", 
    "#NO": "Norwegian", "#FI": "Finnish", "#CS": "Czech", "#SK": "Slovak", 
    "#HU": "Hungarian", "#RO": "Romanian", "#HR": "Croatian", "#BG": "Bulgarian", 
    "#UK": "Ukrainian", "#SW": "Swahili", "#AF": "Afrikaans"
}

# ==========================================
# 🌟 ADVANCED SERVICE & LANGUAGE DETECTION
# ==========================================

SERVICE_SMS_KEYWORDS = {
    # 🟢 Social Media & Chat (Added Arabic Keywords)
    "whatsapp": ["whatsapp", "whatsa", "whatsap", "whats", "whatsapp business", "whatsapp me", "whatsapp code", "whatsap", "واتساب", "واتساپ", "واٹس ایپ", "व्हाट्सएप", "वाट्सएप", "वॉट्सऐप", "व्हाट्सप्प", "হোয়াটসঅ্যাপ", "হোটসঅ্যাপ", "ватсап", "уотсап", "вотсап", "ватс апп", "వాట్సాప్", "വാട്‌സ്ആപ്പ്", "வாட்ஸ்அப்", "ವಾಟ್ಸಾಪ್", "વોટ્સએપ", "ਵਟਸਐਪ", "ହ୍ଵାଟସ୍ ଆପ୍", "වට්ස්ඇප්", "วอตส์แอปป์", "วอทส์แอพ", "ဝက်စ်အက်ပ်", "វ៉តសាប់", "ວອດແອັບ", "ワッツアップ", "왓츠앱", "whatsapp的", "whatsapp验证码", "וואטסאפ", "γουάτσαπ", "ዋትስአፕ", "ვოთსאფი", "վոթսափ"],
    "facebook": ["facebook", "fb", "meta", "fbook", "fb code", "facebook code", "فيسبوك", "فيس بوك"],
    "instagram": ["instagram", "insta", "ig", "ig code", "instagram code", "انستغرام", "انستقرام"],
    "telegram": ["telegram", "tg", "tele", "telegram code", "tg code", "t.me", "تيليجرام", "تليجرام"],
    "tiktok": ["tiktok", "tik tok", "tikvideo", "tiktok code", "tik code", "تيك توك"],
    "snapchat": ["snapchat", "snap", "snap code", "سناب شات"],
    "twitter": ["twitter", "x.com", "x code", "twitter code", "تويتر"],
    "discord": ["discord", "discord code", "ديسكورد"],
    "viber": ["viber", "viber code", "فايبر"],
    "line": ["line", "line code", "line verification", "لاين"],
    "wechat": ["wechat", "we chat", "wechat code", "وي تشات"],
    "signal": ["signal", "signal code", "سيجنال"],
    "linkedin": ["linkedin", "linked in", "لينكد إن"],
    "imo": ["imo", "imo code", "imo verification", "ايمو"],
    "kakaotalk": ["kakao", "kakaotalk", "كاكاو"],
    "qq": ["qq", "tencent qq"],
    "vk": ["vk", "vkontakte"],

    # 🔵 Tech & Mail
    "google": ["google", "gmail", "youtube", "g-", "google voice", "جوجل", "غوغل"],
    "microsoft": ["microsoft", "ms", "outlook", "live.com", "hotmail"],
    "apple": ["apple", "icloud", "itunes", "apple id"],
    "yahoo": ["yahoo", "yahoo code", "ymail"],
    "protonmail": ["proton", "protonmail"],
    
    # 💰 Crypto & Trading
    "binance": ["binance", "bnb", "binances"],
    "coinbase": ["coinbase"],
    "okx": ["okx", "okex"],
    "kucoin": ["kucoin"],
    "bybit": ["bybit"],
    "huobi": ["huobi", "htx"],
    "mexc": ["mexc"],
    "trustwallet": ["trust wallet", "trustwallet"],

    # 💳 Finance & Wallets
    "bkash": ["bkash", "b-kash", "bkash code"],
    "nagad": ["nagad", "nagad code"],
    "rocket": ["rocket", "dutch bangla"],
    "upay": ["upay", "upay code"],
    "paypal": ["paypal", "pay pal"],
    "paytm": ["paytm"],
    "cashapp": ["cash app", "cashapp"],
    "wise": ["wise", "transferwise"],

    # 🛒 E-commerce & Delivery
    "amazon": ["amazon", "amzn", "amazon code"],
    "ebay": ["ebay"],
    "aliexpress": ["aliexpress", "ali express"],
    "alibaba": ["alibaba"],
    "daraz": ["daraz", "daraz code"],
    "foodpanda": ["foodpanda", "food panda"],
    "uber": ["uber", "uber code", "uber verification", "uber eats"],
    "pathao": ["pathao", "pathao ride"],

    # 🎮 Gaming & Entertainment
    "netflix": ["netflix", "netflix code"],
    "spotify": ["spotify", "spotify code"],
    "steam": ["steam", "steam guard"],
    "epicgames": ["epic games", "epicgames"],
    "roblox": ["roblox", "roblox code"],
    "riotgames": ["riot", "riot games", "valorant", "league of legends"],
    "garena": ["garena", "free fire", "freefire"],
    "playstation": ["playstation", "psn"],

    # 🎲 Betting & Casino
    "1xbet": ["1xbet", "1x bet"],
    "melbet": ["melbet", "melbet code"],
    "linebet": ["linebet"],
    "bet365": ["bet365"],
    "megapari": ["megapari"],

    # ❤️ Dating
    "tinder": ["tinder", "tinder code"],
    "bumble": ["bumble"],
    "badoo": ["badoo"]
}

def detect_service(text):
    text_lower = str(text).lower()
    for service_key, keywords in SERVICE_SMS_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                return service_key.upper()
    return None

def get_service_info_html(service_text, msg_text=""):
    s = str(service_text).upper().strip()
    m = str(msg_text).lower().strip()
    apps = bot_settings.get("premium_apps", {})
    
    detected_service = s
    if m:
        for service_key, keywords in SERVICE_SMS_KEYWORDS.items():
            for kw in keywords:
                if kw in m:
                    detected_service = service_key.upper()
                    break
            if detected_service != s: break

    clean_s = re.sub(r'[^\w\s]', '', detected_service).strip()
    
    for app_name, data in apps.items():
        if app_name == detected_service or app_name == clean_s or app_name in detected_service or detected_service in app_name:
            full_name = data.get("name", app_name.title())
            char = data.get("char", "📱")
            eid = data.get("id")
            if eid: return full_name, f'<tg-emoji emoji-id="{eid}">{char}</tg-emoji>'
            return full_name, char
            
    if len(detected_service) > 20:
        return "Message", "💬"
        
    return detected_service.title(), "📱"

def detect_language(text):
    if not text: return "#EN"
    text_str = str(text)

    # ১. Unicode Block দিয়ে নিখুঁত বর্ণমালা শনাক্তকরণ (100% Accurate for scripts)
    if any('\u0600' <= c <= '\u06ff' for c in text_str): return "#AR" # Arabic / Persian / Urdu
    if any('\u0980' <= c <= '\u09ff' for c in text_str): return "#BN" # Bengali
    if any('\u0900' <= c <= '\u097f' for c in text_str): return "#HI" # Hindi / Marathi / Nepali
    if any('\u0a00' <= c <= '\u0a7f' for c in text_str): return "#PA" # Punjabi (Gurmukhi)
    if any('\u0a80' <= c <= '\u0aff' for c in text_str): return "#GU" # Gujarati
    if any('\u0b00' <= c <= '\u0b7f' for c in text_str): return "#OR" # Odia
    if any('\u0b80' <= c <= '\u0bff' for c in text_str): return "#TA" # Tamil
    if any('\u0c00' <= c <= '\u0c7f' for c in text_str): return "#TE" # Telugu
    if any('\u0c80' <= c <= '\u0cff' for c in text_str): return "#KN" # Kannada
    if any('\u0d00' <= c <= '\u0d7f' for c in text_str): return "#ML" # Malayalam
    if any('\u0d80' <= c <= '\u0dff' for c in text_str): return "#SI" # Sinhala
    if any('\u0e00' <= c <= '\u0e7f' for c in text_str): return "#TH" # Thai
    if any('\u0e80' <= c <= '\u0eff' for c in text_str): return "#LO" # Lao
    if any('\u0f00' <= c <= '\u0fff' for c in text_str): return "#BO" # Tibetan
    if any('\u1000' <= c <= '\u109f' for c in text_str): return "#MY" # Burmese (Myanmar)
    if any('\u1200' <= c <= '\u137f' for c in text_str): return "#AM" # Amharic (Ethiopic)
    if any('\u1780' <= c <= '\u17ff' for c in text_str): return "#KM" # Khmer
    if any('\u10a0' <= c <= '\u10ff' for c in text_str): return "#KA" # Georgian
    if any('\u0530' <= c <= '\u058f' for c in text_str): return "#HY" # Armenian
    if any('\u0590' <= c <= '\u05ff' for c in text_str): return "#HE" # Hebrew
    if any('\u0370' <= c <= '\u03ff' for c in text_str): return "#EL" # Greek
    if any('\u0400' <= c <= '\u04ff' for c in text_str): return "#RU" # Russian / Ukrainian (Cyrillic)
    if any('\u4e00' <= c <= '\u9fff' for c in text_str): return "#ZH" # Chinese
    if any('\u3040' <= c <= '\u309f' or '\u30a0' <= c <= '\u30ff' for c in text_str): return "#JA" # Japanese
    if any('\uac00' <= c <= '\ud7af' for c in text_str): return "#KO" # Korean

    # ২. OTP Keyword দিয়ে ভাষা শনাক্তকরণ (Latin script languages)
    text_lower = text_str.lower()
    
    # Asian / Pacific
    if any(w in text_lower for w in ["kode verifikasi", "jangan bagikan", "rahasia"]): return "#ID" # Indonesian
    if any(w in text_lower for w in ["kod pengesahan", "jangan kongsi"]): return "#MS" # Malay
    if any(w in text_lower for w in ["mã của bạn", "không chia sẻ", "mã xác minh"]): return "#VN" # Vietnamese
    if any(w in text_lower for w in ["ang iyong code", "huwag ibahagi"]): return "#TL" # Tagalog / Filipino
    
    # European / Americas
    if any(w in text_lower for w in ["código", "tu código", "verificación", "no compartas"]): return "#ES" # Spanish
    if any(w in text_lower for w in ["seu código", "código de verificação", "não compartilhe"]): return "#PT" # Portuguese
    if any(w in text_lower for w in ["code secret", "ne partagez pas", "votre code"]): return "#FR" # French
    if any(w in text_lower for w in ["dein code", "bestätigungscode", "nicht teilen"]): return "#DE" # German
    if any(w in text_lower for w in ["il tuo codice", "codice di verifica", "non condividere"]): return "#IT" # Italian
    if any(w in text_lower for w in ["twój kod", "nie udostępniaj", "kod weryfikacyjny"]): return "#PL" # Polish
    if any(w in text_lower for w in ["doğrulama kodu", "paylaşmayın", "onay kodu"]): return "#TR" # Turkish
    if any(w in text_lower for w in ["jouw code", "verificatiecode", "niet delen"]): return "#NL" # Dutch
    if any(w in text_lower for w in ["din kod", "verifieringskod", "dela inte"]): return "#SV" # Swedish
    if any(w in text_lower for w in ["bekræftelseskode", "del ikke"]): return "#DA" # Danish
    if any(w in text_lower for w in ["bekreftelseskode", "ikke del"]): return "#NO" # Norwegian
    if any(w in text_lower for w in ["vahvistuskoodi", "älä jaa"]): return "#FI" # Finnish
    if any(w in text_lower for w in ["váš kód", "ověřovací kód", "nesdílejte"]): return "#CS" # Czech
    if any(w in text_lower for w in ["overovací kód", "nezdieľajte"]): return "#SK" # Slovak
    if any(w in text_lower for w in ["ellenőrző kód", "ne oszd meg"]): return "#HU" # Hungarian
    if any(w in text_lower for w in ["codul tău", "codul de verificare", "nu partaja"]): return "#RO" # Romanian
    if any(w in text_lower for w in ["kontrolni kod", "kod za potvrdu", "ne delite"]): return "#HR" # Croatian/Serbian
    if any(w in text_lower for w in ["код за потвърждение", "не споделяйте"]): return "#BG" # Bulgarian
    if any(w in text_lower for w in ["ваш код", "код підтвердження"]): return "#UK" # Ukrainian
    
    # African
    if any(w in text_lower for w in ["msimbo wako", "usishiriki"]): return "#SW" # Swahili
    if any(w in text_lower for w in ["verifikasiekode", "moenie deel nie"]): return "#AF" # Afrikaans
    
    # ৩. উপরের কোনোটি না মিললে ডিফল্ট
    return "#EN"

def parse_chat_id(text):
    text = text.strip()
    if text.startswith("-100") or (text.startswith("-") and text[1:].isdigit()):
        return text
    if "t.me/" in text:
        parts = text.split("/")
        username = parts[-1]
        if username: return "@" + username if not username.startswith("@") else username
    if text.startswith("@"):
        return text
    return "@" + text

def is_admin(user_id):
    return user_id in bot_settings["admins"] or user_id == OWNER_ID

def check_force_join(user_id):
    if not bot_settings["fj_on"] or not bot_settings["fj_channels"]: return True
    if is_admin(user_id): return True
    for ch in bot_settings["fj_channels"]:
        res = api_call("getChatMember", {"chat_id": ch, "user_id": user_id})
        if res.get("ok") and res["result"]["status"] not in ["left", "kicked"]: continue
        else: return False
    return True

def send_force_join_msg(chat_id):
    kb = []
    for ch in bot_settings["fj_channels"]:
        url = f"https://t.me/{ch.replace('@', '')}" if ch.startswith("@") else ch
        kb.append([{"text": f"Join Channel", "icon_custom_emoji_id": "5789428375261023681", "url": url, "style": "primary"}])
    kb.append([{"text": "Check Joined", "icon_custom_emoji_id": "5352694861990501856", "callback_data": "check_fj", "style": "success"}])
    send_message(chat_id, render_body_text(f"{PEM['warn']} <b>Please join our channels to use the bot!</b>"), reply_markup={"inline_keyboard": kb})

def is_user_banned(user_id):
    if is_admin(user_id): return False
    if user_id in user_banned_cache and time.time() - user_banned_cache[user_id]['time'] < 60:
        return user_banned_cache[user_id]['banned']
    banned = False
    if db:
        try:
            doc = db.collection('users').document(str(user_id)).get()
            banned = doc.exists and doc.to_dict().get("banned", False)
        except Exception: pass
    user_banned_cache[user_id] = {'banned': banned, 'time': time.time()}
    return banned

# ==========================================
# Captcha Auto Login & Parsing Core
# ==========================================
def extract_otp_code(text):
    clean_text = re.sub(r'[\u200B-\u200D\uFEFF]', '', str(text))

    # 1. Multi-part OTPs (e.g. 123-456 or 809-761)
    multi_part = re.search(r'(\d{3}[-\s]+\d{3})|(\d{2}[-\s]+\d{2}[-\s]+\d{2})', clean_text)
    if multi_part:
        # হাইফেন (-) থাকলে সেটা রেখে দিবে, কিন্তু স্পেস থাকলে মুছে একসাথে করে দিবে
        return multi_part.group(0).replace(" ", "")

    # 2. Keyword-based extraction
    otp_keywords = ['code', 'is', 'otp', 'pin', 'verification', 'auth', 'কোড', 'رمز', 'your code']
    keywords_pattern = '|'.join(otp_keywords)
    keyword_match = re.search(rf'(?:{keywords_pattern})\s*(?:is|:|-|=)?\s*([a-z0-9]{{4,10}})', clean_text, re.I)
    if keyword_match and keyword_match.group(1).isdigit():
        return keyword_match.group(1)
        
    keyword_match_rev = re.search(rf'([a-z0-9]{{4,10}})\s*(?:is your|is the|কোড)', clean_text, re.I)
    if keyword_match_rev and keyword_match_rev.group(1).isdigit():
        return keyword_match_rev.group(1)

    # 3. Google OTP
    g_match = re.search(r'G-(\d{6})', clean_text, re.IGNORECASE)
    if g_match: return g_match.group(1)

    # 4. Digit sequences fallback
    digit_matches = re.findall(r'(?<!\d)\d{4,8}(?!\d)', clean_text)
    if digit_matches: return digit_matches[0]

    return None

def parse_panel_response(response_text, p_config=None):
    results = []
    p_type = p_config.get("type", "API Panel") if p_config else "API Panel"
    
    n_col_name = p_config.get("num_col_name", "number").lower() if p_config else "number"
    m_col_name = p_config.get("msg_col_name", "message").lower() if p_config else "message"
    n_idx = int(p_config.get("num_col_idx", 1)) - 1 if p_config and p_config.get("num_col_idx") else 1
    m_idx = int(p_config.get("msg_col_idx", 2)) - 1 if p_config and p_config.get("msg_col_idx") else 2

    if p_type == "Auto Captcha Panel":
        try:
            soup = BeautifulSoup(response_text, 'html.parser')
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if not rows: continue
                
                # 🌟 Option 1 + Smart HTML Detection: কলামের নাম ও ব্যবহারকারীর দেওয়া সিরিয়াল দিয়ে সঠিক পজিশন বের করা
                final_n_idx = n_idx
                final_m_idx = m_idx
                
                # প্রথম রো (Header) চেক করে কলামের আসল সিরিয়াল মিলিয়ে নেওয়া
                header_cells = rows[0].find_all(['th', 'td'])
                for i, cell in enumerate(header_cells):
                    c_text = cell.get_text(strip=True).lower()
                    if n_col_name in c_text: final_n_idx = i
                    if m_col_name in c_text: final_m_idx = i

                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    
                    # হেডার রো (যেখানে সব th থাকে) সেগুলো থেকে ডাটা নিবে না
                    if all(c.name == 'th' for c in cols): continue
                    
                    if len(cols) > max(final_n_idx, final_m_idx):
                        # HTML টেবিল থেকে টেক্সট বের করা
                        num_text = cols[final_n_idx].get_text(separator=" ", strip=True)
                        msg_text = cols[final_m_idx].get_text(separator=" ", strip=True)
                        
                        clean_num = re.sub(r'\D', '', num_text)
                        
                        # নাম্বারটা আসলেই ৫-১৮ ডিজিটের কিনা তা নিশ্চিত করা (যাতে উল্টাপাল্টা টেক্সট না আসে)
                        if clean_num and 5 <= len(clean_num) <= 18:
                            otp = extract_otp_code(msg_text)
                            if otp and len(msg_text) > 4:
                                results.append({"number": clean_num, "message": msg_text, "otp": otp})
        except Exception as e:
            pass
    else:
        try:
            data = json.loads(response_text)
            temp_results = []
            
            def process_item(item):
                pot_nums_list = []
                pot_msg = None
                values = []
                
                if isinstance(item, dict):
                    # ১. প্রথমে পরিচিত JSON Key (যেমন: num, phone, sms) দিয়ে খোঁজার চেষ্টা
                    lower_keys = {str(k).lower(): v for k, v in item.items()}
                    for k in ["number", "num", "phone", "msisdn", "sender"]:
                        if k in lower_keys:
                            clean_val = re.sub(r'\D', '', str(lower_keys[k]))
                            if 5 <= len(clean_val) <= 18:
                                if clean_val not in pot_nums_list: pot_nums_list.append(clean_val)
                    for k in ["message", "msg", "sms", "content", "text"]:
                        if k in lower_keys:
                            val = str(lower_keys[k])
                            if len(val) > 4:
                                pot_msg = val
                                break
                    values = list(item.values())
                elif isinstance(item, list):
                    values = item

                # ২. যদি Key দিয়ে না পাওয়া যায়, তবে Smart Blind Scan (সব ভ্যালু চেক করবে)
                for v in values:
                    if isinstance(v, (dict, list)) or v is None: continue
                    v_str = str(v).strip()
                    
                    # Number Detection: 7 থেকে 18 ডিজিট
                    clean_v = re.sub(r'\D', '', v_str)
                    if 7 <= len(clean_v) <= 18 and not re.search(r'[a-zA-Z]', v_str):
                        # Date/Time/IP এড়ানোর লজিক
                        if not re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', v_str) and not re.search(r'\d{2}:\d{2}:\d{2}', v_str) and "." not in v_str:
                            if clean_v not in pot_nums_list:
                                pot_nums_list.append(clean_v)
                    
                    # Message Detection: 5 অক্ষরের বেশি এবং শুধু সংখ্যা নয়
                    if len(v_str) > 4 and not v_str.isdigit():
                        if extract_otp_code(v_str):
                            if pot_msg is None or len(v_str) > len(pot_msg):
                                pot_msg = v_str
                                
                # 🌟 ৩. Multiple Numbers Logic (User Priority > Second Number > First Number)
                pot_num = None
                if pot_nums_list:
                    matched_user_num = None
                    for n in pot_nums_list:
                        # চেক করবে ইউজারের অ্যাসাইন করা নাম্বারের তালিকায় এই নাম্বারটি আছে কি না
                        if n in stex_assigned_numbers or any(n in str(key) for key in stex_assigned_numbers.keys()):
                            matched_user_num = n
                            break
                    
                    if matched_user_num:
                        pot_num = matched_user_num
                    elif len(pot_nums_list) >= 2:
                        pot_num = pot_nums_list[1] # ইউজারের কাছে না থাকলে সরাসরি দ্বিতীয় নাম্বারটি নেবে
                    else:
                        pot_num = pot_nums_list[0]
                            
                if pot_num and pot_msg:
                    otp = extract_otp_code(pot_msg)
                    if otp:
                        temp_results.append({"number": pot_num, "message": pot_msg, "otp": otp})
                        
            def traverse_json(node):
                if isinstance(node, list):
                    if len(node) > 0 and not isinstance(node[0], (dict, list)):
                        # It's a flat list representing one record
                        process_item(node)
                    for child in node:
                        if isinstance(child, (dict, list)):
                            traverse_json(child)
                elif isinstance(node, dict):
                    process_item(node)
                    for val in node.values():
                        if isinstance(val, (dict, list)):
                            traverse_json(val)

            traverse_json(data)
            
            # Remove duplicates
            seen = set()
            for r in temp_results:
                uid = f"{r['number']}_{r['otp']}"
                if uid not in seen:
                    seen.add(uid)
                    results.append(r)
        except Exception: pass
        
    return results

# 🌟 Advanced Automated Background Captcha Solver 🌟
def attempt_auto_login(p, idx):
    login_url = p.get("login_url", "").strip()
    if not login_url.startswith("http"):
        login_url = "http://" + login_url
        
    if not login_url.lower().endswith('/login') and not login_url.lower().endswith('.php'):
        login_url = f"{login_url.rstrip('/')}/login"
        
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    })
    
    try:
        res = session.get(login_url, timeout=15)
        soup = BeautifulSoup(res.text, 'html.parser')
        all_text = res.text
        
        # 1. SOLVE CAPTCHA (Exact bot 3.py logic)
        captcha_match = re.search(r'(\d+\s*[\+\-\*]\s*\d+)\s*[=\?:]', all_text)
        if not captcha_match:
            captcha_match = re.search(r'what is\s*(\d+\s*[\+\-\*]\s*\d+)', all_text, re.I)
        if not captcha_match:
            elements = soup.find_all(["label", "div", "span", "p", "strong"])
            for el in elements:
                txt = el.get_text(separator=" ", strip=True)
                if any(op in txt for op in ["+", "-", "*"]):
                    m = re.search(r'(\d+\s*[\+\-\*]\s*\d+)', txt)
                    if m:
                        captcha_match = m
                        break
                        
        captcha_text = captcha_match.group(1) if captcha_match else "0 + 0"
        answer = "0"
        m2 = re.search(r'(\d+)\s*([\+\-\*])\s*(\d+)', captcha_text)
        if m2:
            a, op, b = int(m2.group(1)), m2.group(2), int(m2.group(3))
            if op == '+': answer = str(a + b)
            elif op == '-': answer = str(a - b)
            elif op == '*': answer = str(a * b)

        # 2. FIND FORM
        form = soup.find("form")
        if not form:
            p["login_status"] = "❌ No login form found"
            return False
            
        action = form.get("action")
        from urllib.parse import urljoin
        post_url = urljoin(login_url, action) if action else login_url

        form_data = {}
        for hidden in form.find_all("input", type="hidden"):
            name = hidden.get("name")
            if name: form_data[name] = hidden.get("value") or ""
        
        user_input = form.find("input", {"name": re.compile(r"user|email|id", re.I)}) or \
                     form.find("input", {"type": "text", "placeholder": re.compile(r"user|email", re.I)}) or \
                     form.find("input", {"type": "text"})
                     
        pass_input = form.find("input", {"name": re.compile(r"pass", re.I)}) or \
                     form.find("input", {"type": "password"})
                     
        captcha_input = form.find("input", {"placeholder": re.compile(r"answer|ans|code|verification|value|captcha", re.I)}) or \
                        form.find("input", {"name": re.compile(r"ans|captcha|ver|code", re.I)})
        
        user_field = user_input.get("name") if user_input else "username"
        pass_field = pass_input.get("name") if pass_input else "password"
        captcha_field = captcha_input.get("name") if captcha_input else "answer"

        form_data[user_field] = p.get("username", "")
        form_data[pass_field] = p.get("password", "")
        if captcha_field:
            form_data[captcha_field] = answer

        # 3. SUBMIT
        login_req = session.post(post_url, data=form_data, allow_redirects=True, timeout=15)
        
        # 4. VERIFY (Exact bot 3.py check logic)
        msg_link = p.get("msg_link", "").strip()
        if not msg_link.startswith("http") and msg_link != "":
            msg_link = "http://" + msg_link
            
        check_url = msg_link if msg_link else f"{login_url.split('/login')[0]}/client/SMSCDRStats"
        
        check_res = session.get(check_url, timeout=10)
        
        if 'logout' in login_req.text.lower() or 'logout' in check_res.text.lower() or 'sms reports' in check_res.text.lower() or 'dashboard' in check_res.text.lower() or 'cdrs' in check_res.text.lower():
            panel_sessions[idx] = session
            p["login_status"] = "✅ Active & Fetching"
            return True
        else:
            # এখানে ফেইল হলে অংক কী পেয়েছিল তা দেখা যাবে
            p["login_status"] = f"❌ Login Failed (Math: {captcha_text} = {answer})"
            return False
            
    except Exception as e:
        p["login_status"] = f"❌ Error: {str(e)[:20]}"
        
    return False

def panel_monitor_thread():
    global processed_otps, recent_traffic, panel_sessions
    while True:
        try:
            for idx, p in enumerate(bot_settings.get("panels", [])):
                if p.get("status") == "ON":
                    
                    if p.get("type") == "Auto Captcha Panel":
                        sess = panel_sessions.get(idx)
                        
                        if not sess:
                            now = time.time()
                            if now - p.get("last_login_attempt", 0) < 30: 
                                continue 
                            p["last_login_attempt"] = now
                            
                            success = attempt_auto_login(p, idx)
                            save_db() # Save login status text to show in settings
                            if not success:
                                continue 
                            sess = panel_sessions.get(idx)
                            
                        try:
                            # 🌟 auto sessions with sAjaxSource and Fallback HTML Parser
                            parsed_data, res_text = fetch_cpt_panel_cdrs(p, sess, p["msg_link"])
                            p["login_status"] = "✅ Active & Fetching"
                        except Exception as e:
                            p["login_status"] = "❌ Session Expired (Retrying...)"
                            del panel_sessions[idx]
                            save_db()
                            continue

                    elif p.get("api_url") or p.get("full_api_url"): 
                        full_url = p.get("full_api_url", "").strip()
                        url = p.get("api_url", "").strip()
                        token = p.get("token", "").strip()
                        if not full_url and not url: continue
                        
                        urls_to_try = []
                        if full_url:
                            urls_to_try.append(full_url)
                        else:
                            if "{token}" in url or "{key}" in url:
                                urls_to_try.append(url.replace("{token}", token).replace("{key}", token))
                            elif "token=" in url or "key=" in url:
                                urls_to_try.append(url)
                            else:
                                sep = '&' if '?' in url else '?'
                                urls_to_try.append(f"{url}{sep}token={token}")
                                urls_to_try.append(f"{url}{sep}key={token}&start=0")
                                urls_to_try.append(f"{url}{sep}key={token}")
                            
                        parsed_data = []
                        # 🌟 Browser Bypass (403 Forbidden Fix)
                        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
                        for try_url in urls_to_try:
                            try:
                                res = requests.get(try_url, headers=headers, timeout=10)
                                parsed_data = parse_panel_response(res.text, p)
                                if parsed_data:
                                    if not full_url and try_url != url and token:
                                        p["api_url"] = try_url.replace(token, "{token}")
                                        save_db()
                                    break
                            except Exception: continue
                        if not parsed_data: continue
                    else:
                        continue
                    
                    if p.get("type") != "Auto Captcha Panel":
                        limit = p.get("records", 0)
                        if limit > 0: parsed_data = parsed_data[:limit]
                        
                    for item in parsed_data:
                        num = item["number"]
                        otp = item["otp"]
                        msg_text = item["message"]
                        unique_id = f"{num}_{otp}"
                        
                        if processed_otps.add_if_new(unique_id):
                            processed_otps.add(unique_id)
                                 
                            char, iso = get_flag_and_code(num)
                            app_full_name, prem_app_html = get_service_info_html(p.get("name", "Panel"), msg_text)
                            current_time = time.time()
                            
                            recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                            recent_traffic.append({
                                "service": app_full_name,
                                "iso": iso,
                                "flag": char,
                                "number": num,
                                "time": current_time
                            })
                            # 🌟 শুধু লোকাল ফাইলে সেভ করবে, Firestore এ অহেতুক Write করবে না!
                            save_local_db()
                                 
                            display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                            masked = mask_number(display_num)
                            lang = detect_language(msg_text)
                            
                            lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                            display_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {masked} | 💬 {lang_name}")
                            
                            for fw in bot_settings.get("fw_groups", []):
                                kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                                temp_row = []
                                styles = ["danger", "success", "primary"]
                                for i, btn in enumerate(fw.get("buttons", [])):
                                    b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                                    if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                                    temp_row.append(b_obj)
                                    if len(temp_row) == 2:
                                        kb.append(temp_row)
                                        temp_row = []
                                if temp_row: kb.append(temp_row)
                                send_message(fw["chat_id"], display_msg, reply_markup={"inline_keyboard": kb})
                            
                            owners = []
                            # 🌟 Robust match (handles full vs national / no-plus number format)
                            for uid, session_data in user_active_sessions.items():
                                for act_num in session_data.get("nums", []):
                                    if numbers_match(act_num, num):
                                        owners.append(uid)
                                        break
                                        
                            if not owners:
                                for stex_n, n_owner in stex_assigned_numbers.items():
                                    if numbers_match(stex_n, num):
                                        owners.append(n_owner)
                                for vtx_n, n_owner in voltx_assigned_numbers.items():
                                    if numbers_match(vtx_n, num):
                                        owners.append(n_owner)
                                for znx_n, n_owner in zenex_assigned_numbers.items():
                                    if numbers_match(znx_n, num):
                                        owners.append(n_owner)
                                        
                            owners = list(set(owners)) 
                            for owner_id in owners:
                                lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                inbox_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {strip_cc(display_num)} | 💬 {lang_name}")
                                inbox_kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": otp}, "style": "success"}]]
                                
                                # রিওয়ার্ড যোগ করার লজিক
                                reward = float(bot_settings.get("otp_reward", 0.0))
                                _first = otp_reward_once(owner_id, num, otp)
                                if reward > 0 and _first:
                                    update_balance(owner_id, reward, "otp", str(num))
                                    inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "ignore", "style": "primary"}])
                                
                                send_message(owner_id, inbox_msg, reply_markup={"inline_keyboard": inbox_kb})
                                if _first:
                                    bump_otp_count(owner_id)
        except Exception as e:
            pass
        time.sleep(5) 

# ==========================================
# Firebase User Management
# ==========================================
# 🌟 Local User Cache: বারবার Firestore থেকে Read করা বন্ধ করবে!
user_cache = {}

def get_user(user_id):
    if user_id in user_cache: return user_cache[user_id]
    if not db: return {"user_id": user_id, "balance": 0.0, "total_refers": 0, "total_otps": 0}
    
    doc_ref = db.collection('users').document(str(user_id))
    doc = doc_ref.get()
    if doc.exists: 
        data = doc.to_dict()
        if "total_otps" not in data: data["total_otps"] = 0
        if "banned" not in data: data["banned"] = False
        if "verified" not in data: data["verified"] = False
        data.setdefault("balance", 0.0)
        data.setdefault("total_refers", 0)
        user_cache[user_id] = data
        return data
    else:
        new_user = {"user_id": user_id, "balance": 0.0, "total_refers": 0, "total_otps": 0, "banned": False, "verified": False, "joined_at": time.time()}
        doc_ref.set(new_user)
        user_cache[user_id] = new_user
        return new_user

def update_balance(user_id, amount, kind="adjust", note=""):
    """Atomic balance change + transaction log. Returns True on success."""
    try:
        amount = round(float(amount), 6)
    except (TypeError, ValueError):
        log.error("update_balance: bad amount %r for %s", amount, user_id)
        return False
    if not math.isfinite(amount) or amount == 0:
        return False
    try:
        with db.lock:
            ref = db.collection('users').document(str(user_id))
            ref.set({"user_id": user_id, "balance": Increment(amount)}, merge=True)
            bal = ref.get().to_dict().get("balance", 0.0)
            db.collection('transactions').document(uuid.uuid4().hex).set({
                "user_id": str(user_id), "amount": amount, "kind": kind,
                "note": str(note)[:100], "balance_after": bal, "ts": time.time()})
        if user_id in user_cache:
            user_cache[user_id]["balance"] = bal
        return True
    except Exception:
        log.exception("update_balance failed for %s", user_id)
        return False


def deduct_balance_if_enough(user_id, amount, kind="withdraw", note=""):
    """Check + deduct in ONE locked step (no negative balances)."""
    with db.lock:
        d = db.collection('users').document(str(user_id)).get().to_dict() or {}
        if float(d.get("balance", 0) or 0) + 1e-9 < amount:
            return False
        return update_balance(user_id, -amount, kind, note)


def parse_amount(text, allow_negative=False):
    """float() that refuses nan / inf / zero / negatives. Returns None if invalid."""
    try:
        v = float(str(text).strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    if v == 0 or (v < 0 and not allow_negative):
        return None
    return round(v, 2)


def _pos_float(text):
    v = float(str(text).strip())
    if not math.isfinite(v) or v < 0:
        raise ValueError("bad number")
    return v


otp_rewarded = BoundedSet(20000)

def otp_reward_once(owner_id, num, otp):
    """True only the first time this (user, number, otp) is seen - stops double rewards
    when two listeners report the same OTP."""
    otp = str(otp or "")
    if not otp or otp == "CODE":
        return True
    return otp_rewarded.add_if_new(f"{owner_id}|{_norm_num(num)}|{otp}")


def bump_otp_count(uid):
    try:
        db.collection('users').document(str(uid)).set({"total_otps": Increment(1)}, merge=True)
    except Exception:
        log.exception("bump_otp_count failed for %s", uid)
    if uid in user_cache:
        user_cache[uid]["total_otps"] = user_cache[uid].get("total_otps", 0) + 1
    track_otp_daily(uid)
    maybe_pay_referral(uid, per_otp=True)


def notify_referral_join(inviter, msg):
    """Tell the inviter right away that someone joined through their link."""
    try:
        fr = msg.get("from") or {}
        uid = fr.get("id", "")
        name = html.escape(f"{fr.get('first_name', '')} {fr.get('last_name', '')}".strip() or "User")
        reward = refer_reward_amount()
        need = int(bot_settings.get("refer_min_otps", 1) or 0)
        if refer_every_otp():
            wait = f"\n\u23F3 <b>You will get {reward} TK for every OTP they receive.</b>"
        else:
            wait = (f"\n\u23F3 <b>You will get {reward} TK after they receive {need} OTP.</b>" if need > 0 else "")
        txt = (
            f"{PEM['gift']} <b>New User Joined Via Your Link!</b>\n"
            f"------------------\n"
            f"{PEM['user']} <b>User:</b> <a href='tg://user?id={uid}'>{name}</a>\n"
            f"{PEM['num']} <b>ID:</b> <code>{uid}</code>\n"
            f"------------------{wait}"
        )
        res = send_message(inviter, render_body_text(txt))
        if not res.get("ok"):
            log.warning("referral notify to %s failed: %s", inviter, res.get("description", "network error"))
    except Exception:
        log.exception("notify_referral_join failed")

def refer_every_otp():
    return bot_settings.get("refer_mode", "every") == "every"


def refer_reward_amount():
    """Reward shown to users: per OTP in 'every' mode, one-time bonus in 'once' mode."""
    if refer_every_otp():
        v = bot_settings.get("refer_otp_reward")
        return bot_settings.get("refer_reward", 0.2) if v is None else v
    return bot_settings.get("refer_reward", 0.2)


_ref_notes = {}
_ref_notes_lock = threading.Lock()


def _queue_ref_note(inviter, amt):
    with _ref_notes_lock:
        n = _ref_notes.setdefault(inviter, {"amt": 0.0, "n": 0})
        n["amt"] += amt
        n["n"] += 1


def ref_notify_loop():
    """Every 20s tell each inviter how much referral OTP reward they just earned (one message, not one per OTP)."""
    while True:
        time.sleep(20)
        try:
            with _ref_notes_lock:
                batch = dict(_ref_notes)
                _ref_notes.clear()
            for inviter, n in batch.items():
                txt = (f"{PEM['gift']} <b>Referral OTP Reward</b>\n------------------\n"
                       f"\u2705 <b>You Received {money(n['amt'])} TK</b>\n"
                       f"\U0001F4E9 From <b>{n['n']}</b> OTP of your referrals")
                send_message(inviter, render_body_text(txt))
        except Exception:
            log.exception("ref_notify_loop error")


def pay_referral_per_otp(uid):
    """'every' mode: the inviter gets a reward for EVERY OTP the invited user receives."""
    try:
        with db.lock:
            d = db.collection('users').document(str(uid)).get().to_dict() or {}
            inviter = d.get("referred_by")
            if not inviter:
                return
            need = int(bot_settings.get("refer_min_otps", 1) or 0)
            if int(d.get("total_otps", 0) or 0) < need:
                return
            reward = float(refer_reward_amount())
            if reward <= 0:
                return
            update_balance(inviter, reward, "refer", f"otp from {uid}")
        # instant message to the inviter: who received the OTP + how many OTP this user has so far
        name = html.escape(f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() or d.get("username") or "User")
        txt = (f"{PEM['gift']} <b>Referral OTP Reward</b>\n------------------\n"
               f"\u2705 <b>You Received {money(reward)} TK</b>\n"
               f"{PEM['user']} <b>From:</b> <a href='tg://user?id={uid}'>{name}</a>\n"
               f"{PEM['num']} <b>ID:</b> <code>{uid}</code>\n"
               f"\U0001F4E9 <b>OTP from this user:</b> {int(d.get('total_otps', 0) or 0)}")
        send_message(inviter, render_body_text(txt))
    except Exception:
        log.exception("pay_referral_per_otp failed for %s", uid)


def maybe_pay_referral(uid, per_otp=False):
    """'every' mode: pay the inviter on each OTP.  'once' mode: pay once after `refer_min_otps` OTPs."""
    if refer_every_otp():
        if per_otp:
            pay_referral_per_otp(uid)
        return
    try:
        with db.lock:
            ref = db.collection('users').document(str(uid))
            doc = ref.get()
            if not doc.exists:
                return
            d = doc.to_dict()
            inviter = d.get("referred_by")
            if not inviter or d.get("ref_paid"):
                return
            need = int(bot_settings.get("refer_min_otps", 1) or 0)
            if int(d.get("total_otps", 0) or 0) < need:
                return
            ref.update({"ref_paid": True})
            reward = float(bot_settings.get("refer_reward", 0.2))
            update_balance(inviter, reward, "refer", f"from {uid}")
        ref_msg = (
            f"{PEM['gift']} <b>New Referral !</b>\n"
            f"------------------\n"
            f"\u2705 <b>You Received {reward} TK</b>\n"
            f"------------------\n"
            f"{PEM['user']} <b>From User ID:</b> <code>{uid}</code>"
        )
        send_message(inviter, render_body_text(ref_msg))
    except Exception:
        log.exception("maybe_pay_referral failed for %s", uid)


_touch_cache = {}

def touch_user(msg):
    """Save/refresh the user's bio data (name, username, language...) - at most every 10 min."""
    fr = msg.get("from") or {}
    uid = fr.get("id")
    if not uid:
        return
    now = time.time()
    if now - _touch_cache.get(uid, 0) < 600:
        return
    _touch_cache[uid] = now
    try:
        get_user(uid)    # make sure the document exists
        uname = fr.get("username") or ""
        db.collection('users').document(str(uid)).set({
            "first_name": fr.get("first_name", ""), "last_name": fr.get("last_name", ""),
            "username": uname, "username_lower": uname.lower(),
            "language_code": fr.get("language_code", ""), "is_premium": bool(fr.get("is_premium")),
            "last_seen": now}, merge=True)
    except Exception:
        log.exception("touch_user failed for %s", uid)


twofa_tokens = {}

def _2fa_token(secret):
    """Keep the 2FA secret out of button data (short token instead)."""
    tok = hashlib.sha256(secret.encode()).hexdigest()[:16]
    if len(twofa_tokens) > 2000:
        twofa_tokens.clear()
    twofa_tokens[tok] = secret
    return tok


def claim_numbers(cands, chat_id, want, with_rate=True):
    """Atomically hand out numbers. cands = [(batch_id, index, number)].
    Everything is re-validated under a lock, so two users never get the same number
    and shifted list indexes can never give a wrong number."""
    global total_assigned_stats
    got = []
    with number_lock:
        random.shuffle(cands)
        share_limit = bot_settings.get("num_share", 1)
        for b_id, idx, num in cands:
            if len(got) >= want:
                break
            batch = number_batches.get(b_id)
            if not batch:
                continue
            lst = batch["numbers"]
            if 0 <= idx < len(lst) and lst[idx]["num"] == num:
                n_obj = lst[idx]
            else:
                n_obj = next((x for x in lst if x["num"] == num), None)
            if not n_obj or n_obj.get("to_remove") or chat_id in n_obj.get("used_by", []):
                continue
            got.append(n_obj["num"])
            if with_rate:
                assigned_number_rates[n_obj["num"]] = float(batch.get("rate", bot_settings.get("otp_reward", 0.0)))
            n_obj["shares"] += 1
            n_obj["used_by"].append(chat_id)
            total_assigned_stats += 1
            if n_obj["shares"] >= share_limit:
                n_obj["to_remove"] = True
                used_numbers_list.append(n_obj["num"])
        for b in number_batches.values():
            b["numbers"] = [n for n in b["numbers"] if not n.get("to_remove")]
    save_db()
    return got


# ---------------- Admin: user info / history / balances ----------------
def _e(x):
    return html.escape(str(x if x is not None else ""))

def fmt_ts(ts):
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M") if ts else "-"
    except Exception:
        return "-"

def money(x):
    try:
        return f"{float(x):.2f}".rstrip("0").rstrip(".") or "0"
    except Exception:
        return "0"

def find_user_id(query):
    q = str(query).strip()
    if q.isdigit():
        return q if db.collection('users').document(q).get().exists else None
    q = q.lstrip("@").lower()
    if not q:
        return None
    rows = db.collection('users').where('username_lower', '==', q).limit(1).stream()
    return rows[0].id if rows else None

KIND_LABEL = {"otp": "OTP", "refer": "Referral", "withdraw": "Withdraw", "refund": "Refund",
              "admin": "Admin", "adjust": "Adjust"}

def _tx_lines(uid, limit, offset=0):
    q = db.collection('transactions').where('user_id', '==', str(uid)).order_by('ts', direction="DESCENDING").limit(limit).offset(offset)
    out = ""
    for t in q.stream():
        d = t.to_dict()
        sign = "+" if d.get("amount", 0) >= 0 else ""
        out += (f"{fmt_ts(d.get('ts'))} | {KIND_LABEL.get(d.get('kind'), d.get('kind'))} | "
                f"<b>{sign}{money(d.get('amount'))}</b> \u2192 {money(d.get('balance_after'))}\n")
    return out

def build_my_profile(uid):
    """User's own profile card (no admin-only info)."""
    uid = str(uid)
    if not db.collection('users').document(uid).get().exists:
        get_user(int(uid))
    d = db.collection('users').document(uid).get().to_dict() or {}
    name = f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() or "-"
    uname = f"@{d['username']}" if d.get("username") else "-"

    earn = {r[0]: (r[1], r[2]) for r in db.sql(
        "SELECT json_extract(data,'$.kind'), COALESCE(SUM(json_extract(data,'$.amount')),0), COUNT(*) "
        "FROM docs WHERE collection='transactions' AND json_extract(data,'$.user_id')=? GROUP BY 1", (uid,))}
    wd = {r[0]: (r[1], r[2]) for r in db.sql(
        "SELECT json_extract(data,'$.status'), COUNT(*), COALESCE(SUM(json_extract(data,'$.amount')),0) "
        "FROM docs WHERE collection='withdrawals' AND json_extract(data,'$.user_id')=? GROUP BY 1", (uid,))}

    def e(k):
        a, c = earn.get(k, (0, 0))
        return f"{money(a)} TK ({c})"

    def w(k):
        c, a = wd.get(k, (0, 0))
        return f"{money(a)} TK ({c})"

    line = "\u2501" * 12
    txt = (
        f"{line}\n\U0001F464 <b>MY PROFILE</b>\n{line}\n"
        f"ID: <code>{_e(uid)}</code>\n"
        f"Name: {_e(name)}\n"
        f"Username: {_e(uname)}\n"
        f"Joined: {fmt_ts(d.get('joined_at'))}\n"
        f"{line}\n"
        f"\U0001F4B0 Balance: <b>{money(d.get('balance', 0))} TK</b>\n"
        f"\U0001F4E9 Total OTP: <b>{d.get('total_otps', 0)}</b>\n"
        f"\U0001FAC2 Total Refers: <b>{d.get('total_refers', 0)}</b>\n"
        f"{line}\n"
        f"<b>EARNINGS</b>\n"
        f"OTP: {e('otp')}\nReferral: {e('refer')}\n"
        f"<b>WITHDRAWALS</b>\n"
        f"Paid: {w('approved')}\nPending: {w('pending')}\nRejected: {w('rejected')}\n"
        f"{line}\n"
        f"<b>LAST 5 TRANSACTIONS</b>\n{_tx_lines(uid, 5) or 'No history yet.'}"
    )
    kb = {"inline_keyboard": [[
        {"text": "Refresh", "icon_custom_emoji_id": "5465368548702446780", "callback_data": "my_profile", "style": "success"},
        {"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
    return txt, kb

def build_user_profile(uid):
    doc = db.collection('users').document(str(uid)).get()
    if not doc.exists:
        return None
    d = doc.to_dict()
    name = f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() or "-"
    uname = f"@{d['username']}" if d.get("username") else "-"
    verified = True if d.get("total_otps", 0) > 0 else d.get("verified", False)

    earn = {r[0]: (r[1], r[2]) for r in db.sql(
        "SELECT json_extract(data,'$.kind'), COALESCE(SUM(json_extract(data,'$.amount')),0), COUNT(*) "
        "FROM docs WHERE collection='transactions' AND json_extract(data,'$.user_id')=? GROUP BY 1", (str(uid),))}
    wd = {r[0]: (r[1], r[2]) for r in db.sql(
        "SELECT json_extract(data,'$.status'), COUNT(*), COALESCE(SUM(json_extract(data,'$.amount')),0) "
        "FROM docs WHERE collection='withdrawals' AND json_extract(data,'$.user_id')=? GROUP BY 1", (str(uid),))}

    def e(k):
        return f"{money(earn.get(k, (0, 0))[0])} TK ({earn.get(k, (0, 0))[1]})"

    def w(k):
        c, a = wd.get(k, (0, 0))
        return f"{money(a)} TK ({c})"

    txt = (
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001F464 <b>USER PROFILE</b>\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"ID: <a href='tg://user?id={_e(uid)}'>{_e(uid)}</a>\n"
        f"Name: {_e(name)}\n"
        f"Username: {_e(uname)}\n"
        f"Language: {_e(d.get('language_code') or '-')} | Premium: {'Yes' if d.get('is_premium') else 'No'}\n"
        f"Joined: {fmt_ts(d.get('joined_at'))}\n"
        f"Last seen: {fmt_ts(d.get('last_seen'))}\n"
        f"Referred by: {_e(d.get('referred_by') or '-')}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"Balance: <b>{money(d.get('balance', 0))} TK</b>\n"
        f"Total OTPs: {d.get('total_otps', 0)} | Refers: {d.get('total_refers', 0)}\n"
        f"Verified: {verified} | Banned: {d.get('banned', False)}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"<b>EARNINGS (from history log)</b>\n"
        f"OTP: {e('otp')}\nReferral: {e('refer')}\nAdmin adj.: {e('admin')}\n"
        f"<b>WITHDRAWALS</b>\n"
        f"Approved: {w('approved')}\nPending: {w('pending')}\nRejected: {w('rejected')}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"<b>LAST 8 TRANSACTIONS</b>\n{_tx_lines(uid, 8) or 'No history yet.'}"
    )
    kb = {"inline_keyboard": [
        [{"text": "Full History", "callback_data": f"um_hist_{uid}_0", "style": "primary"}],
        [{"text": "Back", "callback_data": "user_management", "style": "danger"}]]}
    return txt, kb

def build_history_page(uid, page):
    per = 10
    total = db.collection('transactions').where('user_id', '==', str(uid)).count()
    pages = max(1, (total + per - 1) // per)
    page = max(0, min(page, pages - 1))
    body = _tx_lines(uid, per, page * per) or "No history yet."
    txt = (f"\U0001F9FE <b>HISTORY of {_e(uid)}</b>\nPage {page + 1}/{pages} \u2022 {total} records\n"
           f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n{body}")
    nav = []
    if page > 0:
        nav.append({"text": "Prev", "callback_data": f"um_hist_{uid}_{page - 1}", "style": "primary"})
    if page < pages - 1:
        nav.append({"text": "Next", "callback_data": f"um_hist_{uid}_{page + 1}", "style": "primary"})
    kb = {"inline_keyboard": ([nav] if nav else []) + [
        [{"text": "Profile", "callback_data": f"um_prof_{uid}", "style": "success"}]]}
    return txt, kb

def build_balances_page(page):
    per = 10
    n_users, total_bal = db.sql("SELECT COUNT(*), COALESCE(SUM(json_extract(data,'$.balance')),0) FROM docs WHERE collection='users'")[0]
    n_pos = db.sql("SELECT COUNT(*) FROM docs WHERE collection='users' AND json_extract(data,'$.balance') > 0")[0][0]
    wd = {r[0]: (r[1], r[2]) for r in db.sql(
        "SELECT json_extract(data,'$.status'), COUNT(*), COALESCE(SUM(json_extract(data,'$.amount')),0) "
        "FROM docs WHERE collection='withdrawals' GROUP BY 1")}
    paid = wd.get("approved", (0, 0))
    pend = wd.get("pending", (0, 0))
    pages = max(1, (n_users + per - 1) // per)
    page = max(0, min(page, pages - 1))
    rows = db.collection('users').order_by('balance', direction="DESCENDING").limit(per).offset(page * per).stream()
    lines = ""
    for i, u in enumerate(rows, page * per + 1):
        d = u.to_dict()
        nm = (f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() or d.get("username") or u.id)[:18]
        lines += (f"{i}. <a href='tg://user?id={u.id}'>{_e(nm)}</a> \u2014 <b>{money(d.get('balance', 0))}</b> TK"
                  f" \u2022 {d.get('total_otps', 0)} OTP\n")
    txt = (
        f"\U0001F4B0 <b>ALL USERS BALANCE</b>\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"Users: {n_users} (with balance: {n_pos})\n"
        f"Total balance (owed): <b>{money(total_bal)} TK</b>\n"
        f"Paid out: {money(paid[1])} TK ({paid[0]})\n"
        f"Pending withdraw: {money(pend[1])} TK ({pend[0]})\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"<b>Page {page + 1}/{pages} (highest first)</b>\n{lines or 'No users.'}"
    )
    nav = []
    if page > 0:
        nav.append({"text": "Prev", "callback_data": f"um_bal_{page - 1}", "style": "primary"})
    if page < pages - 1:
        nav.append({"text": "Next", "callback_data": f"um_bal_{page + 1}", "style": "primary"})
    kb = {"inline_keyboard": ([nav] if nav else []) + [
        [{"text": "Export CSV", "callback_data": "um_csv", "style": "success"}],
        [{"text": "Back", "callback_data": "user_management", "style": "danger"}]]}
    return txt, kb

def send_users_csv(chat_id):
    buf = io.StringIO()
    wr = csv.writer(buf)
    wr.writerow(["user_id", "username", "first_name", "last_name", "language", "premium", "joined_at",
                 "last_seen", "balance", "total_otps", "total_refers", "referred_by", "banned"])
    for u in db.collection('users').order_by('balance', direction="DESCENDING").stream():
        d = u.to_dict()
        wr.writerow([u.id, d.get("username", ""), d.get("first_name", ""), d.get("last_name", ""),
                     d.get("language_code", ""), d.get("is_premium", False), fmt_ts(d.get("joined_at")),
                     fmt_ts(d.get("last_seen")), money(d.get("balance", 0)), d.get("total_otps", 0),
                     d.get("total_refers", 0), d.get("referred_by", ""), d.get("banned", False)])
    fname = datetime.now().strftime("users_balances_%Y%m%d_%H%M.csv")
    send_document(chat_id, fname, ("\ufeff" + buf.getvalue()).encode("utf-8"))


def send_backup_files(chat_id):
    """Send a fresh consistent copy of bot_users.db and bot_data.json to the admin on Telegram."""
    import sqlite3, tempfile, shutil
    tmpdir = tempfile.mkdtemp()
    try:
        send_message(chat_id, render_body_text("⏳ <b>Preparing backup...</b>"))
        stamp = time.strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(tmpdir, f"bot_users_{stamp}.db")
        with db.lock:
            out = sqlite3.connect(dest)
            try:
                db.conn.backup(out)
            finally:
                out.close()
        files = [(f"bot_users_{stamp}.db", dest)]
        if os.path.exists(DB_FILE):
            files.append((f"bot_data_{stamp}.json", DB_FILE))
        for name, path in files:
            if os.path.getsize(path) > 49 * 1024 * 1024:
                send_message(chat_id, render_body_text(f"⚠️ <b>{name}</b> is larger than 50 MB, Telegram can't send it."))
                continue
            with open(path, "rb") as f:
                send_document(chat_id, name, f.read())
        send_message(chat_id, render_body_text(f"{PEM['ok']} <b>Backup sent!</b> Keep these files safe."))
    except Exception:
        log.exception("send_backup_files failed")
        send_message(chat_id, render_body_text("❌ <b>Backup failed.</b> Check bot_errors.log"))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def restore_backup_file(chat_id, doc):
    """Restore bot_users.db or bot_data.json that the admin uploaded in Telegram."""
    import sqlite3, tempfile, shutil
    name = (doc.get("file_name") or "").lower()
    if not (name.endswith(".db") or name.endswith(".json")):
        send_message(chat_id, render_body_text("❌ Only <b>.db</b> (users) or <b>.json</b> (settings/stock) backup files are accepted."))
        return
    tmpdir = tempfile.mkdtemp()
    try:
        info = requests.get(f"{BASE_URL}/getFile", params={"file_id": doc["file_id"]}, timeout=30).json()
        if not info.get("ok"):
            send_message(chat_id, render_body_text("❌ Telegram bots can download files up to <b>20 MB</b> only. This file is too big or can't be fetched."))
            return
        raw = requests.get(f"{FILE_URL}{info['result']['file_path']}", timeout=120).content
        tmp = os.path.join(tmpdir, "upload.bin")
        with open(tmp, "wb") as f:
            f.write(raw)

        if name.endswith(".db"):
            chk = sqlite3.connect(tmp)
            try:
                ok = chk.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                chk.execute("SELECT collection, id, data FROM docs LIMIT 1").fetchall()
            except Exception:
                ok = False
            finally:
                chk.close()
            if not ok:
                send_message(chat_id, render_body_text("❌ This is not a valid bot_users.db backup. Nothing was changed."))
                return
            try:
                db.backup_now()          # safety copy of the current data, so this restore can be undone
            except Exception:
                log.exception("pre-restore backup failed")
            with db.lock:
                src = sqlite3.connect(tmp)
                try:
                    src.backup(db.conn)  # overwrite the live database with the uploaded one
                finally:
                    src.close()
            user_cache.clear()
            user_banned_cache.clear()
            try:
                all_known_users.clear()
                for r in db.sql("SELECT id FROM docs WHERE collection='users'"):
                    all_known_users.add(str(r[0]))
                _save_users_list()
            except Exception:
                log.exception("rebuild users list failed")
            total = db.collection('users').count()
            send_message(chat_id, render_body_text(f"{PEM['ok']} <b>Users database restored!</b>\n👥 Users: <b>{total}</b>\n\nSend the other file too, or press Cancel."), reply_markup=get_cancel_kb())
        else:
            try:
                data = json.loads(raw.decode("utf-8-sig"))
                if not isinstance(data, dict) or "bot_settings" not in data:
                    raise ValueError("no bot_settings")
            except Exception:
                send_message(chat_id, render_body_text("❌ This is not a valid bot_data.json backup. Nothing was changed."))
                return
            with _flush_lock:            # stop the background saver from overwriting the restored file
                if os.path.exists(DB_FILE):
                    shutil.copyfile(DB_FILE, DB_FILE + ".before_restore")
                shutil.copyfile(tmp, DB_FILE + ".tmp")
                os.replace(DB_FILE + ".tmp", DB_FILE)
                load_db()
            send_message(chat_id, render_body_text(f"{PEM['ok']} <b>Settings and number stock restored!</b>\n📦 Batches: <b>{len(number_batches)}</b>\n\nSend the other file too, or press Cancel."), reply_markup=get_cancel_kb())
    except Exception:
        log.exception("restore_backup_file failed")
        send_message(chat_id, render_body_text("❌ <b>Restore failed.</b> Check bot_errors.log"))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def add_referral(inviter_id, new_user_id):
    if not db.collection('users').document(str(new_user_id)).get().exists:
        get_user(new_user_id) 
        reward = bot_settings.get("refer_reward", 0.2)
        update_balance(inviter_id, reward)
        db.collection('users').document(str(inviter_id)).update({"total_refers": Increment(1)})
        if inviter_id in user_cache:
            user_cache[inviter_id]["total_refers"] = user_cache[inviter_id].get("total_refers", 0) + 1
        
        # আপনার দেওয়া নতুন ডিজাইন
        ref_msg = (
            f"{PEM['gift']} <b>New Referral !</b>\n"
            f"------------------\n"
            f"🔥 <b>You Received {reward} TK</b>\n"
            f"------------------\n"
            f"{PEM['user']} <b>From User ID:</b> <code>{new_user_id}</code>"
        )
        send_message(inviter_id, render_body_text(ref_msg))

# ==========================================
# UI Keyboards & Menu Builders
# ==========================================
def normalize_support_url(text):
    """@name / t.me/name / https://... -> valid https URL. 'off' (or 0/none) removes the Support button link."""
    t = str(text or "").strip()
    if t.lower() in ("off", "0", "none", "remove", "-"):
        return ""
    if t.startswith("@"):
        t = "https://t.me/" + t[1:]
    elif t.lower().startswith(("t.me/", "telegram.me/")):
        t = "https://" + t
    if not t.lower().startswith(("https://", "http://", "tg://")) or " " in t or len(t) < 8:
        raise ValueError("bad url")
    return t


def get_cancel_kb():
    return {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_state", "style": "danger"}]]}

def main_menu(user_id):
    kb = [
        [
            {"text": "GET NUMBER", "icon_custom_emoji_id": "6282996898701775483", "style": "primary"},
            {"text": "SUPPORT", "icon_custom_emoji_id": "5337302974806922068", "style": "success"}
        ],
        [
            {"text": "Refer", "icon_custom_emoji_id": "5420396762189831222", "style": "success"},
            {"text": "PROFILE", "icon_custom_emoji_id": "5352861489541714456", "style": "primary"}
        ],
        [
            {"text": "WITHDRAWAL", "icon_custom_emoji_id": "5352585194295564660", "style": "danger"},
            {"text": "LEADERBOARD", "icon_custom_emoji_id": "5353032893096567467", "style": "success"}
        ]
    ]

    if is_admin(user_id):
        kb.append([
            {"text": "Admin Panel", "icon_custom_emoji_id": "5420155432272438703", "style": "danger"}
        ])

    return {"keyboard": kb, "resize_keyboard": True}

def get_admin_text():
    users_count = len(all_known_users) # 🌟 Zero Cost User Count!
    total_files = len(number_batches)
    available_nums = sum(len(b["numbers"]) for b in number_batches.values())

    txt = f"""
{PEM['admin']} <b>ADMIN CONTROL PANEL</b> {PEM['admin']}
━━━━━━━━━━━━━━━━━━

{PEM['graph']} <b>DATABASE OVERVIEW</b>
— — — — — — — — — —
{PEM['user']} Users      » {users_count}
{PEM['file']} Files      » {total_files}
{PEM['num']} Numbers    » {total_uploaded_stats}
{PEM['ok']} Assigned   » {total_assigned_stats}
{PEM['rocket']} Available  » {available_nums}

{PEM['graph']} <b>STOCK LEVEL</b>
— — — — — — — — — —
[██████░░░░░░░░░] {available_nums} free
"""
    return render_body_text(txt)

def admin_panel_keyboard():
    return {"inline_keyboard": [
        [{"text": "LEADER BOARD SYSTEM", "icon_custom_emoji_id": "5353032893096567467", "callback_data": "lb_main", "style": "success"}],
        [{"text": "Upload Number", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "upload_num", "style": "primary"},
         {"text": "Delete files", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "delete_files", "style": "danger"}],
        [{"text": "Broadcast", "icon_custom_emoji_id": "5789428375261023681", "callback_data": "broadcast_msg", "style": "success"},
         {"text": "System", "icon_custom_emoji_id": "5420155432272438703", "callback_data": "system_settings", "style": "primary"}],
        [{"text": "Used number", "icon_custom_emoji_id": "5352694861990501856", "callback_data": "show_used", "style": "success"},
         {"text": "Unused number", "icon_custom_emoji_id": "5352597830089347330", "callback_data": "show_unused", "style": "success"}],
        [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]
    ]}

def system_settings_keyboard():
    return {"inline_keyboard": [
        [{"text": "StexSMS Control", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "stex_control", "style": "success"},
         {"text": "Voltx Control", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "voltx_control", "style": "primary"}],
        [{"text": "Zenex Control", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "zenex_control", "style": "success"}],
        [{"text": "Force Join System", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "manage_fj", "style": "primary"},
         {"text": "Admin Management", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "manage_admins", "style": "danger"}],
        [{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "callback_data": "manage_otp_groups", "style": "danger"},
         {"text": "User Management", "icon_custom_emoji_id": "5193063022226086560", "callback_data": "user_management", "style": "primary"}], 
        [{"text": "Low Stock Alert", "callback_data": "ls_menu", "style": "primary"}],
        [{"text": "Panel MANAGEMENT", "icon_custom_emoji_id": "5336879280578138635", "callback_data": "manage_panels", "style": "danger"},
         {"text": "Subscription", "icon_custom_emoji_id": "5190899075968441286", "callback_data": "dummy_alert", "style": "success"}],
        [{"text": "DXA Control", "icon_custom_emoji_id": "5193100774988617665", "callback_data": "dxa_control", "style": "primary"},
         {"text": "Premium Emoji", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "manage_emojis", "style": "success"}],
        [{"text": "Menu Design", "icon_custom_emoji_id": "5190751148704833975", "callback_data": "menu_design_list", "style": "primary"},
         {"text": "Test", "icon_custom_emoji_id": "5190781475468915802", "callback_data": "test_message_flow", "style": "primary"}], 
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]
    ]}

def get_user_management_text():
    # 🌟 Fast & Free User Management Stats!
    total = len(all_known_users)
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt = f"""➖➖➖➖➖➖➖➖
《 👋 USER VIEW 》
➖➖➖➖➖➖➖➖
📊 LIVE STATISTICS:
➖➖➖➖➖➖➖➖
🫂 TOTAL USERS: {total}
✅ VERIFIED USERS: (Hidden to save DB Cost)
🚫 BANNED USERS: (Hidden to save DB Cost)
➖➖➖➖➖➖➖➖
⌛ UPDATED: {now_str}"""
    return render_body_text(txt)

def user_management_keyboard():
    return {"inline_keyboard": [
        [{"text": "Manage Balance", "icon_custom_emoji_id": "5190576863226933563", "callback_data": "um_manage_balance", "style": "primary"},
         {"text": "Ban/Unban User", "icon_custom_emoji_id": "5334807341109908955", "callback_data": "um_ban_unban", "style": "danger"}],
        [{"text": "All Balances", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "um_bal_0", "style": "success"},
         {"text": "Export CSV", "icon_custom_emoji_id": "5352721946054268944", "callback_data": "um_csv", "style": "primary"}],
        [{"text": "User Profile / History", "icon_custom_emoji_id": "5352861489541714456", "callback_data": "um_user_profile", "style": "success"}],
        [{"text": "Download Backup", "callback_data": "um_backup", "style": "success"},
         {"text": "Restore Backup", "callback_data": "um_restore", "style": "danger"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def menu_design_list_keyboard():
    return {"inline_keyboard": [
        [{"text": "Edit /start Menu", "icon_custom_emoji_id": "5395444784611480792", "callback_data": "md_edit_start", "style": "primary"}],
        [{"text": "Edit GET NUMBER", "icon_custom_emoji_id": "5337132498965010628", "callback_data": "md_edit_get_number", "style": "success"},
         {"text": "Edit Search Number", "icon_custom_emoji_id": "5190645917711114179", "callback_data": "md_edit_search_number", "style": "success"}],
        [{"text": "Edit Select Country", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "md_edit_select_country", "style": "primary"}],
        [{"text": "Edit TRAFFIC", "icon_custom_emoji_id": "5353032893096567467", "callback_data": "md_edit_traffic", "style": "primary"},
         {"text": "Edit Refer", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "md_edit_refer", "style": "primary"}],
        [{"text": "Edit WITHDRAWAL", "icon_custom_emoji_id": "5352585194295564660", "callback_data": "md_edit_withdrawal", "style": "danger"},
         {"text": "Edit SUPPORT", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "md_edit_support", "style": "danger"}],
        [{"text": "Reset Defaults", "icon_custom_emoji_id": "5192812028632274956", "callback_data": "md_reset_defaults", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def menu_edit_options_keyboard(menu_key):
    return {"inline_keyboard": [
        [{"text": "Edit Body (Text)", "icon_custom_emoji_id": "5395444784611480792", "callback_data": f"md_text_{menu_key}", "style": "primary"}],
        [{"text": "Edit Inline Buttons", "icon_custom_emoji_id": "5420155432272438703", "callback_data": f"md_btns_{menu_key}", "style": "success"}],
        [{"text": "Back to Menus", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "menu_design_list", "style": "danger"}]
    ]}

def menu_buttons_list_keyboard(menu_key):
    kb = []
    btns = bot_settings["custom_messages"].get(menu_key, {}).get("buttons", [])
    for idx, btn in enumerate(btns):
        kb.append([{"text": f"Del: {btn['text']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"md_delbtn_{menu_key}_{idx}", "style": "danger"}])
    kb.append([{"text": "Add Inline Button", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"md_addbtn_{menu_key}", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_edit_{menu_key}", "style": "primary"}])
    return {"inline_keyboard": kb}

def emoji_settings_keyboard():
    return {"inline_keyboard": [
        [{"text": "Upload Flags (TXT)", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "up_flags_txt", "style": "primary"},
         {"text": "Download Flags", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_flags_txt", "style": "success"}],
        [{"text": "Upload Services (TXT)", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "up_apps_txt", "style": "primary"},
         {"text": "Download Services", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_apps_txt", "style": "success"}],
        [{"text": "Delete All Flags", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "del_all_flags", "style": "danger"},
         {"text": "Add Single Emoji", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_single_emoji", "style": "success"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
    ]}

def fj_settings_keyboard():
    status_text = 'ON' if bot_settings['fj_on'] else 'OFF'
    status_icon = "5352694861990501856" if bot_settings['fj_on'] else "5318840353510408444"
    kb = [[{"text": f"STATUS: {status_text}", "icon_custom_emoji_id": status_icon, "callback_data": "toggle_fj", "style": "primary"}]]
    for idx, ch in enumerate(bot_settings["fj_channels"]):
        kb.append([{"text": f"Delete: {ch}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_fj_{idx}", "style": "danger"}])
    kb.append([{"text": "Add Channel", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_fj", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}])
    return {"inline_keyboard": kb}

def admin_settings_keyboard():
    kb = []
    for idx, adm in enumerate(bot_settings["admins"]):
        text_btn = f"Owner: {adm}" if adm == OWNER_ID else f"Delete: {adm}"
        icon_id = "5353032893096567467" if adm == OWNER_ID else "5420130255174145507"
        cb_data = "ignore" if adm == OWNER_ID else f"del_adm_{idx}"
        kb.append([{"text": text_btn, "icon_custom_emoji_id": icon_id, "callback_data": cb_data, "style": "danger" if adm != OWNER_ID else "primary"}])
    kb.append([{"text": "Add Admin", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_adm", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}])
    return {"inline_keyboard": kb}

def otp_groups_list_keyboard():
    rg = bot_settings.get('range_group_id') or "Not set"
    rl = bot_settings.get('range_group_link') or "Not set"
    kb = [
        [{"text": "Edit OTP Button Link", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "edit_otp_link", "style": "primary"}],
        [{"text": "OTP Channel Link", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "edit_otp_channel_link", "style": "primary"}],
        [{"text": "Range GROUP Add", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_range_group", "style": "success"}],
        [{"text": "Active GROUP Link", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "edit_range_group_link", "style": "primary"}],
        [{"text": f"Range GID: {str(rg)[:28]}", "icon_custom_emoji_id": "5193063022226086560", "callback_data": "ignore", "style": "primary"}],
        [{"text": f"Link: {str(rl)[:28]}", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "ignore", "style": "primary"}],
    ]
    for idx, fg in enumerate(bot_settings['fw_groups']):
        kb.append([{"text": f"Group: {fg['chat_id']}", "icon_custom_emoji_id": "5193063022226086560", "callback_data": f"manage_fw_{idx}", "style": "primary"}])
    kb.append([{"text": "Add Forward Group", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_fw", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}])
    return {'inline_keyboard': kb}


def stex_control_keyboard():
    auto_status = "ON" if bot_settings.get("stex_auto", False) else "OFF"
    auto_emoji = "5352694861990501856" if auto_status == "ON" else "5420130255174145507"
    return {"inline_keyboard": [
        [{"text": "Add StexSMS Key", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_stex_key", "style": "success"},
         {"text": "View/Del Keys", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "view_stex_keys", "style": "danger"}],
        [{"text": "Manage StexSMS Services", "icon_custom_emoji_id": "5192739271886282680", "callback_data": "manage_stex_srv", "style": "success"}],
        [{"text": f"Auto Range: {auto_status}", "icon_custom_emoji_id": auto_emoji, "callback_data": "toggle_stex_auto", "style": "primary"},
         {"text": "Search Country", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "stex_search_country", "style": "primary"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def zenex_control_keyboard():
    auto_status = "ON" if bot_settings.get("zenex_auto", False) else "OFF"
    auto_emoji = "5352694861990501856" if auto_status == "ON" else "5420130255174145507"
    return {"inline_keyboard": [
        [{"text": "Add Zenex Key", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_zenex_key", "style": "success"},
         {"text": "View/Del Keys", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "view_zenex_keys", "style": "danger"}],
        [{"text": "Manage Zenex Services", "icon_custom_emoji_id": "5192739271886282680", "callback_data": "manage_zenex_srv", "style": "success"}],
        [{"text": f"Auto Range: {auto_status}", "icon_custom_emoji_id": auto_emoji, "callback_data": "toggle_zenex_auto", "style": "primary"},
         {"text": "Search Country", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "zenex_search_country", "style": "primary"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def voltx_control_keyboard():
    auto_status = "ON" if bot_settings.get("voltx_auto", False) else "OFF"
    auto_emoji = "5352694861990501856" if auto_status == "ON" else "5420130255174145507"
    return {"inline_keyboard": [
        [{"text": "Add Voltx Key", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_voltx_key", "style": "success"},
         {"text": "View/Del Keys", "icon_custom_emoji_id": "5422557736330106570", "callback_data": "view_voltx_keys", "style": "danger"}],
        [{"text": "Manage Voltx Services", "icon_custom_emoji_id": "5192739271886282680", "callback_data": "manage_voltx_srv", "style": "success"}],
        [{"text": f"Auto Range: {auto_status}", "icon_custom_emoji_id": auto_emoji, "callback_data": "toggle_voltx_auto", "style": "primary"},
         {"text": "Search Country", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "voltx_search_country", "style": "primary"}],
        [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "primary"}]
    ]}

def specific_fw_group_keyboard(idx):
    group = bot_settings["fw_groups"][idx]
    kb = []
    for b_idx, btn in enumerate(group.get("buttons", [])):
        kb.append([{"text": f"Del: {btn['text']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_fwbtn_{idx}_{b_idx}", "style": "danger"}])
    
    kb.append([{"text": "Add Inline Button", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"add_fwbtn_{idx}", "style": "success"}])
    kb.append([{"text": "Delete Entire Group", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"del_fw_{idx}", "style": "danger"}])
    kb.append([{"text": "Back to Groups", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "primary"}])
    return {"inline_keyboard": kb}

def dxa_control_keyboard():
    w_status = "ON" if bot_settings["withdraw_on"] else "OFF"
    sup_status = "ON" if bot_settings.get("support_link") else "OFF"
    grp_status = "ON" if bot_settings.get("w_group") else "OFF"
    return {"inline_keyboard": [
        [{"text": f"WITHDRAW: {w_status}", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "dxa_toggle_w", "style": "primary"}],
        [{"text": f"MIN WITHDRAW: {bot_settings['min_withdraw']}", "icon_custom_emoji_id": "5352877703043258544", "callback_data": "dxa_min_w", "style": "success"},
         {"text": f"OTP REWARD: {bot_settings['otp_reward']}", "icon_custom_emoji_id": "5190576863226933563", "callback_data": "dxa_otp_r", "style": "primary"}],
        [{"text": f"REFER REWARD: {bot_settings['refer_reward']}", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "dxa_ref_r", "style": "success"},
         {"text": f"COOLDOWN: {bot_settings['cooldown']}s", "icon_custom_emoji_id": "5337172996211648018", "callback_data": "dxa_cool", "style": "primary"}],
        [{"text": f"NUM/REQ: {bot_settings['num_req']}", "icon_custom_emoji_id": "5337132498965010628", "callback_data": "dxa_num_req", "style": "success"},
         {"text": f"NUM/SHARE: {bot_settings['num_share']}", "icon_custom_emoji_id": "5352862640592949843", "callback_data": "dxa_num_share", "style": "primary"}],
        [{"text": f"SUPPORT LINK: {sup_status}", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "dxa_sup_link", "style": "success"},
         {"text": "W. METHODS", "icon_custom_emoji_id": "5190899075968441286", "callback_data": "manage_w_methods", "style": "primary"}],
        [{"text": f"REFER MIN OTP: {bot_settings.get('refer_min_otps', 1)}", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "dxa_ref_min", "style": "success"}],
        [{"text": f"REFER MODE: {'EVERY OTP' if refer_every_otp() else 'ONCE'}", "callback_data": "dxa_toggle_refmode", "style": "primary"},
         {"text": f"REFER/OTP: {refer_reward_amount()}", "callback_data": "dxa_ref_otp", "style": "success"}],
        [{"text": f"W. GROUP: {grp_status}", "icon_custom_emoji_id": "5420517437885943844", "callback_data": "dxa_w_group", "style": "success"},
         {"text": "BACK", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
    ]}

def w_methods_keyboard():
    kb = []
    for idx, m in enumerate(bot_settings["w_methods"]):
        kb.append([{"text": f"Delete: {m}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_wm_{idx}", "style": "danger"}])
    kb.append([{"text": "Add Method", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_wm", "style": "success"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "dxa_control", "style": "primary"}])
    return {"inline_keyboard": kb}

def typed_panels_list_keyboard(p_type):
    kb = []
    for idx, p in enumerate(bot_settings["panels"]):
        if p.get("type", "API Panel") != p_type: continue
        action_text = f"Turn OFF {p['name']}" if p['status'] == 'ON' else f"Turn ON {p['name']}"
        action_icon = "5318840353510408444" if p['status'] == 'ON' else "5192812028632274956"
        icon_id = "5420155432272438703" 
        kb.append([
            {"text": action_text, "icon_custom_emoji_id": action_icon, "callback_data": f"tog_pnl_{idx}", "style": "danger" if p['status'] == 'ON' else "success"},
            {"text": f"{p['name']}", "icon_custom_emoji_id": icon_id, "callback_data": f"conf_pnl_{idx}", "style": "primary"}
        ])
    add_cb = "add_api_panel" if p_type == "API Panel" else "add_cpt_panel"
    kb.append([{"text": "Add New Provider", "icon_custom_emoji_id": "5420323438508155202", "callback_data": add_cb, "style": "success"}])
    kb.append([{"text": "Delete Provider", "icon_custom_emoji_id": "5336944168944047463", "callback_data": f"list_del_{'api' if p_type=='API Panel' else 'cpt'}", "style": "danger"}])
    kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_panels", "style": "primary"}])
    return {"inline_keyboard": kb}

def panel_config_keyboard(idx):
    p = bot_settings["panels"][idx]
    
    kb = []
    action_text = "Turn OFF" if p['status'] == 'ON' else "Turn ON"
    action_icon = "5318840353510408444" if p['status'] == 'ON' else "5192812028632274956"
    kb.append([{"text": action_text, "icon_custom_emoji_id": action_icon, "callback_data": f"tog_pnl_{idx}", "style": "danger" if p['status'] == 'ON' else "success"}])
    
    if p["type"] != "Auto Captcha Panel":
        rec_count_text = "All (Unlimited)" if p.get('records', 0) == 0 else str(p.get('records'))
        kb.append([{"text": "Set API URL", "icon_custom_emoji_id": "5420517437885943844", "callback_data": f"set_p_api_{idx}", "style": "primary"}])
        kb.append([{"text": "Set Token", "icon_custom_emoji_id": "5353022963132174959", "callback_data": f"set_p_tok_{idx}", "style": "primary"}])
        kb.append([{"text": "🌐 Full API (URL+Token)", "icon_custom_emoji_id": "5420517437885943844", "callback_data": f"set_p_fapi_{idx}", "style": "primary"}])
        kb.append([{"text": f"Set Records Count: {rec_count_text}", "icon_custom_emoji_id": "5192739271886282680", "callback_data": f"set_p_rec_{idx}", "style": "primary"}])
        
    kb.append([{"text": "Test Connection", "icon_custom_emoji_id": "5352694861990501856", "callback_data": f"test_p_conn_{idx}", "style": "success"}])
        
    back_data = "manage_api_panels" if p.get("type", "API Panel") == "API Panel" else "manage_cpt_panels"
    kb.append([{"text": "Back to Providers", "icon_custom_emoji_id": "5267490665117275176", "callback_data": back_data, "style": "danger"}])
    return {"inline_keyboard": kb}

def build_traffic_ui():
    global recent_traffic
    current_time = time.time()
    # ১০ মিনিটের ট্রাফিক নেওয়ার জন্য 3600 (১ ঘণ্টা) এর জায়গায় 600 (১০ মিনিট) করে দেওয়া হলো
    recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 600]
    
    stats = {}
    for t in recent_traffic:
        srv = t.get("service", "Unknown")
        iso = t.get("iso", "XX")
        flag = t.get("flag", "🌍")
        
        if srv not in stats:
            stats[srv] = {}
        if iso not in stats[srv]:
            stats[srv][iso] = {"count": 0, "flag": flag}
        stats[srv][iso]["count"] += 1
        
    txt = "🔥 10 Minute LIVE Traffic\n\n"
    
    kb = []
    if not stats:
        txt += "<i>No recent traffic found in the last 10 minutes...</i>\n"
    else:
        srv_totals = []
        for srv, countries in stats.items():
            total = sum(c["count"] for c in countries.values())
            srv_totals.append((srv, total, countries))
        
        srv_totals.sort(key=lambda x: x[1], reverse=True)
        
        for srv, total, countries in srv_totals:
            app_full_name, prem_app_html = get_service_info_html(srv)
            # Service Name and Total count
            txt += f"{prem_app_html} {app_full_name.upper()} {total}\n"
            
            c_list = sorted(countries.items(), key=lambda x: x[1]["count"], reverse=True)
            c_list = c_list[:7] # আপনি চাইলে লিমিট সরাতে পারেন
            
            for i, (iso, c_data) in enumerate(c_list):
                prem_flag_html = get_flag_info_html(iso)
                count = c_data["count"]
                
                c_name = iso
                for code, fdata in bot_settings.get("premium_flags", {}).items():
                    if fdata.get("iso") == iso:
                        c_name = fdata.get("name", iso)
                        break
                
                # Logic for LOW, MEDIUM, HIGH
                if count < 20:
                    status_text = "LOW"
                    status_emoji = '<tg-emoji emoji-id="5408846744727334338">🔴</tg-emoji>'
                elif count < 30:
                    status_text = "MEDIUM"
                    status_emoji = '<tg-emoji emoji-id="5409220909393290791">🟡</tg-emoji>'
                else:
                    status_text = "HIGH"
                    status_emoji = '<tg-emoji emoji-id="5192812028632274956">🟢</tg-emoji>'
                        
                txt += f"{prem_flag_html} {c_name.upper()} :  {status_text} {status_emoji}\n"
                
            txt += "\n"
        
        now = datetime.now()
        current_time_str = now.strftime("%I:%M %p")
        updated_emoji = '<tg-emoji emoji-id="5336983442125001376">🕒</tg-emoji>'
        txt += f"{updated_emoji} Updated {current_time_str}\n"

        for srv, _, _ in srv_totals: 
            safe_srv = srv[:20] 
            app_full_name, _ = get_service_info_html(safe_srv, safe_srv)
            kb.append([{"text": f"Explore {app_full_name} Range", "icon_custom_emoji_id": "5190645917711114179", "callback_data": f"exp_rng_{safe_srv}", "style": "success"}])
            
    txt = render_body_text(txt)
    kb.append([{"text": "Refresh", "icon_custom_emoji_id": "5465368548702446780", "callback_data": "refresh_traffic", "style": "primary"}])
    kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
    
    return txt, {"inline_keyboard": kb}

# ==========================================
# Message Handler
# ==========================================
def handle_message(msg):
    global total_uploaded_stats
    chat_id = msg["chat"]["id"]
    chat_type = msg["chat"].get("type", "private")
    
    if chat_type != "private":
        return
        
    text = msg.get("text", "")
    register_user_local(chat_id) # 🌟 Save User locally for Free Broadcasts!

    if is_user_banned(chat_id):
        send_message(chat_id, render_body_text("🚫 <b>You are banned from using this bot!</b>\nIf you think this is a mistake, please contact support."))
        return
    
    # --- REFERRAL FIX: Save inviter BEFORE Force Join ---
    if text.startswith("/start"):
        parts = text.split()
        if len(parts) > 1 and parts[1].isdigit():
            inviter = int(parts[1])
            if inviter != chat_id:
                if db:
                    doc = db.collection('users').document(str(chat_id)).get()
                    if not doc.exists:
                        get_user(chat_id)
                        db.collection('users').document(str(chat_id)).update({"referred_by": inviter, "ref_paid": False})
                        # Count the referral as soon as the new user joins
                        try:
                            get_user(inviter)  # make sure inviter doc exists
                            db.collection('users').document(str(inviter)).set({"total_refers": Increment(1)}, merge=True)
                            if inviter in user_cache:
                                user_cache[inviter]["total_refers"] = user_cache[inviter].get("total_refers", 0) + 1
                        except Exception:
                            log.exception("referral count failed for inviter %s", inviter)
                        notify_referral_join(inviter, msg)
                        
    touch_user(msg)
    if not check_force_join(chat_id):
        send_force_join_msg(chat_id)
        return
        
    MAIN_MENU_CMDS = ["GET NUMBER", "Search Number", "TRAFFIC", "Refer", "WITHDRAWAL", "SUPPORT", "Admin Panel", "2FA ONLINE", "PROFILE", "LEADERBOARD"]
    
    is_main_cmd = False
    if text in MAIN_MENU_CMDS or text.startswith("/start"):
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in temp_data: del temp_data[chat_id]
        is_main_cmd = True
    
    if chat_id in user_states and not is_main_cmd:
        state = user_states[chat_id]
        
        # 🌟 Auto Captcha Panel Setup Flow 
        if state == "wait_for_cpanel_url" and text:
            temp_data[chat_id]["p_data"]["login_url"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_user"
            send_message(chat_id, render_body_text("2️⃣ <b>Username</b>\n➡️ Panel এর Username দিন:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_user" and text:
            temp_data[chat_id]["p_data"]["username"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_pass"
            send_message(chat_id, render_body_text("3️⃣ <b>Password</b>\n➡️ Panel এর Password দিন:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_pass" and text:
            temp_data[chat_id]["p_data"]["password"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_msg_link"
            send_message(chat_id, render_body_text("4️⃣ <b>Message Link</b>\n➡️ যেখান থেকে SMS/OTP ডাটা (JSON) আসবে সেই Link দিন:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_link" and text:
            temp_data[chat_id]["p_data"]["msg_link"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_num_col_name"
            send_message(chat_id, render_body_text("5️⃣ <b>Number Column Name</b>\n➡️ Data তে Number column এর নাম কী? (যেমন: number, phone):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_num_col_name" and text:
            temp_data[chat_id]["p_data"]["num_col_name"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_num_col_idx"
            send_message(chat_id, render_body_text("6️⃣ <b>Number Column Serial</b>\n➡️ Number Column এর Serial Number কত? (যেমন: 3, 5):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_num_col_idx" and text:
            if text.isdigit():
                temp_data[chat_id]["p_data"]["num_col_idx"] = int(text)
                user_states[chat_id] = "wait_for_cpanel_msg_col_name"
                send_message(chat_id, render_body_text("7️⃣ <b>Message Column Name</b>\n➡️ Message/OTP column এর নাম কী? (যেমন: message, sms):"), reply_markup=get_cancel_kb())
            else:
                 send_message(chat_id, render_body_text("❌ Please enter a valid number serial!"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_col_name" and text:
            temp_data[chat_id]["p_data"]["msg_col_name"] = text.strip()
            user_states[chat_id] = "wait_for_cpanel_msg_col_idx"
            send_message(chat_id, render_body_text("8️⃣ <b>Message Column Serial</b>\n➡️ Message Column এর Serial Number কত? (যেমন: 5, 7):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_cpanel_msg_col_idx" and text:
            if text.isdigit():
                temp_data[chat_id]["p_data"]["msg_col_idx"] = int(text)
                temp_data[chat_id]["p_data"]["login_status"] = "⏳ Pending Auto-Login..."
                
                # Save the panel configuration
                bot_settings["panels"].append(temp_data[chat_id]["p_data"])
                save_db()
                
                send_message(chat_id, render_body_text(f"{PEM['ok']} <b>Auto Captcha Panel Added Successfully!</b>\nবট এখন থেকে নিজেই ব্যাকগ্রাউন্ডে ক্যাপচা সলভ করে লগিন করে নিবে।"), reply_markup=main_menu(chat_id))
                
                msg_id = temp_data[chat_id]["msg_id"]
                handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_cpt_panels", "id": "internal"})
                
                del user_states[chat_id]
                del temp_data[chat_id]
            else:
                 send_message(chat_id, render_body_text("❌ Please enter a valid number serial!"), reply_markup=get_cancel_kb())
            return

        # --- User Management Flows ---
        elif state == "wait_for_low_stock_limit" and text:
            if not text.strip().isdigit() or int(text.strip()) > 10_000_000:
                send_message(chat_id, render_body_text("\u274C Send a whole number, e.g. 200."), reply_markup=get_cancel_kb())
                return
            bot_settings["low_stock_limit"] = int(text.strip())
            bot_settings["low_stock_last"] = 0
            save_db()
            user_states.pop(chat_id, None)
            send_message(chat_id, render_body_text(build_low_stock_text()), reply_markup=low_stock_keyboard())
            return

        elif state == "wait_for_um_bal_uid" and text:
            target_uid_str = text.strip()
            if not target_uid_str.isdigit():
                send_message(chat_id, render_body_text("❌ Invalid ID! Please send a numeric User ID."), reply_markup=get_cancel_kb())
                return
            target_uid = int(target_uid_str)
            if db:
                doc = db.collection('users').document(str(target_uid)).get()
                if not doc.exists:
                    send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                    return
                current_bal = doc.to_dict().get('balance', 0.0)
                temp_data[chat_id]["target_uid"] = target_uid
                user_states[chat_id] = "wait_for_um_bal_amt"
                send_message(chat_id, render_body_text(f"✅ User found!\n💰 Current Balance: {current_bal} ৳\n\n📝 Send the amount to ADD (e.g. 50) or REMOVE (e.g. -50):"), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_um_bal_amt" and text:
            try:
                amt = parse_amount(text, allow_negative=True)
                if amt is None: raise ValueError("bad amount")
                target_uid = temp_data[chat_id]["target_uid"]
                update_balance(target_uid, amt, "admin", f"by admin {chat_id}")
                send_message(chat_id, render_body_text(f"{PEM['ok']} Balance updated successfully for {target_uid}!"), reply_markup=main_menu(chat_id))
                send_message(target_uid, render_body_text(f"🔔 Your balance has been adjusted by <b>{amt} ৳</b> by an Admin."))
                del user_states[chat_id]
                del temp_data[chat_id]
            except ValueError:
                send_message(chat_id, render_body_text("❌ Invalid amount! Please send a number."), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_um_ban_uid" and text:
            target_uid_str = text.strip()
            if not target_uid_str.isdigit():
                send_message(chat_id, render_body_text("❌ Invalid ID!"), reply_markup=get_cancel_kb())
                return
            target_uid = int(target_uid_str)
            if db:
                doc_ref = db.collection('users').document(str(target_uid))
                doc = doc_ref.get()
                if not doc.exists:
                    send_message(chat_id, render_body_text("❌ User not found in database!"), reply_markup=get_cancel_kb())
                    return
                current_status = doc.to_dict().get("banned", False)
                new_status = not current_status
                doc_ref.update({"banned": new_status})
                
                user_banned_cache[target_uid] = {'banned': new_status, 'time': time.time()}
                
                status_text = "BANNED 🚫" if new_status else "UNBANNED ✅"
                send_message(chat_id, render_body_text(f"✅ User {target_uid} has been {status_text}!"), reply_markup=main_menu(chat_id))
                del user_states[chat_id]
                del temp_data[chat_id]
            return

        elif state == "wait_for_um_prof_uid" and text:
            target = find_user_id(text)
            if not target:
                send_message(chat_id, render_body_text("User not found! Send a numeric User ID or @username."), reply_markup=get_cancel_kb())
                return
            res_ = build_user_profile(target)
            send_message(chat_id, render_body_text(res_[0]), reply_markup=res_[1])
            user_states.pop(chat_id, None)
            temp_data.pop(chat_id, None)
            return

        # --- Menu Design Flow ---
        elif state == "wait_for_menu_text" and text:
            try:
                menu_key = temp_data[chat_id]["menu_key"]
                formatted_html_text = extract_premium_html(msg)
                
                bot_settings["custom_messages"][menu_key]["text"] = formatted_html_text
                save_db()
                
                delete_message(chat_id, msg["message_id"])
                
                preview_text = render_body_text(formatted_html_text)
                success_text = f"{PEM['ok']} <b>Message Body Updated successfully!</b>\n\n🎨 <b>Editing: {menu_key.upper()}</b>\n\nPreview of current Text:\n{preview_text}"
                edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(success_text), reply_markup=menu_edit_options_keyboard(menu_key))
            except Exception as e:
                send_message(chat_id, f"❌ Error saving text: {e}")
            finally:
                if chat_id in user_states: del user_states[chat_id]
                if chat_id in temp_data: del temp_data[chat_id]
            return
            
        elif state == "wait_for_menu_btn" and text:
            try:
                menu_key = temp_data[chat_id]["menu_key"]
                if "-" in text:
                    parts = text.split("-", 1)
                    btn_text = parts[0].strip()
                    btn_url = parts[1].strip()
                    
                    emoji_id = None
                    emoji_char = ""
                    for ent in msg.get("entities", []):
                        if ent.get("type") == "custom_emoji":
                            emoji_id = ent.get("custom_emoji_id")
                            offset = ent.get("offset", 0)
                            length = ent.get("length", 0)
                            b_text = text.encode('utf-16-le')
                            emoji_char = b_text[offset*2:(offset+length)*2].decode('utf-16-le')
                            break
                            
                    if emoji_char:
                        btn_text = btn_text.replace(emoji_char, "").strip()
                        
                    btn_data = {"text": btn_text, "url": btn_url, "style": "primary"}
                    if emoji_id:
                        btn_data["icon_custom_emoji_id"] = emoji_id
                        
                    bot_settings["custom_messages"][menu_key]["buttons"].append(btn_data)
                    save_db()
                    delete_message(chat_id, msg["message_id"])
                    edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(f"{PEM['gear']} <b>Edit Inline Buttons: {menu_key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(menu_key))
                else:
                    send_message(chat_id, render_body_text(f"{PEM['no']} Invalid format. Use <code>Button Text - https://link.com</code>"))
            except Exception as e:
                 pass
            finally:
                if chat_id in user_states: del user_states[chat_id]
                if chat_id in temp_data: del temp_data[chat_id]
            return

        elif state == "wait_for_test_service" and text:
            temp_data[chat_id]["service"] = text.strip()
            user_states[chat_id] = "wait_for_test_number"
            send_message(chat_id, render_body_text("📝 Send the Number (e.g. +8801712345678):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_number" and text:
            temp_data[chat_id]["number"] = text.strip()
            user_states[chat_id] = "wait_for_test_otp"
            send_message(chat_id, render_body_text("📝 Send the OTP (e.g. 556677):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_otp" and text:
            temp_data[chat_id]["otp"] = text.strip()
            user_states[chat_id] = "wait_for_test_lang"
            send_message(chat_id, render_body_text("📝 Send the Language (e.g. EN, AR):"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_test_lang" and text:
            lang = text.strip().upper()
            if not lang.startswith("#"):
                lang = "#" + lang
                
            srv = temp_data[chat_id]["service"]
            num = temp_data[chat_id]["number"]
            otp = temp_data[chat_id]["otp"]
            
            masked = mask_number(num)
            prem_flag_html = get_flag_info_html(num)
            char, iso = get_flag_and_code(num)
            app_full_name, prem_app_html = get_service_info_html(srv)
            
            lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
            msg_text = render_body_text(f"{prem_flag_html} {iso} | {prem_app_html} {masked} | 💬 {lang_name}")
            
            for fw in bot_settings.get("fw_groups", []):
                kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                temp_row = []
                styles = ["danger", "success", "primary"]
                for i, btn in enumerate(fw.get("buttons", [])):
                    b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                    if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                    temp_row.append(b_obj)
                    if len(temp_row) == 2:
                        kb.append(temp_row)
                        temp_row = []
                if temp_row: kb.append(temp_row)
                send_message(fw["chat_id"], msg_text, reply_markup={"inline_keyboard": kb})
                
            send_message(chat_id, render_body_text(f"{PEM['ok']} Test message formatted and sent to all Forward Groups!"), reply_markup=main_menu(chat_id))
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_emoji_extract":
            entities = msg.get("entities", [])
            custom_emoji_id = None
            emoji_text = ""
            for ent in entities:
                if ent.get("type") == "custom_emoji":
                    custom_emoji_id = ent.get("custom_emoji_id")
                    offset = ent.get("offset", 0)
                    length = ent.get("length", 0)
                    b_text = msg.get("text", "").encode('utf-16-le')
                    emoji_text = b_text[offset*2:(offset+length)*2].decode('utf-16-le')
                    break
            
            if custom_emoji_id:
                temp_data[chat_id] = {"id": custom_emoji_id, "char": emoji_text}
                user_states[chat_id] = "wait_for_emoji_details"
                send_message(chat_id, render_body_text(f"{PEM['ok']} Emoji ID পাওয়া গেছে: <code>{custom_emoji_id}</code>\n\n📌 এখন এটি সেভ করার জন্য টাইপ এবং নাম লিখুন।\n\n<b>ফরমেট:</b>\n`FLAG | 880 | BD | Bangladesh`\nঅথবা\n`APP | WhatsApp`"), reply_markup=get_cancel_kb())
            else:
                send_message(chat_id, render_body_text(f"{PEM['no']} কোনো Premium Emoji পাওয়া যায়নি! দয়া করে Custom Emoji সেন্ড করুন।"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_emoji_details" and text:
            parts = [p.strip() for p in text.split("|")]
            mode = parts[0].upper()
            eid = temp_data[chat_id]["id"]
            char = temp_data[chat_id]["char"]
            
            if mode == "FLAG" and len(parts) == 4:
                code, iso, name = parts[1], parts[2], parts[3]
                bot_settings["premium_flags"][code] = {"char": char, "iso": iso.upper(), "name": name, "id": eid}
                save_db()
                send_message(chat_id, render_body_text(f"{PEM['ok']} Flag Emoji সেভ হয়েছে!\nCode: {code} | Name: {name}"), reply_markup=emoji_settings_keyboard())
            elif mode == "APP" and len(parts) == 2:
                name = parts[1]
                bot_settings["premium_apps"][name.upper()] = {"char": char, "id": eid, "name": name.title()}
                save_db()
                send_message(chat_id, render_body_text(f"{PEM['ok']} App Emoji সেভ হয়েছে!\nName: {name}"), reply_markup=emoji_settings_keyboard())
            else:
                send_message(chat_id, render_body_text(f"{PEM['no']} ফরম্যাট ভুল!\n\nসঠিক ফরম্যাট:\n`FLAG | 880 | BD | Bangladesh`\n`APP | WhatsApp`"))
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_restore" and "document" in msg:
            if is_admin(chat_id):
                restore_backup_file(chat_id, msg["document"])
            return

        elif state in ["wait_for_flag_txt", "wait_for_app_txt"] and "document" in msg:
            doc = msg["document"]
            if not doc["file_name"].endswith(".txt"):
                send_message(chat_id, render_body_text(f"{PEM['no']} Please upload a .txt file only."))
                return
            file_id = doc["file_id"]
            file_info = requests.get(f"{BASE_URL}/getFile?file_id={file_id}").json()
            file_path = file_info["result"]["file_path"]
            content = requests.get(f"{FILE_URL}{file_path}").text
            
            mode = "flags" if state == "wait_for_flag_txt" else "apps"
            count = 0
            
            if mode == "flags":
                for line in content.splitlines():
                    json_match = re.search(r'(\{.*\})', line)
                    if json_match:
                        try:
                            data = json.loads(json_match.group(1))
                            char = data.get("emoji")
                            eid = data.get("id")
                            
                            prefix_str = line[:json_match.start()].strip()
                            code_match = re.search(r'\((\d+)\)', prefix_str)
                            iso_match = re.search(r'\(([A-Za-z]+)\)', prefix_str)
                            
                            if code_match and iso_match and char and eid:
                                code = code_match.group(1)
                                iso = iso_match.group(1).upper()
                                name = prefix_str.replace(f"({code})", "").replace(f"({iso_match.group(1)})", "").replace(char, "").strip()
                                bot_settings["premium_flags"][code] = {"char": char, "iso": iso, "name": name, "id": eid}
                                count += 1
                        except Exception: pass
            else:
                for line in content.splitlines():
                    json_match = re.search(r'(\{.*\})', line)
                    if json_match:
                        try:
                            data = json.loads(json_match.group(1))
                            char = data.get("emoji")
                            eid = data.get("id")
                            
                            name_part = line[:json_match.start()].strip()
                            name = name_part.replace(char, '').strip() if char else name_part
                            
                            if char and eid and name:
                                bot_settings["premium_apps"][name.upper()] = {"char": char, "id": eid, "name": name}
                                count += 1
                        except Exception: pass
            
            save_db()
            send_message(chat_id, render_body_text(f"{PEM['ok']} Successfully loaded {count} Emojis!"), reply_markup=emoji_settings_keyboard())
            del user_states[chat_id]
            return

        elif state == "wait_for_broadcast":
            msg_id = msg["message_id"]
            send_message(chat_id, render_body_text(f"{PEM['ok']} Broadcast started..."))
            threading.Thread(target=broadcast_copymessage, args=(chat_id, msg_id)).start()
            del user_states[chat_id]
            return

        elif state == "wait_for_txt" and "document" in msg:
            doc = msg["document"]
            if not doc["file_name"].endswith(".txt"):
                send_message(chat_id, render_body_text(f"{PEM['no']} Please upload a .txt file only."))
                return
            file_id = doc["file_id"]
            file_info = requests.get(f"{BASE_URL}/getFile?file_id={file_id}").json()
            file_path = file_info["result"]["file_path"]
            file_content = requests.get(f"{FILE_URL}{file_path}").text
            
            temp_data[chat_id] = {"numbers": file_content.splitlines(), "filename": doc["file_name"]}
            user_states[chat_id] = "wait_for_service"
            send_message(chat_id, render_body_text(f"{PEM['ok']} File received.\n\n📌 Enter the service name (e.g., WHATSAPP):"), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_service" and text:
            temp_data[chat_id]["service"] = text.upper()
            user_states[chat_id] = "wait_for_country"
            send_message(chat_id, render_body_text(f"{PEM['ok']} Service set.\n\n🌍 Enter the country name (e.g., YEMEN):"), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_country" and text:
            country = text.upper()
            temp_data[chat_id]["country"] = country
            user_states[chat_id] = "wait_for_file_rate"
            send_message(chat_id, render_body_text(f"{PEM['ok']} Country set.\n\n💰 Enter the OTP reward rate for this file (e.g. 5.0):"), reply_markup=get_cancel_kb())
            return

        elif state == "wait_for_file_rate" and text:
            try:
                rate = float(text.strip())
            except ValueError:
                send_message(chat_id, render_body_text("❌ Invalid rate! Please enter a number (e.g. 5.0):"), reply_markup=get_cancel_kb())
                return
                
            country = temp_data[chat_id]["country"]
            service = temp_data[chat_id]["service"]
            raw_numbers = temp_data[chat_id]["numbers"]
            
            clean_nums = []
            for num in raw_numbers:
                num = num.strip()
                if num:
                    if not num.startswith('+'): num = '+' + num
                    clean_nums.append(num)
            
            batch_id = str(uuid.uuid4())[:8]
            number_batches[batch_id] = {
                "filename": temp_data[chat_id]["filename"], 
                "service": service, 
                "country": country, 
                "rate": rate,
                "numbers": [{"num": n, "shares": 0, "used_by": []} for n in clean_nums]
            }
            total_uploaded_stats += len(clean_nums)
            save_db()
            
            app_full_name, prem_app_html = get_service_info_html(service)
            prem_flag_html = get_flag_info_html(clean_nums[0]) if clean_nums else f"{PEM['world']} "
            
            broadcast_txt = f"➖➖➖➖➖➖➖➖\n《 NEW NUMBERS 》\n➖➖➖➖➖➖➖➖\n{prem_flag_html} {country} {prem_app_html} {service}\n➖➖➖➖➖➖➖➖\n📤 Total Added: <b>{len(clean_nums)}</b>\n💰 Reward: <b>{rate} TK</b>\n➖➖➖➖➖➖➖➖\nUse /start to get your numbers!"
            broadcast_txt = render_body_text(broadcast_txt)
            
            send_message(chat_id, render_body_text(f"{PEM['ok']} Numbers added to local stock with {rate} TK rate! Starting broadcast..."))
            
            def simple_broadcast(txt):
                b_session = requests.Session()
                url = f"{BASE_URL}/sendMessage"
                for u_id in list(all_known_users):
                    try:
                        b_session.post(url, json={"chat_id": u_id, "text": txt, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=5)
                    except Exception: pass
                    time.sleep(0.035)
            threading.Thread(target=simple_broadcast, args=(broadcast_txt,)).start()
            
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_stex_key" and text:
            bot_settings["stex_keys"].append(text.strip())
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(f"✅ StexSMS API Key Added! Total Keys: {len(bot_settings.get('stex_keys', []))}"), reply_markup=stex_control_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_voltx_key" and text:
            bot_settings["voltx_keys"].append(text.strip())
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(f"✅ Voltx API Key Added! Total Keys: {len(bot_settings.get('voltx_keys', []))}"), reply_markup=voltx_control_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_sc" and text:
            code = text.strip().replace("+", "")
            if "search_countries" not in bot_settings: bot_settings["search_countries"] = []
            bot_settings["search_countries"].append(code)
            save_db()
            delete_message(chat_id, msg["message_id"])
            kb = []
            for idx, c in enumerate(bot_settings.get("search_countries", [])):
                kb.append([{"text": f"❌ Delete {c}", "callback_data": f"del_sc_{idx}", "style": "danger"}])
            kb.append([{"text": "➕ Add Country Code", "callback_data": "add_search_country", "style": "success"}])
            kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_control", "style": "primary"}])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("🌍 <b>Allowed Search Countries:</b>\nOnly these country codes will be allowed in Search Number."), reply_markup={"inline_keyboard": kb})
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_vsc" and text:
            code = text.strip().replace("+", "")
            if "voltx_search_countries" not in bot_settings: bot_settings["voltx_search_countries"] = []
            bot_settings["voltx_search_countries"].append(code)
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": "voltx_search_country", "id": "internal"})
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_nx_srv_name" and text:
            srv = text.strip().upper()
            if "stex_services" not in bot_settings: bot_settings["stex_services"] = {}
            if srv not in bot_settings["stex_services"]: bot_settings["stex_services"][srv] = {}
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": "manage_stex_srv", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_nx_cnt_name" and text:
            cnt = text.strip()
            srv = temp_data[chat_id]["srv"]
            if cnt not in bot_settings["stex_services"][srv]: bot_settings["stex_services"][srv][cnt] = []
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"nx_srv_{srv}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_nx_addr" and text:
            srv, cnt = temp_data[chat_id]["srv"], temp_data[chat_id]["cnt"]
            new_range = text.strip().replace("+", "")
            
            if new_range not in bot_settings["stex_services"][srv][cnt]:
                bot_settings["stex_services"][srv][cnt].append(new_range)
                
                if "search_countries" not in bot_settings:
                    bot_settings["search_countries"] = []
                if new_range not in bot_settings["search_countries"]:
                    bot_settings["search_countries"].append(new_range)
                    
                save_db()
                
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"nx_cnt_{srv}_{cnt}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_vx_srv_name" and text:
            srv = text.strip().upper()
            if "voltx_services" not in bot_settings: bot_settings["voltx_services"] = {}
            if srv not in bot_settings["voltx_services"]: bot_settings["voltx_services"][srv] = {}
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": "manage_voltx_srv", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_vx_cnt_name" and text:
            cnt = text.strip()
            srv = temp_data[chat_id]["srv"]
            if cnt not in bot_settings["voltx_services"][srv]: bot_settings["voltx_services"][srv][cnt] = []
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"vx_srv_{srv}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_vx_addr" and text:
            srv, cnt = temp_data[chat_id]["srv"], temp_data[chat_id]["cnt"]
            new_range = text.strip().replace("+", "")
            
            if new_range not in bot_settings["voltx_services"][srv][cnt]:
                bot_settings["voltx_services"][srv][cnt].append(new_range)
                
                if "voltx_search_countries" not in bot_settings:
                    bot_settings["voltx_search_countries"] = []
                if new_range not in bot_settings["voltx_search_countries"]:
                    bot_settings["voltx_search_countries"].append(new_range)
                    
                save_db()
                
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"vx_cnt_{srv}_{cnt}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_for_add_zenex_key" and text:
            bot_settings["zenex_keys"].append(text.strip())
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(f"✅ Zenex API Key Added! Total Keys: {len(bot_settings.get('zenex_keys', []))}"), reply_markup=zenex_control_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_zsc" and text:
            code = text.strip().replace("+", "")
            if "zenex_search_countries" not in bot_settings: bot_settings["zenex_search_countries"] = []
            bot_settings["zenex_search_countries"].append(code)
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": "zenex_search_country", "id": "internal"})
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_zx_srv_name" and text:
            srv = text.strip().upper()
            if "zenex_services" not in bot_settings: bot_settings["zenex_services"] = {}
            if srv not in bot_settings["zenex_services"]: bot_settings["zenex_services"][srv] = {}
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": "manage_zenex_srv", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_zx_cnt_name" and text:
            cnt = text.strip()
            srv = temp_data[chat_id]["srv"]
            if cnt not in bot_settings["zenex_services"][srv]: bot_settings["zenex_services"][srv][cnt] = []
            save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"zx_srv_{srv}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_zx_addr" and text:
            srv, cnt = temp_data[chat_id]["srv"], temp_data[chat_id]["cnt"]
            new_range = text.strip().replace("+", "")
            if new_range not in bot_settings["zenex_services"][srv][cnt]:
                bot_settings["zenex_services"][srv][cnt].append(new_range)
                if "zenex_search_countries" not in bot_settings: bot_settings["zenex_search_countries"] = []
                if new_range not in bot_settings["zenex_search_countries"]: bot_settings["zenex_search_countries"].append(new_range)
                save_db()
            delete_message(chat_id, msg["message_id"])
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": temp_data[chat_id]["msg_id"]}, "data": f"zx_cnt_{srv}_{cnt}", "id": "internal"})
            del user_states[chat_id]
            return

        elif state == "wait_for_add_wm" and text:
            bot_settings["w_methods"].append(text.strip())
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_fj" and text:
            bot_settings["fj_channels"].append(parse_chat_id(text))
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("🔗 <b>FORCE JOIN SYSTEM</b>\nManage channels below:\n<i>(Note: For private links, use numeric IDs like -100...)</i>"), reply_markup=fj_settings_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return
            
        elif state == "wait_for_add_adm" and text:
            if text.isdigit():
                bot_settings["admins"].append(int(text))
                save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("👥 <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_add_fw_id" and text:
            bot_settings["fw_groups"].append({"chat_id": text.strip(), "buttons": []})
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return
            
        elif state == "wait_for_add_fw_btn" and text:
            fw_idx = temp_data[chat_id]["fw_idx"]
            if "-" in text:
                parts = text.split("-", 1)
                btn_text = parts[0].strip()
                btn_url = parts[1].strip()
                
                emoji_id = None
                emoji_char = ""
                for ent in msg.get("entities", []):
                    if ent.get("type") == "custom_emoji":
                        emoji_id = ent.get("custom_emoji_id")
                        offset = ent.get("offset", 0)
                        length = ent.get("length", 0)
                        b_text = text.encode('utf-16-le')
                        emoji_char = b_text[offset*2:(offset+length)*2].decode('utf-16-le')
                        break
                
                if emoji_char:
                    btn_text = btn_text.replace(emoji_char, "").strip()
                    
                btn_data = {"text": btn_text, "url": btn_url}
                if emoji_id:
                    btn_data["icon_custom_emoji_id"] = emoji_id
                    
                bot_settings["fw_groups"][fw_idx]["buttons"].append(btn_data)
                save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(f"🛡 <b>Manage Group:</b> {bot_settings['fw_groups'][fw_idx]['chat_id']}"), reply_markup=specific_fw_group_keyboard(fw_idx))
            del user_states[chat_id]
            del temp_data[chat_id]
            return
            
        elif state == "wait_for_otp_link" and text:
            bot_settings["otp_link"] = text.strip()
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return
        elif state == "wait_for_otp_channel_link" and text:
            bot_settings['otp_channel_link'] = text.strip()
            save_db()
            delete_message(chat_id, msg['message_id'])
            edit_message(chat_id, temp_data[chat_id]['msg_id'], render_body_text("🛡️ <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_range_group_id" and text:
            gid = text.strip().replace(" ", "")
            bot_settings["range_group_id"] = gid
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(
                f"✅ Range GROUP set: <code>{gid}</code>\n\n🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"
            ), reply_markup=otp_groups_list_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_range_group_link" and text:
            link = text.strip()
            bot_settings["range_group_link"] = link
            save_db()
            delete_message(chat_id, msg["message_id"])
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(
                f"✅ Active GROUP Link saved.\n\n🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"
            ), reply_markup=otp_groups_list_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_panel_name" and text:
            p_name = text.strip()
            t_key = temp_data[chat_id].get("add_type", "api")
            msg_id = temp_data[chat_id]["msg_id"]
            delete_message(chat_id, msg["message_id"])
            
            if t_key == "logc":
                user_states[chat_id] = "wait_for_cpanel_url"
                temp_data[chat_id] = {"msg_id": msg_id, "p_data": {
                    "name": p_name, "type": "Auto Captcha Panel", "status": "ON", "records": 0, "login_status": "⏳ Pending First Login"
                }}
                edit_message(chat_id, msg_id, render_body_text("1️⃣ <b>Login URL</b>\n➡️ Panel এর Login Link দিন:"), reply_markup=get_cancel_kb())
                return
            else:
                bot_settings["panels"].append({
                    "name": p_name, "type": "API Panel", "status": "OFF", "api_url": "", "token": "", "records": 0
                })
                save_db()
                handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_api_panels", "id": "internal"})
                if chat_id in user_states: del user_states[chat_id]
                if chat_id in temp_data: del temp_data[chat_id]
                return

        elif state == "wait_for_p_api" and text:
            idx = temp_data[chat_id]["p_idx"]
            bot_settings["panels"][idx]["api_url"] = text.strip()
            save_db()
            delete_message(chat_id, msg["message_id"])
            p = bot_settings["panels"][idx]
            ui_text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Token:</b> <code>{p.get('token', 'None')}</code>"
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(ui_text), reply_markup=panel_config_keyboard(idx))
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_p_tok" and text:
            idx = temp_data[chat_id]["p_idx"]
            bot_settings["panels"][idx]["token"] = text.strip()
            save_db()
            delete_message(chat_id, msg["message_id"])
            p = bot_settings["panels"][idx]
            ui_text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Token:</b> <code>{p.get('token', 'None')}</code>"
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(ui_text), reply_markup=panel_config_keyboard(idx))
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_p_fapi" and text:
            idx = temp_data[chat_id]["p_idx"]
            bot_settings["panels"][idx]["full_api_url"] = text.strip()
            save_db()
            delete_message(chat_id, msg["message_id"])
            p = bot_settings["panels"][idx]
            ui_text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Full API URL:</b> <code>{p.get('full_api_url', 'None')}</code>"
            edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(ui_text), reply_markup=panel_config_keyboard(idx))
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_p_rec" and text:
            if text.isdigit():
                idx = temp_data[chat_id]["p_idx"]
                bot_settings["panels"][idx]["records"] = int(text)
                save_db()
                delete_message(chat_id, msg["message_id"])
                p = bot_settings["panels"][idx]
                
                ui_text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Token:</b> <code>{p.get('token', 'None')}</code>"
                edit_message(chat_id, temp_data[chat_id]["msg_id"], render_body_text(ui_text), reply_markup=panel_config_keyboard(idx))
            else:
                send_message(chat_id, render_body_text("❌ Please enter a valid number! Try again."), reply_markup=get_cancel_kb())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "set_dxa":
            msg_id = temp_data[chat_id]["msg_id"]
            key = temp_data[chat_id]["key"]
            try:
                if key in ["min_withdraw", "otp_reward", "refer_reward", "refer_otp_reward"]: bot_settings[key] = _pos_float(text)
                elif key in ["cooldown", "num_req", "num_share", "refer_min_otps"]: bot_settings[key] = max(0, int(text))
                elif key == "support_link":
                    bot_settings[key] = normalize_support_url(text)
                else: bot_settings[key] = text
                save_db()
                delete_message(chat_id, msg["message_id"])
                edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>"), reply_markup=dxa_control_keyboard())
            except Exception:
                delete_message(chat_id, msg["message_id"])
                edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>\n\n❌ Invalid value!"), reply_markup=dxa_control_keyboard())
            del user_states[chat_id]
            del temp_data[chat_id]
            return

        elif state == "wait_for_search" and text:
            query = text.strip().replace("+", "")
            if not query.isdigit() or len(query) < 3 or len(query) > 9:
                send_message(chat_id, render_body_text("❌ Please enter a valid 3 to 9 digit number!"))
                return
                
            wait_msg = send_message(chat_id, render_body_text("⌛ <i>Processing... Finding Number...</i>"))
            wait_msg_id = wait_msg.get("result", {}).get("message_id")
            
            # 🌟 ১. প্রথমে Local থেকে নাম্বার খুঁজবে (যে কোনো দেশের জন্য)
            found_indices = []
            for b_id, b_data in number_batches.items():
                for idx, n_obj in enumerate(b_data["numbers"]):
                    if n_obj["num"].replace("+", "").startswith(query) and chat_id not in n_obj.get("used_by", []):
                        found_indices.append((b_id, idx, n_obj["num"]))
            
            fetched_nums = []
            if not found_indices:
                # 🌟 ২. যদি Local এ না পায়, তখন চেক করবে Voltx বা StexSMS থেকে আনা যাবে কি না
                stex_allowed = bot_settings.get("search_countries", [])
                voltx_allowed = bot_settings.get("voltx_search_countries", [])
                
                # ফিক্স: অ্যাডমিন প্যানেলে দেশ এড না থাকলে অটোমেটিক Block করে দেবে
                is_stex_allowed = any(query.startswith(c) for c in stex_allowed) if stex_allowed else False
                is_voltx_allowed = any(query.startswith(c) for c in voltx_allowed) if voltx_allowed else False
                
                if not is_stex_allowed and not is_voltx_allowed:
                    if wait_msg_id: delete_message(chat_id, wait_msg_id)
                    send_message(chat_id, render_body_text("❌ This country code is not allowed!"), reply_markup=main_menu(chat_id))
                    del user_states[chat_id]
                    return
                    
                if wait_msg_id: edit_message(chat_id, wait_msg_id, render_body_text("⌛ <i>Processing... Finding Number via API...</i>"))
                
                is_voltx_used = False
                req_count = bot_settings.get("num_req", 1)
                
                # 🌟 প্রথমে Voltx চেক করবে
                if is_voltx_allowed:
                    voltx_keys = bot_settings.get("voltx_keys", [])
                    for _ in range(req_count):
                        if len(fetched_nums) >= req_count: break
                        for api_key in voltx_keys:
                            try:
                                headers = {"mauthapi": api_key}
                                res = requests.post(f"{VOLTX_BASE_URL}/getnum", json={"rid": query}, headers=headers, timeout=10)
                                resp_data = res.json()
                                if resp_data.get("meta", {}).get("code") == 200 and resp_data.get("data"):
                                    num_str = str(resp_data["data"].get("no_plus_number", "")).replace("+", "")
                                    if not num_str: num_str = str(resp_data["data"].get("national_number", ""))
                                    fetched_nums.append(num_str)
                                    voltx_assigned_numbers[num_str] = chat_id 
                                    is_voltx_used = True
                                    global total_assigned_stats
                                    total_assigned_stats += 1
                                    break # শুধু api_key লুপ ব্রেক করবে, যাতে পরের নাম্বার আনতে পারে
                            except Exception: continue

                # 🌟 Voltx এ না পেলে বা আরও নাম্বার লাগলে StexSMS তে চেক করবে
                if len(fetched_nums) < req_count and is_stex_allowed:
                    stex_keys = bot_settings.get("stex_keys", [])
                    
                    for _ in range(req_count - len(fetched_nums)): 
                        for api_key in stex_keys:
                            try:
                                headers = {"mauthapi": api_key}
                                res = requests.post(f"{STEX_BASE_URL}/getnum", json={"rid": query}, headers=headers, timeout=10)
                                data = res.json()
                                if data.get("meta", {}).get("code") == 200 and data.get("data"):
                                    num_str = str(data["data"].get("no_plus_number", "")).replace("+", "")
                                    if not num_str: num_str = str(data["data"].get("national_number", ""))
                                    fetched_nums.append(num_str)
                                    stex_assigned_numbers[num_str] = chat_id 
                                    total_assigned_stats += 1
                                    break 
                            except Exception: continue
                        
                if not fetched_nums:
                    if wait_msg_id: delete_message(chat_id, wait_msg_id)
                    send_message(chat_id, render_body_text("❌ Number out of stock!"), reply_markup=main_menu(chat_id))
                    del user_states[chat_id]
                    return
                save_db()
            else:
                fetched_nums = claim_numbers(found_indices, chat_id, bot_settings.get("num_req", 1), with_rate=False)
                if not fetched_nums:
                    send_message(chat_id, render_body_text(f"{PEM['no']} Number out of stock!"), reply_markup=main_menu(chat_id))
                    user_states.pop(chat_id, None)
                    return
                
            kb = []
            flags_db = bot_settings.get("premium_flags", {})
            country_emoji_html = ""
            for num in fetched_nums:
                char_flag, iso = get_flag_and_code(num)
                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                flag_emoji_id = "5780471598922337683"
                for flag_code, flag_data in flags_db.items():
                    if iso == flag_data.get("iso"):
                        if "id" in flag_data: 
                            flag_emoji_id = flag_data["id"]
                            country_emoji_html = f'<tg-emoji emoji-id="{flag_emoji_id}">{flag_data["char"]}</tg-emoji>'
                        break
                if not country_emoji_html: country_emoji_html = char_flag
                kb.append([{"text": f"{strip_cc(display_num)}", "icon_custom_emoji_id": flag_emoji_id, "copy_text": {"text": strip_cc(display_num)}, "style": "success"}])
            
            vtx_ext = "_vtx" if 'is_voltx_used' in locals() and is_voltx_used else ""
            kb.append([{"text": "Change Number", "icon_custom_emoji_id": "5465368548702446780", "callback_data": f"c_n_s_{query}{vtx_ext}", "style": "danger"}])
            
            c_btns = bot_settings["custom_messages"].get("search_number", {}).get("buttons", [])
            for c_b in c_btns: 
                b_copy = c_b.copy()
                if "style" not in b_copy: b_copy["style"] = "primary"
                kb.append([b_copy])
                
            kb.append([{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
            
            custom_star_html = f'<tg-emoji emoji-id="5305517382138112561">⭐️</tg-emoji>'
            text_numbers = render_body_text(f"{country_emoji_html} {custom_star_html} NEW NUMBER\n")
            
            if wait_msg_id:
                edit_message(chat_id, wait_msg_id, text_numbers, reply_markup={"inline_keyboard": kb})
                user_active_sessions[chat_id] = {"msg_id": wait_msg_id, "nums": fetched_nums}
            else:
                msg_res = send_message(chat_id, text_numbers, reply_markup={"inline_keyboard": kb})
                if msg_res and "result" in msg_res:
                    user_active_sessions[chat_id] = {"msg_id": msg_res["result"]["message_id"], "nums": fetched_nums}
            return
            
        elif state == "wait_for_withdraw_amount" and text:
            msg_id_to_edit = temp_data[chat_id].get("msg_id")
            try:
                amount = parse_amount(text)
                if amount is None: raise ValueError("bad amount")
                bal = temp_data[chat_id]["balance"]
                min_w = bot_settings['min_withdraw']
                
                if amount < min_w:
                    if msg_id_to_edit: edit_message(chat_id, msg_id_to_edit, render_body_text(f"❌ Minimum withdrawal is {min_w} ৳!\n💰 Balance: {bal} ৳\n\n📝 Enter again:"), reply_markup=get_cancel_kb())
                    return
                if amount > bal:
                    if msg_id_to_edit: edit_message(chat_id, msg_id_to_edit, render_body_text(f"❌ You don't have enough balance!\n💰 Balance: {bal} ৳\n\n📝 Enter again:"), reply_markup=get_cancel_kb())
                    return
                    
                temp_data[chat_id]["amount"] = amount
                user_states[chat_id] = "wait_for_withdraw_number"
                if msg_id_to_edit:
                    edit_message(chat_id, msg_id_to_edit, render_body_text(f"✅ Amount: {amount} ৳\n\n📱 Now send your <b>{temp_data[chat_id]['method']}</b> account number:"), reply_markup=get_cancel_kb())
            except ValueError:
                if msg_id_to_edit: edit_message(chat_id, msg_id_to_edit, render_body_text("❌ Invalid amount!\n\n📝 Please send a valid number:"), reply_markup=get_cancel_kb())
            return
            
        elif state == "wait_for_2fa_key" and text:
            msg_id_to_edit = temp_data.get(chat_id, {}).get("msg_id")
            delete_message(chat_id, msg.get("message_id")) # ইউজারের মেসেজ ডিলিট

            if not msg_id_to_edit:
                send_message(chat_id, render_body_text("❌ Error: Message not found. Try again."))
                del user_states[chat_id]
                return

            try:
                secret = text.strip().replace(" ", "")
                totp = pyotp.TOTP(secret)
                code = totp.now()
                remaining_time = 30 - (int(time.time()) % 30)
                
                success_txt = (
                    f"━━━━━━━━━━━━━━━\n"
                    f"《 🔐 <b>2FA CODE</b> 》\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"🔐 <b>CODE:</b> <code>{code}</code>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"🕓 <b>EXPIRES IN:</b> {remaining_time}s\n"
                    f"━━━━━━━━━━━━━━━"
                )
                kb = [[{"text": f"Click to copy {code}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": code}, "style": "success"}],
                      [{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": f"ref_2fa_{_2fa_token(secret)}", "style": "primary"},
                       {"text": "New Code", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "gen_2fa", "style": "danger"}],
                      [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
                
                edit_message(chat_id, msg_id_to_edit, render_body_text(success_txt), reply_markup={"inline_keyboard": kb})
                del user_states[chat_id]
                if chat_id in temp_data: del temp_data[chat_id]
            except Exception:
                error_txt = "━━━━━━━━━━━━━━━\n《 🔑 <b>ENTER 2FA KEY</b> 》\n━━━━━━━━━━━━━━━\n📝 <b>SEND YOUR 2FA SECRET KEY</b>\n━━━━━━━━━━━━━━━\n❌ <b>Invalid Secret Key! Try again.</b>\n━━━━━━━━━━━━━━━"
                cancel_kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_2fa", "style": "danger"}]]}
                edit_message(chat_id, msg_id_to_edit, render_body_text(error_txt), reply_markup=cancel_kb)
            return

        elif state == "wait_for_withdraw_number":
            msg_id_to_edit = temp_data[chat_id].get("msg_id")
            
            method = temp_data[chat_id]["method"]
            amount = temp_data[chat_id]["amount"]
            number = html.escape(str(text).strip())[:40]
            if not number:
                send_message(chat_id, render_body_text(f"{PEM['no']} Invalid account number! Send it again:"), reply_markup=get_cancel_kb())
                return
            req_id = f"W_{str(uuid.uuid4())[:6].upper()}"
            
            first_name = msg.get("from", {}).get("first_name", "User")
            last_name = msg.get("from", {}).get("last_name", "")
            full_name = html.escape(f"{first_name} {last_name}".strip())
            
            if not deduct_balance_if_enough(chat_id, amount, "withdraw", req_id):
                send_message(chat_id, render_body_text(f"{PEM['no']} Not enough balance for this withdrawal."), reply_markup=main_menu(chat_id))
                user_states.pop(chat_id, None)
                temp_data.pop(chat_id, None)
                return
            pending_withdrawals[req_id] = {"user_id": chat_id, "amount": amount, "method": method, "number": number, "full_name": full_name}
            
            # Save to Firestore for History
            if db:
                try:
                    db.collection('withdrawals').document(req_id).set({
                        "user_id": str(chat_id),
                        "amount": amount,
                        "method": method,
                        "status": "pending",
                        "number": number,
                        "full_name": full_name,
                        "timestamp": SERVER_TIMESTAMP
                    })
                except Exception: pass
                
            if bot_settings["w_group"]:
                admin_msg = f"🎙 <b>NEW WITHDRAWAL REQUEST</b>\n\n👤 <b>USER:</b> <a href='tg://user?id={chat_id}'>{full_name}</a>\n💳 <b>WITHDRAWAL:</b> {amount} TK\n🍏 <b>NUMBER:</b> <code>{number}</code>\n🏦 <b>METHOD:</b> {method}\n\n🧾 <b>REQ ID:</b> {req_id}\n👨‍⚖️ <b>PROCESSED BY ADMIN</b>"
                kb = {"inline_keyboard": [[{"text": "APPROVE", "icon_custom_emoji_id": "5352694861990501856", "callback_data": f"wapp_{req_id}", "style": "success"}, {"text": "REJECT", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"wrej_{req_id}", "style": "danger"}]]}
                send_message(bot_settings["w_group"], render_body_text(admin_msg), reply_markup=kb)
            
            kb = {"inline_keyboard": [[{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]}
            success_text = f"{PEM['ok']} Your withdrawal request has been submitted!\n\n🧾 <b>Req ID:</b> {req_id}\n💰 <b>Amount:</b> {amount} ৳\n🏦 <b>Method:</b> {method}\n📱 <b>Number:</b> <code>{number}</code>"
            
            if msg_id_to_edit:
                edit_message(chat_id, msg_id_to_edit, render_body_text(success_text), reply_markup=kb)
            else:
                send_message(chat_id, render_body_text(success_text), reply_markup=kb)
                
            del user_states[chat_id]
            del temp_data[chat_id]
            return

    # --- Regular Commands ---
    if text.startswith("/start"):
        get_user(chat_id)
        
        # Deep link from Range Group "Get number" → open bot + fetch number from that range
        parts = text.split()
        if len(parts) > 1 and parts[1].startswith("rng_get_"):
            fake_call = {
                "id": "internal",
                "from": {"id": chat_id},
                "message": {"chat": {"id": chat_id}, "message_id": 0},
                "data": parts[1]
            }
            handle_callback(fake_call)
            return
        
        # --- PROCESS PENDING REFERRAL (paid after the invited user gets enough OTPs) ---
        maybe_pay_referral(chat_id)
                    
        c_msg = bot_settings["custom_messages"].get("start", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['hi']} Welcome!"))
        kb = []
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
        
        if kb:
            send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})
            send_message(chat_id, render_body_text(f"{PEM['gear']} Navigation Menu:"), reply_markup=main_menu(chat_id))
        else:
            send_message(chat_id, txt, reply_markup=main_menu(chat_id))
            
    elif text == "TRAFFIC":
        txt, markup = build_traffic_ui()
        send_message(chat_id, txt, reply_markup=markup)
        
    elif text == "Refer":
        u_data = get_user(chat_id)
        ref_link = f"https://t.me/{BOT_USERNAME}?start={chat_id}"
        c_msg = bot_settings["custom_messages"].get("refer", {})
        
        raw_txt = c_msg.get("text", f"{PEM['gift']} Refer").replace("{ref_link}", ref_link).replace("{total_ref}", str(u_data.get('total_refers', 0))).replace("{ref_reward}", str(refer_reward_amount()))
        txt = render_body_text(raw_txt)
        
        kb = [[{"text": "COPY LINK", "icon_custom_emoji_id": "5192739271886282680", "copy_text": {"text": ref_link}, "style": "success"}]]
        for b in c_msg.get("buttons", []): 
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
        kb.append([{"text": "CLOSE", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == "WITHDRAWAL":
        if not bot_settings["withdraw_on"]:
            send_message(chat_id, render_body_text(f"{PEM['no']} Withdrawals are currently disabled."))
            return
        
        u_data = get_user(chat_id)
        bal = u_data.get('balance', 0.0)
        
        c_msg = bot_settings["custom_messages"].get("withdrawal", {})
        raw_txt = c_msg.get("text", "Withdrawal").replace("{bal}", str(bal)).replace("{total_otp}", str(u_data.get('total_otps', 0))).replace("{total_ref}", str(u_data.get('total_refers', 0))).replace("{min_w}", str(bot_settings['min_withdraw']))
        txt = render_body_text(raw_txt)
        
        kb = []
        for m in bot_settings["w_methods"]:
            kb.append([{"text": m.strip(), "icon_custom_emoji_id": "5190899075968441286", "callback_data": f"sel_wm_{m.strip()}", "style": "primary"}])
        
        for b in c_msg.get("buttons", []): 
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
        kb.append([{"text": "Cancel", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == "Admin Panel" and is_admin(chat_id):
        send_message(chat_id, get_admin_text(), reply_markup=admin_panel_keyboard())

    elif text == "GET NUMBER":
        local_srvs = set([b["service"] for b in number_batches.values() if b["numbers"]])
        stex_srvs = set(bot_settings.get("stex_services", {}).keys())
        voltx_srvs = set(bot_settings.get("voltx_services", {}).keys())
        zenex_srvs = set(bot_settings.get("zenex_services", {}).keys())
        all_services = local_srvs.union(stex_srvs).union(voltx_srvs).union(zenex_srvs)
        
        if not all_services:
            send_message(chat_id, render_body_text(f"{PEM['no']} No numbers or services available!"))
        else:
            c_msg = bot_settings["custom_messages"].get("get_number", {})
            txt = render_body_text(c_msg.get("text", f"{PEM['pin']} Select Service"))
            
            apps_db = bot_settings.get("premium_apps", {})
            kb = []
            for s in all_services:
                emoji_id = "5352694861990501856" # Default icon
                for app_key, app_data in apps_db.items():
                    if s.upper() == app_key or s.upper() in app_key or app_key in s.upper():
                        if "id" in app_data:
                            emoji_id = app_data["id"]
                            break
                kb.append([{"text": f"{s}", "icon_custom_emoji_id": emoji_id, "callback_data": f"g_s_{s}", "style": "primary"}])
            
            for b in c_msg.get("buttons", []): 
                b_copy = b.copy()
                if "style" not in b_copy: b_copy["style"] = "primary"
                kb.append([b_copy])
            kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
            
            send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == "Search Number":
        user_states[chat_id] = "wait_for_search"
        c_msg = bot_settings["custom_messages"].get("search_number", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['num']} Search Number"))
        kb = [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_state", "style": "danger"}]]
        for b in c_msg.get("buttons", []): 
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb})

    elif text == "LEADERBOARD":
        txt_, kb_ = build_user_lb(chat_id, "t")
        send_message(chat_id, render_body_text(txt_), reply_markup=kb_)

    elif text == "PROFILE":
        txt_, kb_ = build_my_profile(chat_id)
        send_message(chat_id, render_body_text(txt_), reply_markup=kb_)

    elif text == "2FA ONLINE" or text == "🔐 2FA ONLINE":
        txt = "━━━━━━━━━━━━━━━\n《 🔐 <b>2FA ONLINE</b> 》\n━━━━━━━━━━━━━━━\n<i>Generate your 2FA security code instantly using your secret key.</i>\n━━━━━━━━━━━━━━━"
        kb = [[{"text": "Generate 2fa code", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "gen_2fa", "style": "success"}],
              [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
        send_message(chat_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif text == "SUPPORT":
        c_msg = bot_settings["custom_messages"].get("support", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['msg']} Support"))
        if not txt.strip(): txt = render_body_text(f"{PEM['msg']} Support")
        kb = []
        for b in c_msg.get("buttons", []):
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
            
        sup_link = bot_settings.get("support_link", "")
        if sup_link:
            kb.insert(0, [{"text": "Contact Support", "icon_custom_emoji_id": "5337302974806922068", "url": sup_link, "style": "success"}])
            
        kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        send_message(chat_id, txt, reply_markup={"inline_keyboard": kb} if kb else None)

def expire_previous_number(chat_id):
    if chat_id in user_active_sessions:
        prev_data = user_active_sessions[chat_id]
        prev_msg_id = prev_data["msg_id"]
        nums = prev_data["nums"]
        
        # API সিস্টেম থেকে রিমুভ করা যাতে ইনবক্সে আর মেসেজ না যায়
        for num in nums:
            if num in stex_assigned_numbers: del stex_assigned_numbers[num]
            if num in voltx_assigned_numbers: del voltx_assigned_numbers[num]
            if num in zenex_assigned_numbers: del zenex_assigned_numbers[num]
        save_db()
        
        # আগের মেসেজ ইডিট করে Expired বাটন বসানো
        kb = [[{"text": "Number Expired", "icon_custom_emoji_id": "5336997731481193790", "callback_data": "ignore", "style": "danger"}]]
        try:
            edit_message(chat_id, prev_msg_id, "NEW NUMBER\n", reply_markup={"inline_keyboard": kb})
        except Exception:
            pass
        del user_active_sessions[chat_id]

# ==========================================
# Callback Query Handler
# ==========================================
ADMIN_CB_PREFIXES = ('user_management', 'um_', 'ui_', 'menu_design_list', 'md_', 'manage_emojis', 'up_flags_txt', 'up_apps_txt', 'add_single_emoji', 'dl_flags_txt', 'dl_apps_txt', 'del_all_flags', 'broadcast_msg', 'upload_num', 'delete_files', 'del_b_', 'show_used', 'show_unused', 'dl_used', 'dl_unused', 'lb_', 'alb_', 'drpt_', 'ls_', 'back_to_admin', 'system_settings', 'stex_control', 'toggle_', 'add_stex_key', 'view_stex_keys', 'del_nxa_', 'stex_search_country', 'add_search_country', 'del_sc_', 'manage_stex_srv', 'nx_', 'voltx_control', 'add_voltx_key', 'view_voltx_keys', 'del_vtx_', 'voltx_search_country', 'add_voltx_search_country', 'del_vsc_', 'manage_voltx_srv', 'vx_', 'zenex_control', 'add_zenex_key', 'view_zenex_keys', 'del_znx_', 'zenex_search_country', 'add_zenex_search_country', 'del_zsc_', 'manage_zenex_srv', 'zx_', 'manage_fj', 'add_fj', 'del_fj_', 'manage_admins', 'add_adm', 'del_adm_', 'manage_otp_groups', 'add_fw', 'manage_fw_', 'add_fwbtn_', 'del_fwbtn_', 'del_fw_', 'edit_otp_link', 'edit_otp_channel_link', 'add_range_group', 'edit_range_group_link', 'manage_panels', 'manage_api_panels', 'add_api_panel', 'add_ptype_', 'list_del_api', 'do_del_pnl_', 'tog_pnl_', 'conf_pnl_', 'set_p_', 'dxa_', 'manage_w_methods', 'add_wm', 'del_wm_', 'test_p_conn_', 'test_message_flow', 'cancel_dxa_edit')

def handle_callback(call):
    global total_assigned_stats
    chat_id = call["message"]["chat"]["id"]
    chat_type = call["message"]["chat"].get("type", "private")
    data = call.get("data", "")

    # 🌟 Button Loading Fix: বাটন চাপার সাথে সাথেই টেলিগ্রামকে Response দিয়ে দেওয়া, যাতে বাটন আটকে না থাকে!
    if not data.startswith("test_p_conn_") and not data.startswith("c_n_") and not data.startswith("g_c_") and not data.startswith("rng_get_"):
        try: threading.Thread(target=answer_callback, args=(call["id"],)).start()
        except Exception: pass

    # Allow Get number from Range GROUP (rng_get_) + withdraw admin buttons
    if chat_type != "private" and not (
        data.startswith("wapp_") or data.startswith("wrej_") or data.startswith("rng_get_")
    ):
        return

    msg_id = call["message"]["message_id"]

    if chat_type == "private":
        if is_user_banned(chat_id):
            answer_callback(call["id"], "🚫 You are banned from using this bot!", show_alert=True)
            return

        if not check_force_join(chat_id) and data != "check_fj":
            send_force_join_msg(chat_id)
            return

    if data.startswith(ADMIN_CB_PREFIXES) and not is_admin(call.get("from", {}).get("id", chat_id)):
        answer_callback(call.get("id", ""), "Admin only!", show_alert=True)
        return

    if data == "check_fj":
        if check_force_join(chat_id):
            delete_message(chat_id, msg_id)
            send_message(chat_id, render_body_text(f"{PEM['ok']} Thanks for joining! You can now use the bot."), reply_markup=main_menu(chat_id))
            
            # --- PROCESS PENDING REFERRAL (paid after the invited user gets enough OTPs) ---
            maybe_pay_referral(chat_id)
        else:
            answer_callback(call["id"], "❌ You haven't joined all channels yet!", show_alert=True)
        return

    if data == "close_msg":
        delete_message(chat_id, msg_id)

    elif data == "my_profile":
        txt_, kb_ = build_my_profile(chat_id)
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)
        answer_callback(call["id"], "✅ Updated")
        
    elif data == "cancel_state":
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in temp_data: del temp_data[chat_id]
        delete_message(chat_id, msg_id)

    elif data == "cancel_2fa":
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in temp_data: del temp_data[chat_id]
        txt = "━━━━━━━━━━━━━━━\n《 🔐 <b>2FA ONLINE</b> 》\n━━━━━━━━━━━━━━━\n<i>Generate your 2FA security code instantly using your secret key.</i>\n━━━━━━━━━━━━━━━"
        kb = [[{"text": "Generate 2fa code", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "gen_2fa", "style": "success"}],
              [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    elif data == "gen_2fa":
        user_states[chat_id] = "wait_for_2fa_key"
        temp_data[chat_id] = {"msg_id": msg_id}
        txt = "━━━━━━━━━━━━━━━\n《 🔑 <b>ENTER 2FA KEY</b> 》\n━━━━━━━━━━━━━━━\n📝 <b>SEND YOUR 2FA SECRET KEY</b>\n━━━━━━━━━━━━━━━"
        kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_2fa", "style": "danger"}]]}
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup=kb)
        answer_callback(call["id"])

    elif data.startswith("ref_2fa_"):
        secret = twofa_tokens.get(data.replace("ref_2fa_", ""))
        if not secret:
            answer_callback(call.get("id", ""), "Session expired. Press New Code.", show_alert=True)
            return
        try:
            totp = pyotp.TOTP(secret)
            code = totp.now()
            remaining_time = 30 - (int(time.time()) % 30)
            
            success_txt = (
                f"━━━━━━━━━━━━━━━\n"
                f"《 🔐 <b>2FA CODE</b> 》\n"
                f"━━━━━━━━━━━━━━━\n"
                f"🔐 <b>CODE:</b> <code>{code}</code>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"🕓 <b>EXPIRES IN:</b> {remaining_time}s\n"
                f"━━━━━━━━━━━━━━━"
            )
            kb = [[{"text": f"Click to copy {code}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": code}, "style": "success"}],
                  [{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": f"ref_2fa_{_2fa_token(secret)}", "style": "primary"},
                   {"text": "New Code", "icon_custom_emoji_id": "5352552689983067014", "callback_data": "gen_2fa", "style": "danger"}],
                  [{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}]]
            
            edit_message(chat_id, msg_id, render_body_text(success_txt), reply_markup={"inline_keyboard": kb})
        except Exception:
            answer_callback(call["id"], "❌ Error refreshing code!", show_alert=True)

    elif data == "cancel_dxa_edit":
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in temp_data: del temp_data[chat_id]
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>"), reply_markup=dxa_control_keyboard())
        
    elif data == "dummy_alert":
        answer_callback(call["id"], "This feature will be added later!", show_alert=True)
        
    elif data == "refresh_traffic":
        txt, markup = build_traffic_ui()
        edit_message(chat_id, msg_id, txt, reply_markup=markup)
        answer_callback(call["id"], "✅ Traffic Refreshed!", show_alert=False)

    elif data.startswith("exp_rng_"):
        srv_query = data.replace("exp_rng_", "")
        
        country_stats = {}
        current_time = time.time()
        for t in recent_traffic:
            if current_time - t.get("time", 0) <= 3600:
                if t.get("service", "").startswith(srv_query):
                    iso = t.get("iso", "XX")
                    flag = t.get("flag", "🌍")
                    if iso not in country_stats:
                        country_stats[iso] = {"count": 0, "flag": flag}
                    country_stats[iso]["count"] += 1
        
        if not country_stats:
            answer_callback(call["id"], "❌ No recent traffic found for this service!", show_alert=True)
            return
            
        kb = []
        for iso, c_data in sorted(country_stats.items(), key=lambda x: x[1]["count"], reverse=True):
            count = c_data["count"]
            c_name = iso
            emoji_id = "5780471598922337683"
            for code, fdata in bot_settings.get("premium_flags", {}).items():
                if fdata.get("iso") == iso:
                    c_name = fdata.get("name", iso)
                    if "id" in fdata: emoji_id = fdata["id"]
                    break
            
            btn_text = f"{c_name} ({iso}) - {count} OTP"
            kb.append([{"text": btn_text, "icon_custom_emoji_id": emoji_id, "callback_data": f"exp_c_{srv_query}_{iso}", "style": "primary"}])
            
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "refresh_traffic", "style": "danger"}])
        
        app_full_name, prem_app_html = get_service_info_html(srv_query)
        edit_message(chat_id, msg_id, render_body_text(f"📊 <b>Explore Service: {prem_app_html} {app_full_name}</b>\n\nSelect a country to view available ranges:"), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    elif data.startswith("exp_c_"):
        parts = data.split("_")
        srv_query = parts[2]
        iso_query = parts[3]
        
        nums = []
        current_time = time.time()
        for t in recent_traffic:
            if current_time - t.get("time", 0) <= 3600:
                if t.get("service", "").startswith(srv_query) and t.get("iso") == iso_query:
                    # 🌟 X রিমুভ করে ক্লিন রেঞ্জ নেওয়া হলো
                    num = t.get("real_range", t.get("number", "").replace("X", "").replace("x", "")).replace("+", "").strip()
                    if num: nums.append(num)
        
        if not nums:
            answer_callback(call["id"], "❌ No recent numbers found for this country!", show_alert=True)
            return
            
        # শুধুমাত্র StexSMS Services থেকে রেঞ্জ নিবো (Search Countries নিবো না, কারণ ওগুলোতে শুধু দেশের কোড থাকে)
        known_ranges = set()
        for s_name, c_dict in bot_settings.get("stex_services", {}).items():
            for c_name, r_list in c_dict.items():
                for r in r_list:
                    # 🌟 এখানেও সেভ করা রেঞ্জ থেকে X সরিয়ে নিচ্ছি
                    known_ranges.add(r.replace("X", "").replace("x", ""))
                    
        sorted_known = sorted(list(known_ranges), key=len, reverse=True)
        
        r_counts = Counter()
        for num in nums:
            matched = False
            for r in sorted_known:
                if num.startswith(r):
                    r_counts[r] += 1
                    matched = True
                    break
            if not matched:
                if len(num) >= 7:
                    r_counts[num[:7]] += 1
                else:
                    r_counts[num] += 1
                    
        r_list = r_counts.most_common(12)
        
        kb = []
        temp_row = []
        for r, count in r_list:
            # 🌟 বাটনে এবং কলব্যাকে যাতে কোনো X না থাকে তা নিশ্চিত করা হলো
            clean_r = str(r).replace("X", "").replace("x", "")
            temp_row.append({"text": f"{clean_r} ({count})", "icon_custom_emoji_id": "5352862640592949843", "callback_data": f"c_n_s_{clean_r}_{srv_query}", "style": "primary"})
            if len(temp_row) == 2:
                kb.append(temp_row)
                temp_row = []
        if temp_row:
            kb.append(temp_row)
            
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"exp_rng_{srv_query}", "style": "danger"}])
        
        app_full_name, prem_app_html = get_service_info_html(srv_query)
        prem_flag_html = get_flag_info_html(iso_query)
        
        edit_message(chat_id, msg_id, render_body_text(f"📊 <b>Ranges for {prem_app_html} {app_full_name} - {prem_flag_html} {iso_query}</b>\n\nClick on any range to copy it."), reply_markup={"inline_keyboard": kb})
        answer_callback(call["id"])

    # --- User Management Flows Integration ---
    elif data == "user_management":
        edit_message(chat_id, msg_id, get_user_management_text(), reply_markup=user_management_keyboard())

    elif data.startswith("um_bal_"):
        txt_, kb_ = build_balances_page(int(data.split("_")[2]))
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)

    elif data == "um_csv":
        send_users_csv(chat_id)

    elif data == "um_backup":
        send_backup_files(chat_id)

    elif data == "um_restore":
        user_states[chat_id] = "wait_for_restore"
        send_message(chat_id, render_body_text("♻️ <b>RESTORE BACKUP</b>\n\nSend your backup file here:\n• <b>bot_users_....db</b> → users, balances, history\n• <b>bot_data_....json</b> → settings and number stock\n\n⚠️ This <b>replaces</b> the current data. Max file size 20 MB."), reply_markup=get_cancel_kb())

    elif data.startswith("um_hist_"):
        _p = data.split("_")
        txt_, kb_ = build_history_page(_p[2], int(_p[3]))
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)

    elif data.startswith("um_prof_"):
        res_ = build_user_profile(data.split("_")[2])
        if res_:
            edit_message(chat_id, msg_id, render_body_text(res_[0]), reply_markup=res_[1])

    elif data == "um_manage_balance":
        user_states[chat_id] = "wait_for_um_bal_uid"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID to Manage Balance:"), reply_markup=get_cancel_kb())
        
    elif data == "um_ban_unban":
        user_states[chat_id] = "wait_for_um_ban_uid"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID to Ban or Unban:"), reply_markup=get_cancel_kb())

    elif data == "um_user_profile":
        user_states[chat_id] = "wait_for_um_prof_uid"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID or @username to View Profile / History:"), reply_markup=get_cancel_kb())

    # --- Menu Design Integration ---
    elif data == "menu_design_list":
        edit_message(chat_id, msg_id, render_body_text(f"🎨 <b>Menu Design Editor</b>\n\nSelect a menu block to edit its Body Text and Inline Buttons. You can use Premium Emojis too!"), reply_markup=menu_design_list_keyboard())

    elif data == "md_reset_defaults":
        bot_settings["custom_messages"] = DEFAULT_CUSTOM_MESSAGES.copy()
        save_db()
        answer_callback(call["id"], "✅ Resetted to Premium Defaults!", show_alert=True)

    elif data.startswith("md_edit_"):
        answer_callback(call["id"])
        if chat_id in user_states: del user_states[chat_id]
        if chat_id in temp_data: del temp_data[chat_id]
        key = data.replace("md_edit_", "")
        cm_text = render_body_text(bot_settings["custom_messages"].get(key, {}).get("text", "..."))
        try:
            edit_message(chat_id, msg_id, render_body_text(f"🎨 <b>Editing: {key.upper()}</b>\n\nPreview of current Text:\n{cm_text}"), reply_markup=menu_edit_options_keyboard(key))
        except Exception: pass

    elif data.startswith("md_text_"):
        key = data.replace("md_text_", "")
        user_states[chat_id] = "wait_for_menu_text"
        temp_data[chat_id] = {"msg_id": msg_id, "menu_key": key}
        edit_message(chat_id, msg_id, render_body_text(f"📝 <b>Edit Body: {key.upper()}</b>\n\nSend the new text. You can use Premium Emojis directly here.\n(Use standard HTML like <b>bold</b>, <i>italic</i> for formatting)"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_edit_{key}", "style": "danger"}]]})

    elif data.startswith("md_btns_"):
        answer_callback(call["id"]) 
        if chat_id in user_states: del user_states[chat_id] 
        if chat_id in temp_data: del temp_data[chat_id]
        key = data.replace("md_btns_", "")
        try:
            edit_message(chat_id, msg_id, render_body_text(f"⚙️ <b>Edit Inline Buttons: {key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(key))
        except Exception: pass

    elif data.startswith("md_addbtn_"):
        key = data.replace("md_addbtn_", "")
        user_states[chat_id] = "wait_for_menu_btn"
        temp_data[chat_id] = {"msg_id": msg_id, "menu_key": key}
        edit_message(chat_id, msg_id, render_body_text(f"➕ <b>Add Button: {key.upper()}</b>\n\nSend custom button in this format:\n<code>Button Text - https://link.com</code>\n\n<i>(Only normal Emojis supported here!)</i>"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"md_btns_{key}", "style": "danger"}]]})

    elif data.startswith("md_delbtn_"):
        parts = data.split("_")
        key = parts[2]
        b_idx = int(parts[3])
        if b_idx < len(bot_settings["custom_messages"][key]["buttons"]):
            del bot_settings["custom_messages"][key]["buttons"][b_idx]
            save_db()
            answer_callback(call["id"], "✅ Button Deleted!", show_alert=True)
            edit_message(chat_id, msg_id, render_body_text(f"⚙️ <b>Edit Inline Buttons: {key.upper()}</b>"), reply_markup=menu_buttons_list_keyboard(key))

    elif data.startswith("sel_wm_"):
        method = data.replace("sel_wm_", "")
        bal = get_user(chat_id).get('balance', 0.0)
        min_w = bot_settings['min_withdraw']
        
        if bal < min_w:
            answer_callback(call["id"], f"❌ আপনার ব্যালেন্স অপর্যাপ্ত! মিনিমাম {min_w} ৳ প্রয়োজন।", show_alert=True)
            return
            
        temp_data[chat_id] = {"method": method, "balance": bal, "msg_id": msg_id}
        user_states[chat_id] = "wait_for_withdraw_amount"
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['ok']} Method: {method}\n💰 Available Balance: {bal} ৳\n\n📝 Enter the amount you want to withdraw (Min: {min_w} ৳):"), reply_markup=get_cancel_kb())
        answer_callback(call["id"])

    elif data == "test_message_flow":
        user_states[chat_id] = "wait_for_test_service"
        temp_data[chat_id] = {}
        edit_message(chat_id, msg_id, render_body_text("🧪 <b>Test Mode</b>\n\n📝 Send the Service Name (e.g., IG):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]]})

    elif data == "manage_emojis":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['star']} <b>Premium Emoji Management</b>\n\nUpload your TXT files or manually add them below:"), reply_markup=emoji_settings_keyboard())

    elif data == "up_flags_txt":
        user_states[chat_id] = "wait_for_flag_txt"
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the <b>Flag Emojis</b> <code>.txt</code> file."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "up_apps_txt":
        user_states[chat_id] = "wait_for_app_txt"
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the <b>Service Apps</b> <code>.txt</code> file."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "add_single_emoji":
        user_states[chat_id] = "wait_for_emoji_extract"
        edit_message(chat_id, msg_id, render_body_text("📝 যেকোনো একটি Premium Emoji সেন্ড করুন (যেমন: 🇧🇩 বা 🚫):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_emojis", "style": "danger"}]]})

    elif data == "dl_flags_txt":
        content = generate_emoji_txt("flags")
        if content:
            send_document(chat_id, "Flag_Emojis.txt", content)
            answer_callback(call["id"], "✅ Downloaded!")
        else:
            answer_callback(call["id"], "❌ No Flag Emojis found!", show_alert=True)

    elif data == "dl_apps_txt":
        content = generate_emoji_txt("apps")
        if content:
            send_document(chat_id, "Service_Apps.txt", content)
            answer_callback(call["id"], "✅ Downloaded!")
        else:
            answer_callback(call["id"], "❌ No App Emojis found!", show_alert=True)

    elif data == "del_all_flags":
        bot_settings["premium_flags"] = {}
        save_db()
        answer_callback(call["id"], "✅ All Premium Flags Deleted Successfully!", show_alert=True)

    elif data == "broadcast_msg":
        user_states[chat_id] = "wait_for_broadcast"
        edit_message(chat_id, msg_id, render_body_text("📢 <b>Broadcast Mode</b>\n\nSend the message you want to broadcast (Text, Photo, Video, File etc)."), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]})

    elif data == "upload_num":
        user_states[chat_id] = "wait_for_txt"
        edit_message(chat_id, msg_id, render_body_text("📂 Please upload the numbers in a <b>.txt</b> file."), reply_markup={"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]})

    elif data == "delete_files":
        kb = []
        for b_id, b_data in number_batches.items():
            kb.append([{"text": f"{b_data['filename']} ({len(b_data['numbers'])})", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"del_b_{b_id}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "primary"}])
        txt = "🗑 Select a file to delete:" if len(kb) > 1 else f"{PEM['no']} No files found."
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("del_b_"):
        b_id = data.split("del_b_")[1]
        if b_id in number_batches:
            del number_batches[b_id]
            save_db()
            answer_callback(call["id"], "✅ File deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "delete_files", "id": call["id"]})

    elif data == "show_used":
        kb = {"inline_keyboard": [[{"text": "Download TXT", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_used", "style": "primary"}], [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]}
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['ok']} <b>Total Used Numbers:</b> {len(used_numbers_list)}"), reply_markup=kb)

    elif data == "show_unused":
        unused_count = sum(len(b["numbers"]) for b in number_batches.values())
        kb = {"inline_keyboard": [[{"text": "Download TXT", "icon_custom_emoji_id": "5257969839313526622", "callback_data": "dl_unused", "style": "primary"}], [{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]]}
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['rocket']} <b>Total Unused Numbers:</b> {unused_count}"), reply_markup=kb)

    elif data == "dl_used":
        if not used_numbers_list:
            answer_callback(call["id"], "❌ No used numbers found!", show_alert=True)
            return
        content = "\n".join(used_numbers_list).encode('utf-8')
        send_document(chat_id, "used_numbers.txt", content)
        answer_callback(call["id"])

    elif data == "dl_unused":
        unused_list = [n["num"] for b in number_batches.values() for n in b["numbers"]]
        if not unused_list:
            answer_callback(call["id"], "❌ No unused numbers found!", show_alert=True)
            return
        content = "\n".join(unused_list).encode('utf-8')
        send_document(chat_id, "unused_numbers.txt", content)
        answer_callback(call["id"])

    elif data.startswith("ulb_"):
        # members: top 10 leaderboard (Today / 7 Days / All Time)
        txt_, kb_ = build_user_lb(chat_id, data[4:])
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)

    elif data.startswith("alb_all_"):
        # admin: full leaderboard, alb_all_<t|w|a>_<page>
        _, _, _p, _pg = data.split("_")
        txt_, kb_ = build_admin_lb(_p, int(_pg))
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)

    elif data == "alb_total":
        txt_, kb_ = build_total_otp()
        edit_message(chat_id, msg_id, render_body_text(txt_), reply_markup=kb_)

    elif data == "drpt_now":
        send_daily_report(_day_str(), to=[chat_id])

    elif data == "drpt_toggle":
        bot_settings["daily_report_on"] = not bot_settings.get("daily_report_on", True)
        save_db()
        txt_, kb_ = lb_main_view()
        edit_message(chat_id, msg_id, txt_, reply_markup=kb_)

    elif data == "lb_main":
        txt_, kb_ = lb_main_view()
        edit_message(chat_id, msg_id, txt_, reply_markup=kb_)

    elif data.startswith("lb_"):
        sub = data.replace("lb_", "")
        edit_message(chat_id, msg_id, render_body_text("⌛ <i>Fetching Data...</i>"))
        
        num_map = {"1": "1️⃣", "2": "2️⃣", "3": "3️⃣", "4": "4️⃣", "5": "5️⃣", "6": "6️⃣", "7": "7️⃣", "8": "8️⃣", "9": "9️⃣", "0": "0️⃣"}
        def get_p_num(n): return "".join([num_map.get(c, c) for c in str(n)])
        
        try:
            if sub == "top_refs":
                title, field, limit, icon = "TOP 5 REFERRERS", "total_refers", 5, PEM.get('user', '👥')
                users = db.collection('users').order_by(field, direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for u in users:
                    d = u.to_dict()
                    if d.get(field, 0) > 0:
                        p = "└" if count == limit else "├"
                        res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={u.id}'>{u.id}</a> ➔ <b>{d.get(field,0)}</b>\n"
                        count += 1
                if not res_txt: res_txt = "└ <i>No data found.</i>\n"

            elif sub == "top_otps":
                title, field, limit, icon = "TOP 5 OTP RECEIVERS", "total_otps", 5, PEM.get('msg', '📩')
                users = db.collection('users').order_by(field, direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for u in users:
                    d = u.to_dict()
                    if d.get(field, 0) > 0:
                        p = "└" if count == limit else "├"
                        res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={u.id}'>{u.id}</a> ➔ <b>{d.get(field,0)}</b>\n"
                        count += 1
                if not res_txt: res_txt = "└ <i>No data found.</i>\n"

            elif sub == "w_history":
                title, limit, icon = "LAST 10 WITHDRAWALS", 10, PEM.get('money', '💸')
                ws = db.collection('withdrawals').order_by('timestamp', direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for w in ws:
                    d = w.to_dict()
                    s = str(d.get('status','Pending')).lower()
                    stat_icon = PEM.get('ok','✅') if s in ["approved","success"] else PEM.get('no','❌') if s=="rejected" else "⏳"
                    uid = d.get('user_id','User')
                    p = "└" if count == limit else "├"
                    res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={uid}'>{uid}</a> ➔ <b>{d.get('amount',0)}৳</b> {stat_icon}\n"
                    count += 1
                if not res_txt: res_txt = "└ <i>No history found.</i>\n"

            final_msg = f"━━━━━━━━━━━━━━━\n{icon} <b>{title}</b>\n━━━━━━━━━━━━━━━\n{res_txt}━━━━━━━━━━━━━━━"
            kb = [[{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": data, "style": "success"}, {"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]
            edit_message(chat_id, msg_id, render_body_text(final_msg), reply_markup={"inline_keyboard": kb})

        except Exception as e:
            edit_message(chat_id, msg_id, render_body_text(f"❌ Error: {e}"), reply_markup={"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]})

    elif data == "lb_main":
        txt = f"━━━━━━━━━━━━━━━\n《 {PEM['admin']} <b>LEADER BOARD MENU</b> 》\n━━━━━━━━━━━━━━━\n<i>Select a category to view the top performers or history.</i>\n━━━━━━━━━━━━━━━"
        kb = [
            [{"text": "Top Referrers", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "lb_top_refs", "style": "primary"}],
            [{"text": "Top OTP Receivers", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "lb_top_otps", "style": "primary"}],
            [{"text": "Withdrawal History", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "lb_w_history", "style": "success"}],
            [{"text": "Back to Admin", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}]
        ]
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("lb_"):
        sub = data.replace("lb_", "")
        edit_message(chat_id, msg_id, render_body_text("⌛ <i>Fetching Data...</i>"))
        
        num_map = {"1": "1️⃣", "2": "2️⃣", "3": "3️⃣", "4": "4️⃣", "5": "5️⃣", "6": "6️⃣", "7": "7️⃣", "8": "8️⃣", "9": "9️⃣", "0": "0️⃣"}
        def get_p_num(n): return "".join([num_map.get(c, c) for c in str(n)])
        
        try:
            if sub == "top_refs":
                title, field, limit, icon = "TOP 5 REFERRERS", "total_refers", 5, PEM.get('user', '👥')
                users = db.collection('users').order_by(field, direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for u in users:
                    d = u.to_dict()
                    if d.get(field, 0) > 0:
                        p = "└" if count == limit else "├"
                        res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={u.id}'>{u.id}</a> ➔ <b>{d.get(field,0)}</b>\n"
                        count += 1
                if not res_txt: res_txt = "└ <i>No data found.</i>\n"

            elif sub == "top_otps":
                title, field, limit, icon = "TOP 5 OTP RECEIVERS", "total_otps", 5, PEM.get('msg', '📩')
                users = db.collection('users').order_by(field, direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for u in users:
                    d = u.to_dict()
                    if d.get(field, 0) > 0:
                        p = "└" if count == limit else "├"
                        res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={u.id}'>{u.id}</a> ➔ <b>{d.get(field,0)}</b>\n"
                        count += 1
                if not res_txt: res_txt = "└ <i>No data found.</i>\n"

            elif sub == "w_history":
                title, limit, icon = "LAST 10 WITHDRAWALS", 10, PEM.get('money', '💸')
                ws = db.collection('withdrawals').order_by('timestamp', direction="DESCENDING").limit(limit).stream()
                res_txt = ""
                count = 1
                for w in ws:
                    d = w.to_dict()
                    s = str(d.get('status','Pending')).lower()
                    stat_icon = PEM.get('ok','✅') if s in ["approved","success"] else PEM.get('no','❌') if s=="rejected" else "⏳"
                    uid = d.get('user_id','User')
                    p = "└" if count == limit else "├"
                    res_txt += f"{p} {get_p_num(count)} <a href='tg://user?id={uid}'>{uid}</a> ➔ <b>{d.get('amount',0)}৳</b> {stat_icon}\n"
                    count += 1
                if not res_txt: res_txt = "└ <i>No history found.</i>\n"

            final_msg = f"━━━━━━━━━━━━━━━\n{icon} <b>{title}</b>\n━━━━━━━━━━━━━━━\n{res_txt}━━━━━━━━━━━━━━━"
            kb = [[{"text": "Refresh", "icon_custom_emoji_id": "5420155432272438703", "callback_data": data, "style": "success"}, {"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]
            edit_message(chat_id, msg_id, render_body_text(final_msg), reply_markup={"inline_keyboard": kb})

        except Exception as e:
            edit_message(chat_id, msg_id, render_body_text(f"❌ Error: {e}"), reply_markup={"inline_keyboard": [[{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "lb_main", "style": "danger"}]]})

    elif data == "back_to_admin":
        if chat_id in user_states: del user_states[chat_id]
        edit_message(chat_id, msg_id, get_admin_text(), reply_markup=admin_panel_keyboard())
        
    elif data == "system_settings":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['gear']} <b>System Settings</b>\nManage advanced bot configurations below:"), reply_markup=system_settings_keyboard())

    elif data == "ls_menu":
        edit_message(chat_id, msg_id, render_body_text(build_low_stock_text()), reply_markup=low_stock_keyboard())

    elif data == "ls_toggle":
        bot_settings["low_stock_on"] = not bot_settings.get("low_stock_on", True)
        bot_settings["low_stock_last"] = 0
        save_db()
        edit_message(chat_id, msg_id, render_body_text(build_low_stock_text()), reply_markup=low_stock_keyboard())

    elif data == "ls_set":
        user_states[chat_id] = "wait_for_low_stock_limit"
        send_message(chat_id, render_body_text("\U0001F4E6 <b>Send the new alert limit</b> (a number, e.g. 200).\nYou will get a message when stock falls below it."), reply_markup=get_cancel_kb())

    elif data == "stex_control":
        edit_message(chat_id, msg_id, render_body_text(f"🌐 <b>StexSMS Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('stex_keys', []))}\nManage your StexSMS API Keys below:"), reply_markup=stex_control_keyboard())

    elif data == "toggle_stex_auto":
        bot_settings["stex_auto"] = not bot_settings.get("stex_auto", False)
        save_db()
        edit_message(chat_id, msg_id, render_body_text(f"🌐 <b>StexSMS Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('stex_keys', []))}\nManage your StexSMS API Keys below:"), reply_markup=stex_control_keyboard())
        answer_callback(call["id"], f"✅ Stex Auto Range is now {'ON' if bot_settings['stex_auto'] else 'OFF'}!", show_alert=False)

    elif data == "add_stex_key":
        user_states[chat_id] = "wait_for_add_stex_key"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new StexSMS API Key (e.g. nxa_...):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_control", "style": "danger"}]]})

    elif data == "view_stex_keys":
        kb = []
        for idx, key in enumerate(bot_settings.get("stex_keys", [])):
            safe_name = key[:10] + "..." if len(key)>10 else key
            kb.append([{"text": f"Delete {safe_name}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_nxa_{idx}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🗑 <b>Select StexSMS Key to Delete:</b>"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("del_nxa_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("stex_keys", [])):
            del bot_settings["stex_keys"][idx]
            save_db()
            answer_callback(call["id"], "✅ StexSMS Key Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "view_stex_keys", "id": call["id"]})

    elif data == "stex_search_country":
        kb = []
        for idx, c in enumerate(bot_settings.get("search_countries", [])):
            kb.append([{"text": f"Delete {c}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_sc_{idx}", "style": "danger"}])
        kb.append([{"text": "Add Country Code", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_search_country", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🌍 <b>Allowed Search Countries:</b>\nOnly these country codes will be allowed in Search Number."), reply_markup={"inline_keyboard": kb})

    elif data == "add_search_country":
        user_states[chat_id] = "wait_for_add_sc"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Country Code (e.g. 880 or 92):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_search_country", "style": "danger"}]]})

    elif data.startswith("del_sc_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("search_countries", [])):
            del bot_settings["search_countries"][idx]
            save_db()
            answer_callback(call["id"], "✅ Country Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "stex_search_country", "id": call["id"]})

    elif data == "manage_stex_srv":
        kb = []
        srvs = bot_settings.get("stex_services", {})
        apps_db = bot_settings.get("premium_apps", {})
        for srv in srvs:
            emoji_id = "5257969839313526622"
            for app_key, app_data in apps_db.items():
                if srv.upper() == app_key or srv.upper() in app_key or app_key in srv.upper():
                    if "id" in app_data:
                        emoji_id = app_data["id"]
                        break
            kb.append([{"text": f"{srv}", "icon_custom_emoji_id": emoji_id, "callback_data": f"nx_srv_{srv}", "style": "primary"}])
        kb.append([{"text": "Add New Service", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "nx_add_srv", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "stex_control", "style": "danger"}])
        edit_message(chat_id, msg_id, render_body_text("📦 <b>StexSMS Services Manager</b>\nManage your API-based dynamic services below:"), reply_markup={"inline_keyboard": kb})

    elif data == "nx_add_srv":
        user_states[chat_id] = "wait_nx_srv_name"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Enter Service Name (e.g. TELEGRAM):"), reply_markup=get_cancel_kb())

    elif data.startswith("nx_srv_"):
        srv = data.replace("nx_srv_", "")
        kb = []
        countries = bot_settings["stex_services"].get(srv, {})
        flags_db = bot_settings.get("premium_flags", {})
        for c in countries:
            emoji_id = "5780471598922337683"
            for flag_code, flag_data in flags_db.items():
                iso = flag_data.get("iso", "").upper()
                name = flag_data.get("name", "").upper()
                if c.upper() == iso or c.upper() == name or c.upper() in name or name in c.upper():
                    if "id" in flag_data:
                        emoji_id = flag_data["id"]
                        break
            kb.append([{"text": f"{c} ({len(countries[c])} Ranges)", "icon_custom_emoji_id": emoji_id, "callback_data": f"nx_cnt_{srv}_{c}", "style": "primary"}])
        kb.append([{"text": "Add Country", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"nx_add_cnt_{srv}", "style": "success"}])
        kb.append([{"text": "Delete Service", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"nx_del_srv_{srv}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_stex_srv", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"📂 <b>Service: {srv}</b>\nManage countries for this service:"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("nx_add_cnt_"):
        srv = data.replace("nx_add_cnt_", "")
        user_states[chat_id] = "wait_nx_cnt_name"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv}
        edit_message(chat_id, msg_id, render_body_text(f"🌍 Enter Country Name for <b>{srv}</b> (e.g. BD, INDIA):"), reply_markup=get_cancel_kb())

    elif data.startswith("nx_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        ranges = bot_settings["stex_services"][srv].get(cnt, [])
        
        kb = []
        row = []
        for r in ranges:
            row.append({"text": f"Delete {r}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"nx_dr_{srv}_{cnt}_{r}", "style": "danger"})
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        
        kb.append([{"text": "Add Range", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"nx_addr_{srv}_{cnt}", "style": "success"}])
        kb.append([{"text": "Delete Entire Country", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"nx_del_cnt_{srv}_{cnt}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"nx_srv_{srv}", "style": "primary"}])
        
        txt = f"📍 <b>Service: {srv} | Country: {cnt}</b>\n\n<b>Total Ranges:</b> {len(ranges)}\n<i>Click on a range below to delete it, or add a new one.</i>"
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("nx_addr_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        user_states[chat_id] = "wait_nx_addr"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv, "cnt": cnt}
        edit_message(chat_id, msg_id, render_body_text(f"📝 Send the new Range for <b>{cnt}</b> (e.g. 88017):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"nx_cnt_{srv}_{cnt}", "style": "danger"}]]})

    elif data.startswith("nx_dr_"):
        parts = data.split("_")
        srv, cnt, rng = parts[2], parts[3], parts[4]
        if rng in bot_settings["stex_services"].get(srv, {}).get(cnt, []):
            bot_settings["stex_services"][srv][cnt].remove(rng)
            save_db()
            answer_callback(call["id"], f"✅ Range {rng} deleted!", show_alert=True)
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"nx_cnt_{srv}_{cnt}", "id": call["id"]})

    elif data.startswith("nx_del_srv_"):
        srv = data.replace("nx_del_srv_", "")
        if srv in bot_settings["stex_services"]: del bot_settings["stex_services"][srv]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_stex_srv", "id": call["id"]})

    elif data.startswith("nx_del_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[3], parts[4]
        if cnt in bot_settings["stex_services"].get(srv, {}): del bot_settings["stex_services"][srv][cnt]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"nx_srv_{srv}", "id": call["id"]})

    elif data == "voltx_control":
        edit_message(chat_id, msg_id, render_body_text(f"⚡ <b>Voltx Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('voltx_keys', []))}\nManage your Voltx API Keys below:"), reply_markup=voltx_control_keyboard())

    elif data == "toggle_voltx_auto":
        bot_settings["voltx_auto"] = not bot_settings.get("voltx_auto", False)
        save_db()
        edit_message(chat_id, msg_id, render_body_text(f"⚡ <b>Voltx Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('voltx_keys', []))}\nManage your Voltx API Keys below:"), reply_markup=voltx_control_keyboard())
        answer_callback(call["id"], f"✅ Voltx Auto Range is now {'ON' if bot_settings['voltx_auto'] else 'OFF'}!", show_alert=False)

    elif data == "add_voltx_key":
        user_states[chat_id] = "wait_for_add_voltx_key"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new Voltx API Key:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "voltx_control", "style": "danger"}]]})

    elif data == "view_voltx_keys":
        kb = []
        for idx, key in enumerate(bot_settings.get("voltx_keys", [])):
            safe_name = key[:10] + "..." if len(key)>10 else key
            kb.append([{"text": f"Delete {safe_name}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_vtx_{idx}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "voltx_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🗑 <b>Select Voltx Key to Delete:</b>"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("del_vtx_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("voltx_keys", [])):
            del bot_settings["voltx_keys"][idx]
            save_db()
            answer_callback(call["id"], "✅ Voltx Key Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "view_voltx_keys", "id": call["id"]})

    elif data == "voltx_search_country":
        kb = []
        for idx, c in enumerate(bot_settings.get("voltx_search_countries", [])):
            kb.append([{"text": f"Delete {c}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_vsc_{idx}", "style": "danger"}])
        kb.append([{"text": "Add Country Code", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_voltx_search_country", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "voltx_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🌍 <b>Voltx Allowed Ranges:</b>\nOnly these ranges/codes will be allowed in Voltx Search Number."), reply_markup={"inline_keyboard": kb})

    elif data == "add_voltx_search_country":
        user_states[chat_id] = "wait_for_add_vsc"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Voltx Range Code (e.g. 26134):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "voltx_search_country", "style": "danger"}]]})

    elif data.startswith("del_vsc_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("voltx_search_countries", [])):
            del bot_settings["voltx_search_countries"][idx]
            save_db()
            answer_callback(call["id"], "✅ Voltx Range Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "voltx_search_country", "id": call["id"]})

    elif data == "manage_voltx_srv":
        kb = []
        srvs = bot_settings.get("voltx_services", {})
        apps_db = bot_settings.get("premium_apps", {})
        for srv in srvs:
            emoji_id = "5257969839313526622"
            for app_key, app_data in apps_db.items():
                if srv.upper() == app_key or srv.upper() in app_key or app_key in srv.upper():
                    if "id" in app_data: emoji_id = app_data["id"]; break
            kb.append([{"text": f"{srv}", "icon_custom_emoji_id": emoji_id, "callback_data": f"vx_srv_{srv}", "style": "primary"}])
        kb.append([{"text": "Add New Service", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "vx_add_srv", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "voltx_control", "style": "danger"}])
        edit_message(chat_id, msg_id, render_body_text("⚡ <b>Voltx Services Manager</b>\nManage your API-based dynamic services below:"), reply_markup={"inline_keyboard": kb})

    elif data == "vx_add_srv":
        user_states[chat_id] = "wait_vx_srv_name"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Enter Service Name (e.g. TELEGRAM):"), reply_markup=get_cancel_kb())

    elif data.startswith("vx_srv_"):
        srv = data.replace("vx_srv_", "")
        kb = []
        countries = bot_settings["voltx_services"].get(srv, {})
        flags_db = bot_settings.get("premium_flags", {})
        for c in countries:
            emoji_id = "5780471598922337683"
            for flag_code, flag_data in flags_db.items():
                iso = flag_data.get("iso", "").upper()
                name = flag_data.get("name", "").upper()
                if c.upper() == iso or c.upper() == name or c.upper() in name or name in c.upper():
                    if "id" in flag_data: emoji_id = flag_data["id"]; break
            kb.append([{"text": f"{c} ({len(countries[c])} Ranges)", "icon_custom_emoji_id": emoji_id, "callback_data": f"vx_cnt_{srv}_{c}", "style": "primary"}])
        kb.append([{"text": "Add Country", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"vx_add_cnt_{srv}", "style": "success"}])
        kb.append([{"text": "Delete Service", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"vx_del_srv_{srv}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_voltx_srv", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"📂 <b>Service: {srv}</b>\nManage countries for this service:"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("vx_add_cnt_"):
        srv = data.replace("vx_add_cnt_", "")
        user_states[chat_id] = "wait_vx_cnt_name"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv}
        edit_message(chat_id, msg_id, render_body_text(f"🌍 Enter Country Name for <b>{srv}</b> (e.g. BD, INDIA):"), reply_markup=get_cancel_kb())

    elif data.startswith("vx_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        ranges = bot_settings["voltx_services"][srv].get(cnt, [])
        kb = []
        row = []
        for r in ranges:
            row.append({"text": f"Delete {r}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"vx_dr_{srv}_{cnt}_{r}", "style": "danger"})
            if len(row) == 2:
                kb.append(row)
                row = []
        if row: kb.append(row)
        kb.append([{"text": "Add Range", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"vx_addr_{srv}_{cnt}", "style": "success"}])
        kb.append([{"text": "Delete Entire Country", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"vx_del_cnt_{srv}_{cnt}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"vx_srv_{srv}", "style": "primary"}])
        txt = f"📍 <b>Service: {srv} | Country: {cnt}</b>\n\n<b>Total Ranges:</b> {len(ranges)}\n<i>Click on a range below to delete it, or add a new one.</i>"
        edit_message(chat_id, msg_id, render_body_text(txt), reply_markup={"inline_keyboard": kb})

    elif data.startswith("vx_addr_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        user_states[chat_id] = "wait_vx_addr"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv, "cnt": cnt}
        edit_message(chat_id, msg_id, render_body_text(f"📝 Send the new Range for <b>{cnt}</b> (e.g. 26134):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"vx_cnt_{srv}_{cnt}", "style": "danger"}]]})

    elif data.startswith("vx_dr_"):
        parts = data.split("_")
        srv, cnt, rng = parts[2], parts[3], parts[4]
        if rng in bot_settings["voltx_services"].get(srv, {}).get(cnt, []):
            bot_settings["voltx_services"][srv][cnt].remove(rng)
            save_db()
            answer_callback(call["id"], f"✅ Range {rng} deleted!", show_alert=True)
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"vx_cnt_{srv}_{cnt}", "id": call["id"]})

    elif data.startswith("vx_del_srv_"):
        srv = data.replace("vx_del_srv_", "")
        if srv in bot_settings["voltx_services"]: del bot_settings["voltx_services"][srv]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_voltx_srv", "id": call["id"]})

    elif data.startswith("vx_del_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[3], parts[4]
        if cnt in bot_settings["voltx_services"].get(srv, {}): del bot_settings["voltx_services"][srv][cnt]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"vx_srv_{srv}", "id": call["id"]})

    # --- ZENEX CONTROL CALLBACKS ---
    elif data == "zenex_control":
        edit_message(chat_id, msg_id, render_body_text(f"💠 <b>Zenex Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('zenex_keys', []))}\nManage your Zenex API Keys below:"), reply_markup=zenex_control_keyboard())

    elif data == "toggle_zenex_auto":
        bot_settings["zenex_auto"] = not bot_settings.get("zenex_auto", False)
        save_db()
        edit_message(chat_id, msg_id, render_body_text(f"💠 <b>Zenex Control Panel</b>\n\nTotal API Keys: {len(bot_settings.get('zenex_keys', []))}\nManage your Zenex API Keys below:"), reply_markup=zenex_control_keyboard())
        answer_callback(call["id"], f"✅ Zenex Auto Range is now {'ON' if bot_settings['zenex_auto'] else 'OFF'}!", show_alert=False)

    elif data == "add_zenex_key":
        user_states[chat_id] = "wait_for_add_zenex_key"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new Zenex API Key:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "zenex_control", "style": "danger"}]]})

    elif data == "view_zenex_keys":
        kb = []
        for idx, key in enumerate(bot_settings.get("zenex_keys", [])):
            safe_name = key[:10] + "..." if len(key)>10 else key
            kb.append([{"text": f"Delete {safe_name}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_znx_{idx}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "zenex_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🗑 <b>Select Zenex Key to Delete:</b>"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("del_znx_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("zenex_keys", [])):
            del bot_settings["zenex_keys"][idx]
            save_db()
            answer_callback(call["id"], "✅ Zenex Key Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "view_zenex_keys", "id": call["id"]})

    elif data == "zenex_search_country":
        kb = []
        for idx, c in enumerate(bot_settings.get("zenex_search_countries", [])):
            kb.append([{"text": f"Delete {c}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"del_zsc_{idx}", "style": "danger"}])
        kb.append([{"text": "Add Country Code", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "add_zenex_search_country", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "zenex_control", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text("🌍 <b>Zenex Allowed Ranges:</b>\nOnly these codes will be allowed in Zenex Search Number."), reply_markup={"inline_keyboard": kb})

    elif data == "add_zenex_search_country":
        user_states[chat_id] = "wait_for_add_zsc"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Zenex Range Code (e.g. 447):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "zenex_search_country", "style": "danger"}]]})

    elif data.startswith("del_zsc_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings.get("zenex_search_countries", [])):
            del bot_settings["zenex_search_countries"][idx]
            save_db()
            answer_callback(call["id"], "✅ Zenex Range Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "zenex_search_country", "id": call["id"]})

    elif data == "manage_zenex_srv":
        kb = []
        for srv in bot_settings.get("zenex_services", {}):
            kb.append([{"text": f"{srv}", "icon_custom_emoji_id": "5257969839313526622", "callback_data": f"zx_srv_{srv}", "style": "primary"}])
        kb.append([{"text": "Add New Service", "icon_custom_emoji_id": "5420323438508155202", "callback_data": "zx_add_srv", "style": "success"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "zenex_control", "style": "danger"}])
        edit_message(chat_id, msg_id, render_body_text("💠 <b>Zenex Services Manager</b>\nManage your API-based manual services below:"), reply_markup={"inline_keyboard": kb})

    elif data == "zx_add_srv":
        user_states[chat_id] = "wait_zx_srv_name"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Enter Service Name (e.g. TELEGRAM):"), reply_markup=get_cancel_kb())

    elif data.startswith("zx_srv_"):
        srv = data.replace("zx_srv_", "")
        kb = []
        for c in bot_settings.get("zenex_services", {}).get(srv, {}):
            kb.append([{"text": f"{c} ({len(bot_settings['zenex_services'][srv][c])} Ranges)", "icon_custom_emoji_id": "5780471598922337683", "callback_data": f"zx_cnt_{srv}_{c}", "style": "primary"}])
        kb.append([{"text": "Add Country", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"zx_add_cnt_{srv}", "style": "success"}])
        kb.append([{"text": "Delete Service", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"zx_del_srv_{srv}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_zenex_srv", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"📂 <b>Service: {srv}</b>\nManage countries for this service:"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("zx_add_cnt_"):
        srv = data.replace("zx_add_cnt_", "")
        user_states[chat_id] = "wait_zx_cnt_name"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv}
        edit_message(chat_id, msg_id, render_body_text(f"🌍 Enter Country Name for <b>{srv}</b> (e.g. UK, USA):"), reply_markup=get_cancel_kb())

    elif data.startswith("zx_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        ranges = bot_settings["zenex_services"][srv].get(cnt, [])
        kb, row = [], []
        for r in ranges:
            row.append({"text": f"Delete {r}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"zx_dr_{srv}_{cnt}_{r}", "style": "danger"})
            if len(row) == 2: kb.append(row); row = []
        if row: kb.append(row)
        kb.append([{"text": "Add Range", "icon_custom_emoji_id": "5420323438508155202", "callback_data": f"zx_addr_{srv}_{cnt}", "style": "success"}])
        kb.append([{"text": "Delete Entire Country", "icon_custom_emoji_id": "5422557736330106570", "callback_data": f"zx_del_cnt_{srv}_{cnt}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"zx_srv_{srv}", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"📍 <b>Service: {srv} | Country: {cnt}</b>\n\n<b>Total Ranges:</b> {len(ranges)}"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("zx_addr_"):
        parts = data.split("_")
        srv, cnt = parts[2], parts[3]
        user_states[chat_id] = "wait_zx_addr"
        temp_data[chat_id] = {"msg_id": msg_id, "srv": srv, "cnt": cnt}
        edit_message(chat_id, msg_id, render_body_text(f"📝 Send the new Range for <b>{cnt}</b> (e.g. 4473845XXX):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"zx_cnt_{srv}_{cnt}", "style": "danger"}]]})

    elif data.startswith("zx_dr_"):
        parts = data.split("_")
        srv, cnt, rng = parts[2], parts[3], parts[4]
        if rng in bot_settings["zenex_services"].get(srv, {}).get(cnt, []):
            bot_settings["zenex_services"][srv][cnt].remove(rng)
            save_db()
            answer_callback(call["id"], f"✅ Range {rng} deleted!", show_alert=True)
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"zx_cnt_{srv}_{cnt}", "id": call["id"]})

    elif data.startswith("zx_del_srv_"):
        srv = data.replace("zx_del_srv_", "")
        if srv in bot_settings["zenex_services"]: del bot_settings["zenex_services"][srv]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": "manage_zenex_srv", "id": call["id"]})

    elif data.startswith("zx_del_cnt_"):
        parts = data.split("_")
        srv, cnt = parts[3], parts[4]
        if cnt in bot_settings["zenex_services"].get(srv, {}): del bot_settings["zenex_services"][srv][cnt]
        save_db()
        handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"zx_srv_{srv}", "id": call["id"]})

    elif data == "manage_fj":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['link']} <b>FORCE JOIN SYSTEM</b>\nManage channels below:"), reply_markup=fj_settings_keyboard())

    elif data == "toggle_fj":
        bot_settings["fj_on"] = not bot_settings["fj_on"]
        save_db()
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['link']} <b>FORCE JOIN SYSTEM</b>\nManage channels below:"), reply_markup=fj_settings_keyboard())

    elif data == "add_fj":
        user_states[chat_id] = "wait_for_add_fj"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send Channel Username or Invite Link:\n<i>(Note: For private channels, use the numeric ID like -100...)</i>"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_fj", "style": "danger"}]]})

    elif data.startswith("del_fj_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["fj_channels"]):
            del bot_settings["fj_channels"][idx]
            save_db()
            answer_callback(call["id"], "✅ Channel deleted!", show_alert=True)
            edit_message(chat_id, msg_id, render_body_text(f"{PEM['link']} <b>FORCE JOIN SYSTEM</b>\nManage channels below:"), reply_markup=fj_settings_keyboard())

    elif data == "manage_admins":
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['user']} <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())

    elif data == "add_adm":
        user_states[chat_id] = "wait_for_add_adm"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the User ID of the new Admin:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_admins", "style": "danger"}]]})

    elif data.startswith("del_adm_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["admins"]):
            del bot_settings["admins"][idx]
            save_db()
            answer_callback(call["id"], "✅ Admin deleted!", show_alert=True)
            edit_message(chat_id, msg_id, render_body_text(f"{PEM['user']} <b>ADMIN MANAGEMENT</b>\nManage your bot admins below:"), reply_markup=admin_settings_keyboard())

    elif data == "manage_otp_groups":
        edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())

    elif data == "add_fw":
        user_states[chat_id] = "wait_for_add_fw_id"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Group ID/Username to forward messages to:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})

    elif data.startswith("manage_fw_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["fw_groups"]):
            grp_id = bot_settings["fw_groups"][idx]["chat_id"]
            edit_message(chat_id, msg_id, render_body_text(f"🛡 <b>Manage Group:</b> {grp_id}"), reply_markup=specific_fw_group_keyboard(idx))

    elif data.startswith("add_fwbtn_"):
        idx = int(data.split("_")[2])
        user_states[chat_id] = "wait_for_add_fw_btn"
        temp_data[chat_id] = {"msg_id": msg_id, "fw_idx": idx}
        edit_message(chat_id, msg_id, render_body_text("📝 Send Custom Inline Button format:\n<code>Button Text - https://link.com</code>"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_fw_{idx}", "style": "danger"}]]})

    elif data.startswith("del_fwbtn_"):
        parts = data.split("_")
        idx, b_idx = int(parts[2]), int(parts[3])
        if 0 <= idx < len(bot_settings["fw_groups"]):
            if 0 <= b_idx < len(bot_settings["fw_groups"][idx]["buttons"]):
                del bot_settings["fw_groups"][idx]["buttons"][b_idx]
                save_db()
                answer_callback(call["id"], "✅ Button deleted!", show_alert=True)
                edit_message(chat_id, msg_id, render_body_text(f"🛡 <b>Manage Group:</b> {bot_settings['fw_groups'][idx]['chat_id']}"), reply_markup=specific_fw_group_keyboard(idx))

    elif data.startswith("del_fw_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["fw_groups"]):
            del bot_settings["fw_groups"][idx]
            save_db()
            answer_callback(call["id"], "✅ Group deleted!", show_alert=True)
            edit_message(chat_id, msg_id, render_body_text("🛡 <b>OTP GROUP MANAGEMENT</b>\nManage settings below:"), reply_markup=otp_groups_list_keyboard())

    elif data == "edit_otp_link":
        user_states[chat_id] = "wait_for_otp_link"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new OTP Group Link:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})
    elif data == "edit_otp_channel_link":
        user_states[chat_id] = "wait_for_otp_channel_link"
        temp_data[chat_id] = {'msg_id': msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the new OTP Channel Link:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})


    elif data == "add_range_group":
        user_states[chat_id] = "wait_for_range_group_id"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text(
            "📡 <b>Range GROUP Add</b>\n\n"
            "যে গ্রুপে <b>Live Range</b> ফরওয়ার্ড হবে সেই গ্রুপের <b>Chat ID</b> পাঠান।\n"
            "উদাহরণ: <code>-1001234567890</code>\n\n"
            "⚠️ বটকে সেই গ্রুপে Admin হিসেবে অ্যাড করুন।"
        ), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})

    elif data == "edit_range_group_link":
        user_states[chat_id] = "wait_for_range_group_link"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text(
            "🔗 <b>Active GROUP Link</b>\n\n"
            "Range গ্রুপের invite লিংক পাঠান।\n"
            "উদাহরণ: <code>https://t.me/+xxxx</code> বা <code>https://t.me/groupname</code>\n\n"
            "মেম্বাররা নাম্বার নিতে গেলে <b>Active Range</b> বাটনে এই লিংক পাবে।"
        ), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_otp_groups", "style": "danger"}]]})

    elif data.startswith("rng_get_"):
        # Get number from live range card (works from Range GROUP)
        user_id = None
        try:
            user_id = int(call.get("from", {}).get("id") or 0)
        except Exception:
            user_id = None
        if not user_id:
            try:
                answer_callback(call["id"], "❌ User not found", show_alert=True)
            except Exception:
                pass
            return

        if is_user_banned(user_id):
            answer_callback(call["id"], "🚫 You are banned from using this bot!", show_alert=True)
            return
        if not check_force_join(user_id):
            answer_callback(call["id"], "⚠️ Please join our channels first, then open the bot and press /start.", show_alert=True)
            return

        parts = data.split("_")
        # rng_get_{src}_{range_or_token}
        src_code = parts[2] if len(parts) > 2 else "stx"
        raw_tail = "_".join(parts[3:]) if len(parts) > 3 else ""
        src_map = {"znx": "zenex", "vtx": "voltx", "stx": "stex"}
        source = src_map.get(src_code, "stex")

        range_str = ""
        service = "FACEBOOK"
        resolved = _range_resolve(raw_tail)
        if resolved:
            service, range_str, source = resolved
            source = src_map.get(src_code, source or "stex")
        else:
            # digits embedded in callback
            range_str = re.sub(r"\D", "", raw_tail) or raw_tail
            # try map by src:digits
            alt = _RANGE_TOKENS.get(f"{src_code}:{range_str}")
            if alt:
                service, range_str, source = alt
                source = src_map.get(src_code, source or "stex")
            else:
                # best-effort service from recent traffic
                for t in reversed(list(recent_traffic)[-80:]):
                    rr = str(t.get("real_range") or "").replace("X", "").replace("x", "")
                    if rr and range_str and (rr.startswith(range_str) or range_str.startswith(rr)):
                        service = str(t.get("service") or service).upper()
                        if not range_str:
                            range_str = rr
                        break

        range_str = re.sub(r"\D", "", str(range_str or ""))
        if not range_str:
            try:
                answer_callback(call["id"], "❌ Range expired — wait for a new live card", show_alert=True)
            except Exception:
                pass
            return

        # Cooldown per user (not group)
        now = time.time()
        if now - user_cooldowns.get(user_id, 0) < float(bot_settings.get("cooldown", 0) or 0):
            left = int(bot_settings.get("cooldown", 0) - (now - user_cooldowns.get(user_id, 0)))
            try:
                answer_callback(call["id"], f"⌛ Wait {max(1, left)}s", show_alert=True)
            except Exception:
                pass
            return
        user_cooldowns[user_id] = now
        try:
            answer_callback(call["id"], "⏳ Getting number...")
        except Exception:
            pass

        # Allocate: high-traffic panel first (stex/voltx/zenex), then fallbacks → better OTP rate
        num_str = None
        last_err = "out of stock"
        try:
            try_order = best_sources_for(range_str, service)
            # always include original source first if present, then traffic order
            if source and source not in try_order:
                try_order = [source] + try_order
            elif source in try_order:
                try_order = [source] + [s for s in try_order if s != source]
            if not try_order:
                try_order = [s for s in ("stex", "voltx", "zenex") if bot_settings.get(f"{s}_keys")]
            for src in try_order:
                if num_str:
                    break
                if src == "zenex":
                    for api_key in (bot_settings.get("zenex_keys") or []):
                        if not api_key:
                            continue
                        try:
                            headers = {"mapikey": api_key, "Content-Type": "application/json", "Accept": "application/json"}
                            payload = {"range": range_str, "is_national": False, "remove_plus": False}
                            res = requests.post(f"{ZENEX_BASE_URL}/v1/getnum", json=payload, headers=headers, timeout=12)
                            resp = res.json()
                            data_obj = resp.get("data") if isinstance(resp, dict) else None
                            ok = bool(isinstance(resp, dict) and (resp.get("success") is True or (resp.get("meta") or {}).get("code") == 200))
                            if ok and isinstance(data_obj, dict):
                                for k in ("full_number", "number", "no_plus_number", "national_number", "copy"):
                                    v = data_obj.get(k)
                                    if v:
                                        cand = re.sub(r"\D", "", str(v))
                                        if 6 <= len(cand) <= 18:
                                            num_str = cand
                                            break
                            elif ok and isinstance(data_obj, str):
                                cand = re.sub(r"\D", "", data_obj)
                                if 6 <= len(cand) <= 18:
                                    num_str = cand
                            if num_str:
                                zenex_assigned_numbers[num_str] = user_id
                                source = "zenex"
                                break
                            last_err = str(resp.get("message") or resp.get("meta") or "zenex fail")[:80]
                        except Exception as e:
                            last_err = str(e)[:80]
                elif src == "voltx":
                    for api_key in (bot_settings.get("voltx_keys") or []):
                        if not api_key:
                            continue
                        try:
                            headers = {"mauthapi": api_key, "Content-Type": "application/json"}
                            res = requests.post(f"{VOLTX_BASE_URL}/getnum", json={"rid": range_str}, headers=headers, timeout=12)
                            resp = res.json()
                            if resp.get("meta", {}).get("code") == 200 and resp.get("data"):
                                d = resp["data"]
                                cand = re.sub(r"\D", "", str(d.get("no_plus_number") or d.get("national_number") or d.get("full_number") or d.get("number") or ""))
                                if 6 <= len(cand) <= 18:
                                    num_str = cand
                                    voltx_assigned_numbers[num_str] = user_id
                                    source = "voltx"
                                    break
                            last_err = str(resp.get("message") or "voltx fail")[:80]
                        except Exception as e:
                            last_err = str(e)[:80]
                else:  # stex
                    for api_key in (bot_settings.get("stex_keys") or []):
                        if not api_key:
                            continue
                        try:
                            headers = {"mauthapi": api_key, "Content-Type": "application/json"}
                            res = requests.post(f"{STEX_BASE_URL}/getnum", json={"rid": range_str}, headers=headers, timeout=12)
                            resp = res.json()
                            if resp.get("meta", {}).get("code") == 200 and resp.get("data"):
                                d = resp["data"]
                                cand = re.sub(r"\D", "", str(d.get("no_plus_number") or d.get("national_number") or d.get("full_number") or d.get("number") or ""))
                                if 6 <= len(cand) <= 18:
                                    num_str = cand
                                    stex_assigned_numbers[num_str] = user_id
                                    source = "stex"
                                    break
                            last_err = str(resp.get("message") or "stex fail")[:80]
                        except Exception as e:
                            last_err = str(e)[:80]
        except Exception as e:
            last_err = str(e)[:80]

        if not num_str:
            try:
                answer_callback(call["id"], f"❌ No number: {last_err}"[:180], show_alert=True)
            except Exception:
                pass
            try:
                send_message(user_id, render_body_text(f"❌ <b>Number out of stock</b>\nRange: <code>{range_str}</code>\n{last_err}"))
            except Exception:
                pass
            return

        total_assigned_stats += 1
        try:
            assigned_number_rates[num_str] = float(bot_settings.get("otp_reward", 0.0))
        except Exception:
            pass
        try:
            expire_previous_number(user_id)
        except Exception:
            pass
        try:
            user_active_sessions[user_id] = {"msg_id": 0, "nums": [num_str]}
        except Exception:
            pass
        try:
            save_db()
        except Exception:
            pass

        display_num = f"+{num_str}" if not str(num_str).startswith("+") else str(num_str)
        char_flag, iso = get_flag_and_code(num_str)
        app_full_name, prem_app_html = get_service_info_html(service, "")
        flag_html = get_flag_info_html(display_num)
        body = render_body_text(
            f"✅ <b>Number Get Successfully</b>\n\n"
            f"{flag_html} {country_label(display_num)} | {app_full_name} {prem_app_html}\n\n"            
            f"📊 Range: <code>{range_str}</code>\n"
        )
        flag_emoji_id = "5780471598922337683"
        for flag_code, flag_data in bot_settings.get("premium_flags", {}).items():
            if iso == flag_data.get("iso") and flag_data.get("id"):
                flag_emoji_id = flag_data["id"]
                break
        kb = {
            "inline_keyboard": [
                [{"text": strip_cc(display_num), "icon_custom_emoji_id": flag_emoji_id, "copy_text": {"text": strip_cc(display_num)}, "style": "success"}],
            ]
        }
        rlink = str(bot_settings.get("range_group_link") or "").strip()
        if rlink.startswith("http"):
            kb["inline_keyboard"].append([{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": rlink, "style": "primary"}])
        # Change from same range
        kb["inline_keyboard"].append([{"text": "Change Number", "icon_custom_emoji_id": "5465368548702446780", "callback_data": f"rng_get_{src_code}_{range_str}"[:64], "style": "danger"}])

        ok = False
        try:
            send_message(user_id, body, reply_markup=kb)
            ok = True
        except Exception as e:
            print(f"rng_get dm fail: {e}")
        if not ok:
            # fallback: reply in current chat
            try:
                send_message(chat_id, body, reply_markup=kb)
            except Exception:
                pass
        try:
            answer_callback(call["id"], f"✅ {strip_cc(display_num)}", show_alert=False)
        except Exception:
            pass
        return


    elif data == "manage_panels":
        api_count = len([p for p in bot_settings["panels"] if p.get("type") == "API Panel"])
        cpt_count = len([p for p in bot_settings["panels"] if p.get("type", "API Panel") == "Auto Captcha Panel"])
        text = f"{PEM['gear']} <b>Panel Management</b>\n\nSelect which type of panel system you want to manage:"
        kb = {"inline_keyboard": [
            [{"text": f"Manage API Panels ({api_count})", "icon_custom_emoji_id": "5336972142066047577", "callback_data": "manage_api_panels", "style": "primary"}],
            [{"text": f"Manage Auto Captcha Panels ({cpt_count})", "icon_custom_emoji_id": "5353022963132174959", "callback_data": "manage_cpt_panels", "style": "success"}],
            [{"text": "Back to System", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "system_settings", "style": "danger"}]
        ]}
        edit_message(chat_id, msg_id, render_body_text(text), reply_markup=kb)

    elif data in ["manage_api_panels", "manage_cpt_panels"]:
        p_type = "API Panel" if data == "manage_api_panels" else "Auto Captcha Panel"
        p_list = [p for p in bot_settings["panels"] if p.get("type", "API Panel") == p_type]
        icon = f"{PEM['world']} API" if p_type == 'API Panel' else f"{PEM['lock']} Auto Captcha"
        
        text = f"{icon} <b>{p_type}s Management</b>\n\n👀 <b>Active Monitors:</b> {len(p_list)}\n\n🟢 <b>Available Providers:</b>\n"
        for p in p_list:
            status = "Monitoring" if p['status'] == 'ON' else "Stopped"
            login_state = p.get('login_status', '')
            if p['type'] == 'Auto Captcha Panel':
                conf = f" {login_state}" if login_state else f"{PEM['ok']} Configured"
            else:
                conf = f"{PEM['ok']} Configured" if p.get('api_url') else f"{PEM['no']} Not Configured"
            text += f"• {p['name']}: {PEM['ok'] if p['status']=='ON' else PEM['no']} {status} | {conf}\n"
        edit_message(chat_id, msg_id, render_body_text(text), reply_markup=typed_panels_list_keyboard(p_type))

    elif data in ["add_api_panel", "add_cpt_panel"]:
        user_states[chat_id] = "wait_for_panel_name"
        p_type = "api" if data == "add_api_panel" else "logc"
        temp_data[chat_id] = {"msg_id": msg_id, "add_type": p_type}
        edit_message(chat_id, msg_id, render_body_text("📝 Please send the name of the New Provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_{'api' if p_type=='api' else 'cpt'}_panels", "style": "danger"}]]})

    elif data.startswith("add_ptype_"):
        pass

    elif data in ["list_del_api", "list_del_cpt"]:
        p_type = "API Panel" if data == "list_del_api" else "Auto Captcha Panel"
        kb = []
        for idx, p in enumerate(bot_settings["panels"]):
            if p.get("type", "API Panel") == p_type:
                kb.append([{"text": f"Delete {p['name']}", "icon_custom_emoji_id": "5420130255174145507", "callback_data": f"do_del_pnl_{idx}", "style": "danger"}])
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"manage_{'api' if p_type=='API Panel' else 'cpt'}_panels", "style": "primary"}])
        edit_message(chat_id, msg_id, render_body_text(f"{PEM['trash']} <b>Select a Provider to Delete:</b>"), reply_markup={"inline_keyboard": kb})

    elif data.startswith("do_del_pnl_"):
        idx = int(data.split("_")[3])
        if 0 <= idx < len(bot_settings["panels"]):
            p_type = bot_settings["panels"][idx].get("type", "API Panel")
            del bot_settings["panels"][idx]
            save_db()
            answer_callback(call["id"], "✅ Provider Deleted!", show_alert=True)
            handle_callback({"message": {"chat": {"id": chat_id}, "message_id": msg_id}, "data": f"manage_{'api' if p_type=='API Panel' else 'cpt'}_panels", "id": "internal"})

    elif data.startswith("tog_pnl_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["panels"]):
            p = bot_settings["panels"][idx]
            
            p["status"] = "ON" if p["status"] == "OFF" else "OFF"
            save_db()
            
            if p["type"] == "Auto Captcha Panel":
                text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>Login Status:</b> {p.get('login_status', 'Unknown')}\n<b>Login URL:</b> <code>{p.get('login_url', 'None')}</code>\n<b>User:</b> <code>{p.get('username', 'None')}</code>"
            else:
                text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Token:</b> <code>{p.get('token', 'None')}</code>"
            edit_message(chat_id, msg_id, render_body_text(text), reply_markup=panel_config_keyboard(idx))

    elif data.startswith("conf_pnl_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["panels"]):
            p = bot_settings["panels"][idx]
            if p["type"] == "Auto Captcha Panel":
                text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>Login Status:</b> {p.get('login_status', 'Unknown')}\n<b>Login URL:</b> <code>{p.get('login_url', 'None')}</code>\n<b>User:</b> <code>{p.get('username', 'None')}</code>\n<b>Num Col:</b> {p.get('num_col_name')} (Idx: {p.get('num_col_idx')})\n<b>Msg Col:</b> {p.get('msg_col_name')} (Idx: {p.get('msg_col_idx')})"
            else:
                text = f"⚙️ <b>Configure {p['name']}</b>\n\n<b>Type:</b> {p['type']}\n<b>Status:</b> {'🟢 Monitoring' if p['status'] == 'ON' else '🔴 Stopped'}\n<b>API URL:</b> <code>{p.get('api_url', 'None')}</code>\n<b>Token:</b> <code>{p.get('token', 'None')}</code>\n<b>Full API URL:</b> <code>{p.get('full_api_url', 'None')}</code>"
            edit_message(chat_id, msg_id, render_body_text(text), reply_markup=panel_config_keyboard(idx))

    elif data.startswith("set_p_api_"):
        idx = int(data.split("_")[3])
        user_states[chat_id] = "wait_for_p_api"
        temp_data[chat_id] = {"msg_id": msg_id, "p_idx": idx}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the API URL for this provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_tok_"):
        idx = int(data.split("_")[3])
        user_states[chat_id] = "wait_for_p_tok"
        temp_data[chat_id] = {"msg_id": msg_id, "p_idx": idx}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the Token for this provider:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_fapi_"):
        idx = int(data.split("_")[3])
        user_states[chat_id] = "wait_for_p_fapi"
        temp_data[chat_id] = {"msg_id": msg_id, "p_idx": idx}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the FULL API URL (Example: http://api.com/get?key=YOUR_TOKEN&start=0):"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("set_p_rec_"):
        idx = int(data.split("_")[3])
        user_states[chat_id] = "wait_for_p_rec"
        temp_data[chat_id] = {"msg_id": msg_id, "p_idx": idx}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the number of records to fetch (e.g. 10).\nType <code>0</code> for Unlimited:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": f"conf_pnl_{idx}", "style": "danger"}]]})

    elif data.startswith("test_p_conn_"):
        idx = int(data.split("_")[3])
        p = bot_settings["panels"][idx]
        wait_msg = send_message(chat_id, render_body_text("⏳ Testing connection. Please wait..."))
        wait_msg_id = wait_msg.get("result", {}).get("message_id") if wait_msg else None
        answer_callback(call["id"])
        
        try:
            parsed = []
            raw_text = ""
            
            if p["type"] == "Auto Captcha Panel":
                sess = panel_sessions.get(idx)
                if not sess:
                    success = attempt_auto_login(p, idx)
                    if not success:
                        if wait_msg_id: delete_message(chat_id, wait_msg_id)
                        send_message(chat_id, render_body_text(f"❌ <b>Auto Login Failed!</b>\nReason: {html.escape(str(p.get('login_status', 'Unknown')))}"))
                        return
                    sess = panel_sessions.get(idx)
                    
                login_url = p.get("login_url", "").strip()
                if not login_url.startswith("http"): login_url = "http://" + login_url
                msg_link = p.get("msg_link", "").strip()
                if not msg_link.startswith("http") and msg_link != "": msg_link = "http://" + msg_link
                check_url = msg_link if msg_link else f"{login_url.split('/login')[0]}/client/SMSCDRStats"
                
                # 🌟 test connection supports sAjaxSource & HTML table parser
                parsed, raw_text = fetch_cpt_panel_cdrs(p, sess, check_url)
                
            else:
                full_url = p.get("full_api_url", "").strip()
                url = p.get("api_url", "").strip()
                token = p.get("token", "").strip()
                if not full_url and not url:
                    if wait_msg_id: delete_message(chat_id, wait_msg_id)
                    send_message(chat_id, render_body_text("❌ Please Set API URL or Full API URL first!"))
                    return
                
                urls_to_try = []
                if full_url:
                    urls_to_try.append(full_url)
                else:
                    if "{token}" in url or "{key}" in url:
                        urls_to_try.append(url.replace("{token}", token).replace("{key}", token))
                    elif "token=" in url or "key=" in url:
                        urls_to_try.append(url)
                    else:
                        sep = '&' if '?' in url else '?'
                        urls_to_try.append(f"{url}{sep}token={token}")
                        urls_to_try.append(f"{url}{sep}key={token}&start=0")
                        urls_to_try.append(f"{url}{sep}key={token}")
                    
                parsed = []
                raw_text = ""
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
                for try_url in urls_to_try:
                    try:
                        res = requests.get(try_url, headers=headers, timeout=10)
                        raw_text = res.text
                        parsed = parse_panel_response(raw_text, p)
                        if parsed:
                            if not full_url and try_url != url and token:
                                p["api_url"] = try_url.replace(token, "{token}")
                                save_db()
                            break
                    except Exception: pass
                 
            if wait_msg_id: delete_message(chat_id, wait_msg_id)
                 
            if parsed:
                txt = f"✅ <b>Connection Successful!</b>\n\n🎯 <b>Parsed Data Sample (Max 3):</b>\n\n"
                
                for i, sample in enumerate(parsed[:3]):
                    num = sample['number']
                    msg = sample['message']
                    otp = sample['otp']
                    
                    detected_app = detect_service(msg)
                    app_name = detected_app if detected_app else p.get("name", "Unknown")
                    app_full_name, prem_app_html = get_service_info_html(app_name, msg)
                    
                    txt += f"<b>{i+1}.</b> {prem_app_html} <b>{app_full_name}</b>\n"
                    txt += f"📱 Number: <code>{num}</code>\n"
                    txt += f"📝 Full Msg: <code>{html.escape(msg)}</code>\n"
                    txt += f"🔐 OTP: <code>{otp}</code>\n"
                    txt += "➖" * 12 + "\n"
                    
                send_message(chat_id, render_body_text(txt))
            else:
                if p["type"] == "Auto Captcha Panel":
                    try:
                        soup = BeautifulSoup(raw_text, 'html.parser')
                        tables = soup.find_all('table')
                        if tables:
                            full_table_data = "🔍 FULL TABLE DATA (A-Z)\n" + "="*50 + "\n\n"
                            for t_idx, table in enumerate(tables):
                                full_table_data += f"--- Table {t_idx+1} ---\n"
                                rows = table.find_all('tr')
                                for r_idx, row in enumerate(rows):
                                    cols = row.find_all(['th', 'td'])
                                    col_texts = [f"[{c_idx+1}] {c.get_text(separator=' ', strip=True)}" for c_idx, c in enumerate(cols)]
                                    full_table_data += f"Row {r_idx+1}: {' | '.join(col_texts)}\n"
                                full_table_data += "\n" + "="*50 + "\n"
                            
                            send_document(chat_id, f"Full_Panel_Data_{idx}.txt", full_table_data.encode('utf-8'))
                            fail_txt = f"⚠️ <b>Connected, but couldn't parse OTP data!</b>\n\n<i>আমি ওই লিংকের সম্পূর্ণ (A-Z) ডাটা একটি Text File এ পাঠিয়েছি। ফাইলটি ওপেন করে সঠিক Column Number (যেমন: [1], [3]) চেক করে প্যানেলে আপডেট করে নাও।</i>"
                            send_message(chat_id, render_body_text(fail_txt))
                        else:
                            send_message(chat_id, render_body_text(f"⚠️ <b>Connected, but no HTML Table found!</b>\nMake sure the message link is correct."))
                    except Exception as e:
                        send_message(chat_id, render_body_text(f"❌ <b>Error parsing HTML:</b> {html.escape(str(e))}"))
                else:
                    safe_html = html.escape(str(raw_text)[:300])
                    send_message(chat_id, render_body_text(f"⚠️ <b>Connected, but couldn't find/parse OTP data.</b>\n\n<i>Make sure your API config is correct.</i>\n\nRaw HTML/Data (excerpt):\n<code>{safe_html}...</code>"))
        except Exception as e:
            if wait_msg_id: delete_message(chat_id, wait_msg_id)
            send_message(chat_id, render_body_text(f"❌ <b>Connection Failed!</b>\nError: {html.escape(str(e))}"))

    elif data == "dxa_control":
        if chat_id in user_states: del user_states[chat_id]
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>"), reply_markup=dxa_control_keyboard())

    elif data == "dxa_toggle_w":
        bot_settings["withdraw_on"] = not bot_settings["withdraw_on"]
        save_db()
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>"), reply_markup=dxa_control_keyboard())

    elif data == "manage_w_methods":
        edit_message(chat_id, msg_id, render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())

    elif data == "add_wm":
        user_states[chat_id] = "wait_for_add_wm"
        temp_data[chat_id] = {"msg_id": msg_id}
        edit_message(chat_id, msg_id, render_body_text("📝 Send the name of the new Withdrawal Method:"), reply_markup={"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "manage_w_methods", "style": "danger"}]]})

    elif data.startswith("del_wm_"):
        idx = int(data.split("_")[2])
        if 0 <= idx < len(bot_settings["w_methods"]):
            del bot_settings["w_methods"][idx]
            save_db()
            answer_callback(call["id"], "✅ Method deleted!", show_alert=True)
            edit_message(chat_id, msg_id, render_body_text("💳 <b>WITHDRAWAL METHODS</b>\n\nManage your withdrawal methods below:"), reply_markup=w_methods_keyboard())

    elif data == "dxa_toggle_refmode":
        bot_settings["refer_mode"] = "once" if refer_every_otp() else "every"
        save_db()
        edit_message(chat_id, msg_id, render_body_text("🕹 <b>DXA CONTROL PANEL</b>"), reply_markup=dxa_control_keyboard())

    elif data.startswith("dxa_"):
        key = data.replace("dxa_", "")
        key_map = {"min_w": "min_withdraw", "otp_r": "otp_reward", "ref_r": "refer_reward", "ref_min": "refer_min_otps", "cool": "cooldown", "num_req": "num_req", "num_share": "num_share", "sup_link": "support_link", "w_group": "w_group", "ref_otp": "refer_otp_reward"}
        if key in key_map:
            temp_data[chat_id] = {"msg_id": msg_id, "key": key_map[key]}
            user_states[chat_id] = "set_dxa"
            cancel_kb = {"inline_keyboard": [[{"text": "Cancel", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "cancel_dxa_edit", "style": "danger"}]]}
            edit_message(chat_id, msg_id, render_body_text(f"📝 Please send the new value for <code>{key_map[key]}</code>:" + ("\n\nSend a link (https://t.me/yourname) or @username.\nSend <code>off</code> to hide the link." if key_map[key] == "support_link" else "")), reply_markup=cancel_kb)
            answer_callback(call["id"])

    elif data == "back_to_services":
        local_srvs = set([b["service"] for b in number_batches.values() if b["numbers"]])
        stex_srvs = set(bot_settings.get("stex_services", {}).keys())
        voltx_srvs = set(bot_settings.get("voltx_services", {}).keys())
        zenex_srvs = set(bot_settings.get("zenex_services", {}).keys())
        all_services = local_srvs.union(stex_srvs).union(voltx_srvs).union(zenex_srvs)
        
        if not all_services:
            answer_callback(call["id"], "❌ No numbers or services available!", show_alert=True)
            return
            
        c_msg = bot_settings["custom_messages"].get("get_number", {})
        txt = render_body_text(c_msg.get("text", f"{PEM['pin']} Select Service"))
        
        apps_db = bot_settings.get("premium_apps", {})
        kb = []
        for s in all_services:
            emoji_id = "5352694861990501856" 
            for app_key, app_data in apps_db.items():
                if s.upper() == app_key or s.upper() in app_key or app_key in s.upper():
                    if "id" in app_data:
                        emoji_id = app_data["id"]
                        break
            kb.append([{"text": f"{s}", "icon_custom_emoji_id": emoji_id, "callback_data": f"g_s_{s}", "style": "primary"}])
        
        for b in c_msg.get("buttons", []): 
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
        kb.append([{"text": "Close", "icon_custom_emoji_id": "5420130255174145507", "callback_data": "close_msg", "style": "danger"}])
        
        edit_message(chat_id, msg_id, txt, reply_markup={"inline_keyboard": kb})

    elif data.startswith("g_s_"):
        service = data.split("g_s_")[1]
        local_cnts = set([b["country"] for b in number_batches.values() if b["service"] == service and b["numbers"]])
        stex_cnts = set(bot_settings.get("stex_services", {}).get(service, {}).keys())
        voltx_cnts = set(bot_settings.get("voltx_services", {}).get(service, {}).keys())
        zenex_cnts = set(bot_settings.get("zenex_services", {}).get(service, {}).keys())
        all_countries = local_cnts.union(stex_cnts).union(voltx_cnts).union(zenex_cnts)
        
        c_msg = bot_settings["custom_messages"].get("select_country", {})
        raw_txt = c_msg.get("text", "📌 Select a country for {service}:").replace("{service}", service)
        txt = render_body_text(raw_txt)
        
        flags_db = bot_settings.get("premium_flags", {})
        kb = []
        for c in all_countries:
            emoji_id = "5780471598922337683" # Default flag
            for flag_code, flag_data in flags_db.items():
                iso = flag_data.get("iso", "").upper()
                name = flag_data.get("name", "").upper()
                if c.upper() == iso or c.upper() == name or c.upper() in name or name in c.upper():
                    if "id" in flag_data:
                        emoji_id = flag_data["id"]
                        break
            
            otp_price = bot_settings.get("otp_reward", 0.0)
            # 🌟 ফাইলের নির্দিষ্ট কোনো রেট থাকলে সেটা নিয়ে আসবে
            for b in number_batches.values():
                if b.get("service") == service and b.get("country") == c and "rate" in b and b.get("numbers"):
                    otp_price = b.get("rate")
                    break
                    
            kb.append([{"text": f"{c} - {otp_price} TK/OTP", "icon_custom_emoji_id": emoji_id, "callback_data": f"g_c_{service}_{c}", "style": "success"}])
        
        for b in c_msg.get("buttons", []): 
            b_copy = b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
            
        kb.append([{"text": "Back", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_services", "style": "danger"}])
        edit_message(chat_id, msg_id, txt, reply_markup={"inline_keyboard": kb})

    elif data.startswith("g_c_") or data.startswith("c_n_"):
        # ১. গ্লোবাল কুলডাউন চেক (সকল নাম্বার মেথডের জন্য)
        now = time.time()
        if now - user_cooldowns.get(chat_id, 0) < bot_settings["cooldown"]:
            answer_callback(call["id"], f"⌛ Please wait {int(bot_settings['cooldown'] - (now - user_cooldowns.get(chat_id, 0)))}s.", show_alert=True)
            return
        
        # কুলডাউন আপডেট
        user_cooldowns[chat_id] = now
        
        # আগের নাম্বার এক্সপায়ার করা
        expire_previous_number(chat_id)

        # যদি সার্চ নাম্বার থেকে আসে
        if data.startswith("c_n_s_"):
            is_voltx_req = data.endswith("_vtx")
            clean_data = data[:-4] if is_voltx_req else data
            parts_s = clean_data.split("_", 4)
            
            query = parts_s[3] if len(parts_s) > 3 else ""
            service_from_cb = parts_s[4] if len(parts_s) > 4 else None
            
            allowed_countries = bot_settings.get("search_countries", [])
            voltx_allowed = bot_settings.get("voltx_search_countries", [])
            
            zenex_allowed = bot_settings.get("zenex_search_countries", [])
            
            is_stex_allowed = any(query.startswith(c) for c in allowed_countries) if allowed_countries else False
            is_voltx_allowed = any(query.startswith(c) for c in voltx_allowed) if voltx_allowed else False
            is_zenex_allowed = any(query.startswith(c) for c in zenex_allowed) if zenex_allowed else False
            is_zenex_req = data.endswith("_znx")
            
            if not is_voltx_req and not is_zenex_req and not is_stex_allowed and not is_voltx_allowed and not is_zenex_allowed:
                answer_callback(call["id"], "❌ This country code is not allowed for search!", show_alert=True)
                return
                
            edit_message(chat_id, msg_id, render_body_text("⌛ <i>Processing... Finding Number...</i>"))
            wait_msg_id = msg_id
            
            found_indices = []
            for b_id, b_data in number_batches.items():
                for idx, n_obj in enumerate(b_data["numbers"]):
                    if n_obj["num"].replace("+", "").startswith(query) and chat_id not in n_obj.get("used_by", []):
                        found_indices.append((b_id, idx, n_obj["num"]))
            
            fetched_nums = []
            if not found_indices:
                api_found = False
                req_count = bot_settings.get("num_req", 1)
                
                # প্রথমে Zenex চেক করবে
                if is_zenex_allowed or is_zenex_req:
                    zenex_keys = bot_settings.get("zenex_keys", [])
                    for _ in range(req_count):
                        if len(fetched_nums) >= req_count: break
                        for api_key in zenex_keys:
                            try:
                                headers = {"mapikey": api_key}
                                payload = {"range": query, "is_national": False, "remove_plus": False}
                                res = requests.post(f"{ZENEX_BASE_URL}/v1/getnum", json=payload, headers=headers, timeout=10)
                                resp_data = res.json()
                                if resp_data.get("meta", {}).get("code") == 200 and resp_data.get("data"):
                                    num_str = str(resp_data["data"].get("full_number", "")).replace("+", "")
                                    if not num_str: num_str = str(resp_data["data"].get("number", "")).replace("+", "")
                                    fetched_nums.append(num_str)
                                    zenex_assigned_numbers[num_str] = chat_id 
                                    api_found = True
                                    total_assigned_stats += 1
                                    is_zenex_req = True 
                                    break
                            except Exception: continue

                # তারপর Voltx চেক করবে
                if len(fetched_nums) < req_count and (is_voltx_allowed or is_voltx_req) and not is_zenex_req:
                    voltx_keys = bot_settings.get("voltx_keys", [])
                    for _ in range(req_count):
                        if len(fetched_nums) >= req_count: break
                        for api_key in voltx_keys:
                            try:
                                headers = {"mauthapi": api_key}
                                payload = {"rid": query}
                                res = requests.post(f"{VOLTX_BASE_URL}/getnum", json=payload, headers=headers, timeout=10)
                                resp_data = res.json()
                                if resp_data.get("meta", {}).get("code") == 200 and resp_data.get("data"):
                                    num_str = str(resp_data["data"].get("no_plus_number", "")).replace("+", "")
                                    if not num_str: num_str = str(resp_data["data"].get("national_number", ""))
                                    fetched_nums.append(num_str)
                                    voltx_assigned_numbers[num_str] = chat_id 
                                    api_found = True
                                    total_assigned_stats += 1
                                    is_voltx_req = True # মার্ক করে দিলাম যাতে পরবর্তীতে Voltx রিকোয়েস্ট হিসেবে কাজ করে
                                    break
                            except Exception: continue

                # Voltx এ না পেলে বা আরও নাম্বার লাগলে StexSMS তে চেক করবে
                if len(fetched_nums) < req_count and (is_stex_allowed and not is_voltx_req and not is_zenex_req):
                    stex_keys = bot_settings.get("stex_keys", [])
                    for _ in range(req_count - len(fetched_nums)):
                        for api_key in stex_keys:
                            try:
                                headers = {"mauthapi": api_key}
                                res = requests.post(f"{STEX_BASE_URL}/getnum", json={"rid": query}, headers=headers, timeout=10)
                                resp_data = res.json()
                                if resp_data.get("meta", {}).get("code") == 200 and resp_data.get("data"):
                                    num_str = str(resp_data["data"].get("no_plus_number", "")).replace("+", "")
                                    if not num_str: num_str = str(resp_data["data"].get("national_number", ""))
                                    fetched_nums.append(num_str)
                                    stex_assigned_numbers[num_str] = chat_id 
                                    api_found = True
                                    total_assigned_stats += 1
                                    break
                            except Exception: continue
                        
                if not api_found:
                    answer_callback(call["id"], "❌ Number out of stock!", show_alert=True)
                    delete_message(chat_id, wait_msg_id)
                    return
                save_db()
            else:
                fetched_nums = claim_numbers(found_indices, chat_id, bot_settings.get("num_req", 1))
                if not fetched_nums:
                    answer_callback(call["id"], "Number out of stock!", show_alert=True)
                    delete_message(chat_id, wait_msg_id)
                    return
                
            kb = []
            if service_from_cb:
                app_full_name, _ = get_service_info_html(service_from_cb)
                emoji_id_srv = "5337302974806922068"
                for app_key, app_data in bot_settings.get("premium_apps", {}).items():
                    if service_from_cb.upper() == app_key or service_from_cb.upper() in app_key or app_key in service_from_cb.upper():
                        if "id" in app_data: emoji_id_srv = app_data["id"]; break
                kb.append([{"text": f"{app_full_name}", "icon_custom_emoji_id": emoji_id_srv, "callback_data": "ignore", "style": "primary"}])

            flags_db = bot_settings.get("premium_flags", {})
            country_emoji_html = ""
            for num in fetched_nums:
                char_flag, iso = get_flag_and_code(num)
                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                flag_emoji_id = "5780471598922337683"
                for flag_code, flag_data in flags_db.items():
                    if iso == flag_data.get("iso"):
                        if "id" in flag_data: 
                            flag_emoji_id = flag_data["id"]
                            country_emoji_html = f'<tg-emoji emoji-id="{flag_emoji_id}">{flag_data["char"]}</tg-emoji>'
                        break
                if not country_emoji_html: country_emoji_html = char_flag
                kb.append([{"text": f"{strip_cc(display_num)}", "icon_custom_emoji_id": flag_emoji_id, "copy_text": {"text": strip_cc(display_num)}, "style": "success"}])
            
            vtx_ext = "_vtx" if is_voltx_req else "_znx" if is_zenex_req else ""
            srv_ext = f"_{service_from_cb}" if service_from_cb else ""
            kb.append([{"text": "Change Number", "icon_custom_emoji_id": "5465368548702446780", "callback_data": f"c_n_s_{query}{srv_ext}{vtx_ext}", "style": "danger"}])

            
            c_btns = bot_settings["custom_messages"].get("search_number", {}).get("buttons", [])
            for c_b in c_btns: 
                b_copy = c_b.copy()
                if "style" not in b_copy: b_copy["style"] = "primary"
                kb.append([b_copy])
                
            if service_from_cb:
                kb.append([{"text": "Change Country", "icon_custom_emoji_id": "5305517382138112561", "callback_data": f"g_s_{service_from_cb}", "style": "danger"}])
                
            kb.append([{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
            
            text_numbers = render_body_text(f"{country_emoji_html} NEW NUMBER\n")
            edit_message(chat_id, wait_msg_id, text_numbers, reply_markup={"inline_keyboard": kb})
            user_active_sessions[chat_id] = {"msg_id": wait_msg_id, "nums": fetched_nums}
            return

        # যদি আপলোড করা বা সার্ভিস থেকে আসে
        parts = data.split("_")
        service = parts[2]
        country = parts[3]

        available_indices = []
        # Check Local Stock First
        for b_id, b_data in number_batches.items():
            if b_data["service"] == service and b_data["country"] == country:
                for idx, n_obj in enumerate(b_data["numbers"]):
                    if chat_id not in n_obj.get("used_by", []):
                        available_indices.append((b_id, idx, n_obj["num"]))

        # IF NO LOCAL STOCK, Check StexSMS, Voltx & Zenex Services
        if not available_indices:
            stex_srv_data = bot_settings.get("stex_services", {}).get(service, {}).get(country)
            voltx_srv_data = bot_settings.get("voltx_services", {}).get(service, {}).get(country)
            zenex_srv_data = bot_settings.get("zenex_services", {}).get(service, {}).get(country)
            
            target_range = None
            is_voltx = False
            is_zenex = False
            
            if stex_srv_data and len(stex_srv_data) > 0:
                target_range = random.choice(stex_srv_data)
            elif voltx_srv_data and len(voltx_srv_data) > 0:
                target_range = random.choice(voltx_srv_data)
                is_voltx = True
            elif zenex_srv_data and len(zenex_srv_data) > 0:
                target_range = random.choice(zenex_srv_data)
                is_zenex = True
                
            if target_range:
                user_cooldowns[chat_id] = 0
                vtx_flag = "_vtx" if is_voltx else "_znx" if is_zenex else ""
                handle_callback({"message": call["message"], "data": f"c_n_s_{target_range}_{service}{vtx_flag}", "id": call["id"]})
                return
            else:
                answer_callback(call["id"], "❌ Number out of stock or range missing!", show_alert=True)
                if data.startswith("c_n_"): delete_message(chat_id, msg_id)
                return

        fetched_nums = claim_numbers(available_indices, chat_id, bot_settings["num_req"])

        if not fetched_nums:
            answer_callback(call["id"], "❌ You have already taken all numbers or stock is empty!", show_alert=True)
            if data.startswith("c_n_"): delete_message(chat_id, msg_id)
            return

        app_full_name, _ = get_service_info_html(service)
        emoji_id = "5337302974806922068"
        apps_db = bot_settings.get("premium_apps", {})
        for app_key, app_data in apps_db.items():
            if service.upper() == app_key or service.upper() in app_key or app_key in service.upper():
                if "id" in app_data:
                    emoji_id = app_data["id"]
                    break
        kb = [[{"text": f"{app_full_name}", "icon_custom_emoji_id": emoji_id, "callback_data": "ignore", "style": "primary"}]]
        
        flags_db = bot_settings.get("premium_flags", {})
        country_emoji_html = ""
        for num in fetched_nums:
            char_flag, iso = get_flag_and_code(num)
            display_num = f"+{num}" if not num.startswith("+") else num
            
            flag_emoji_id = "5780471598922337683" # Default Flag
            for flag_code, flag_data in flags_db.items():
                if iso == flag_data.get("iso"):
                    if "id" in flag_data: 
                        flag_emoji_id = flag_data["id"]
                        country_emoji_html = f'<tg-emoji emoji-id="{flag_emoji_id}">{flag_data["char"]}</tg-emoji>'
                    break
            
            if not country_emoji_html: country_emoji_html = char_flag
            kb.append([{"text": f"{strip_cc(display_num)}", "icon_custom_emoji_id": flag_emoji_id, "copy_text": {"text": strip_cc(display_num)}, "style": "success"}])
            
        kb.append([{"text": "Change Number", "icon_custom_emoji_id": "5465368548702446780", "callback_data": f"c_n_{service}_{country}", "style": "danger"}])
                   
        c_btns = bot_settings["custom_messages"].get("get_number", {}).get("buttons", [])
        for c_b in c_btns: 
            b_copy = c_b.copy()
            if "style" not in b_copy: b_copy["style"] = "primary"
            kb.append([b_copy])
            
        kb.append([{"text": "Change Country", "icon_custom_emoji_id": "5305517382138112561", "callback_data": f"g_s_{service}", "style": "danger"}])
        kb.append([{"text": "OTP Group", "icon_custom_emoji_id": "5190447043545438788", "url": bot_settings["otp_link"], "style": "primary"}])
        
        text_numbers = render_body_text(f"{country_emoji_html} NEW NUMBER\n")
        # সবসময় মেসেজ ইডিট করবে (Change Number করলেও নতুন মেসেজ আসবে না)
        try:
            edit_message(chat_id, msg_id, text_numbers, reply_markup={"inline_keyboard": kb})
            user_active_sessions[chat_id] = {"msg_id": msg_id, "nums": fetched_nums}
        except Exception:
            # যদি মেসেজ ইডিট করা সম্ভব না হয় (যেমন অনেক আগের মেসেজ), তবে নতুন মেসেজ দিবে
            msg_res = send_message(chat_id, text_numbers, reply_markup={"inline_keyboard": kb})
            if msg_res and "result" in msg_res:
                user_active_sessions[chat_id] = {"msg_id": msg_res["result"]["message_id"], "nums": fetched_nums}

    elif data.startswith("wapp_") or data.startswith("wrej_"):
        # অ্যাডমিন চেক (User ID চেক করতে হবে)
        user_id_clicked = call["from"]["id"]
        if not is_admin(user_id_clicked):
            answer_callback(call["id"], "🚫 Only Bot Admins can process withdrawals!", show_alert=True)
            return
            
        action = "APPROVE" if data.startswith("wapp_") else "REJECT"
        req_id = data.replace("wapp_", "").replace("wrej_", "")
        
        with withdraw_lock:
            req_data = pending_withdrawals.pop(req_id, None)
        if req_data is not None:
            try: db.collection('withdrawals').document(req_id).update({"status": "approved" if action == "APPROVE" else "rejected"})
            except Exception: log.exception("withdraw status update failed %s", req_id)
            u_id, amt = req_data["user_id"], req_data["amount"]
            track_withdraw_result(action == "APPROVE", amt)
            num = req_data["number"]
            full_name = req_data.get("full_name", u_id)
            
            if action == "APPROVE" and len(num) >= 7:
                masked_num = f"{num[:4]}❖DXA❖{num[-3:]}"
            else:
                masked_num = num
            
            status_text = "APPROVED" if action == "APPROVE" else "REJECTED"
            emoji_icon_id = "5352694861990501856" if action == "APPROVE" else "5420130255174145507"
            new_text = f"🎙 <b>WITHDRAWAL {status_text}</b>\n\n👤 <b>USER:</b> <a href='tg://user?id={u_id}'>{full_name}</a>\n💳 <b>WITHDRAWAL:</b> {amt} TK\n🍏 <b>NUMBER:</b> <code>{masked_num}</code>\n🏦 <b>METHOD:</b> {req_data['method']}\n\n🧾 <b>REQ ID:</b> {req_id}\n👨‍⚖️ <b>PROCESSED BY ADMIN</b>"
            
            kb = {"inline_keyboard": [[{"text": status_text, "icon_custom_emoji_id": emoji_icon_id, "callback_data": "ignore", "style": "success" if action == "APPROVE" else "danger"}]]}
            edit_message(chat_id, msg_id, render_body_text(new_text), reply_markup=kb)
            
            if action == "REJECT":
                update_balance(u_id, amt, "refund", req_id) 
                send_message(u_id, render_body_text(f"❌ Your {amt} TK withdrawal request was rejected. Balance refunded."))
            else:
                send_message(u_id, render_body_text(f"{PEM['ok']} Your {amt} TK withdrawal request has been paid successfully!"))
            
            if db:
                try: db.collection('withdrawals').document(req_id).update({"status": "approved" if action == "APPROVE" else "rejected"})
                except Exception: pass
                
        else:
            answer_callback(call["id"], "❌ Request already processed!", show_alert=True)

# ==========================================
# Polling Loop
# ==========================================
def poll_otp_with_status(number_id, num_str, owner_id, api_key):
    headers = {"X-API-Key": api_key}
    for _ in range(150): # 150 * 4 seconds = 10 Minutes Polling
        try:
            res = requests.get(f"{STEX_BASE_URL}/api/v1/numbers/{number_id}/sms", headers=headers, timeout=10)
            data = res.json()
            if data.get("success") and data.get("otp"):
                otp = str(data["otp"])
                msg_text = data.get("message", f"Your code is {otp}")
                
                # 🌟 সম্পূর্ণ মেসেজ থেকে ড্যাশসহ বা বড় OTP খোঁজার ফিক্স
                extracted_otp = extract_otp_code(msg_text)
                if extracted_otp and len(extracted_otp) > len(otp):
                    otp = extracted_otp
                    
                # 🌟 সম্পূর্ণ মেসেজ থেকে সার্ভিস/অ্যাপ চেনার ফিক্স
                app_name = data.get("service", "StexSMS Service")
                detected_app = detect_service(msg_text)
                if detected_app:
                    app_name = detected_app
                
                unique_id = f"POLL_{number_id}_{otp}"
                if processed_otps.add_if_new(unique_id):
                    processed_otps.add(unique_id)
                    
                    char, iso = get_flag_and_code(num_str)
                    app_full_name, prem_app_html = get_service_info_html(app_name, msg_text)
                    
                    global recent_traffic
                    current_time = time.time()
                    recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                    recent_traffic.append({"service": app_full_name, "iso": iso, "flag": char, "number": num_str, "time": current_time})
                    save_local_db()
                    
                    display_num = f"+{num_str}" if not str(num_str).startswith("+") else str(num_str)
                    masked = mask_number(display_num)
                    lang = detect_language(msg_text)
                    
                    display_msg = render_body_text(f"╔═══════════════╗\n║ {prem_app_html} {get_flag_info_html(display_num)} {country_label(display_num)} {masked} {lang}\n╚═══════════════╝")
                    
                    for fw in bot_settings.get("fw_groups", []):
                        kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                        temp_row = []
                        styles = ["danger", "success", "primary"]
                        for i, btn in enumerate(fw.get("buttons", [])):
                            b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                            if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                            temp_row.append(b_obj)
                            if len(temp_row) == 2:
                                kb.append(temp_row)
                                temp_row = []
                        if temp_row: kb.append(temp_row)
                        send_message(fw["chat_id"], display_msg, reply_markup={"inline_keyboard": kb})
                    
                    inbox_msg = render_body_text(f"╔═══════════════╗\n║ {prem_app_html} {get_flag_info_html(display_num)} {country_label(display_num)} {strip_cc(display_num)} {lang}\n╚═══════════════╝")
                    inbox_kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": otp}, "style": "success"}]]
                    
                    reward = float(assigned_number_rates.get(num_str, bot_settings.get("otp_reward", 0.0)))
                    _first = otp_reward_once(owner_id, num_str, otp)
                    if reward > 0 and _first:
                        update_balance(owner_id, reward, "otp", str(num_str))
                        inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "ignore", "style": "primary"}])
                    
                    send_message(owner_id, inbox_msg, reply_markup={"inline_keyboard": inbox_kb})
                    
                    if _first:
                        bump_otp_count(owner_id)
                break
        except Exception: pass
        time.sleep(4)

def stex_console_listener():
    global recent_traffic, bot_settings
    seen_console_hits = BoundedSet(5000)
    last_auto_update = time.time()
    
    while True:
        try:
            stex_keys = bot_settings.get("stex_keys", [])
            if stex_keys:
                api_key = stex_keys[0]
                headers = {"mauthapi": api_key}
                res = requests.get(f"{STEX_BASE_URL}/console", headers=headers, timeout=10)
                data = res.json()
                
                if data.get("meta", {}).get("code") == 200 and data.get("data", {}).get("hits"):
                    current_time = time.time()
                    new_hits = False
                    
                    for hit in data["data"]["hits"]:
                        range_str = str(hit.get("range", "")).replace("X", "")
                        if len(range_str) > 9:
                            range_str = range_str[:9]
                        sid = str(hit.get("sid", "Unknown"))
                        msg = str(hit.get("message", ""))
                        hit_time = hit.get("time", current_time * 1000) / 1000.0
                        
                        unique_hit = f"STEX_{range_str}_{sid}_{hit.get('time', 0)}"
                        if unique_hit not in seen_console_hits and range_str:
                            seen_console_hits.add(unique_hit)
                            
                            char, iso = get_flag_and_code(range_str)
                            app_full_name, _ = get_service_info_html(sid, msg)
                            
                            recent_traffic.append({
                                "service": app_full_name,
                                "iso": iso,
                                "flag": char,
                                "number": f"{range_str}XXX", 
                                "time": hit_time,
                                "real_range": range_str,
                                "source": "stex"
                            })
                            new_hits = True
                            try:
                                post_live_range_card(sid, range_str, source="stex", hits=1, sms_body=msg)
                            except Exception:
                                pass
                            
                    if new_hits:
                        recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                        save_local_db()
                        
                    # 🌟 Auto ADD and DELETE logic for Stex every 2 minutes
                    if bot_settings.get("stex_auto", False) and current_time - last_auto_update > 120:
                        last_auto_update = current_time
                        changed = False
                        
                        if "stex_services" not in bot_settings: bot_settings["stex_services"] = {}
                        if "search_countries" not in bot_settings: bot_settings["search_countries"] = []
                        
                        srv_counts = Counter(t["service"].upper() for t in recent_traffic if "real_range" in t)
                        top_srvs = [s for s, c in srv_counts.most_common(6)]
                        
                        for srv in top_srvs:
                            if srv not in bot_settings["stex_services"]:
                                bot_settings["stex_services"][srv] = {}
                                changed = True
                                
                            iso_counts = Counter(t["iso"] for t in recent_traffic if t.get("service", "").upper() == srv and "real_range" in t)
                            top_isos = [i for i, c in iso_counts.most_common(3)]
                            
                            for iso in top_isos:
                                full_country_name = get_flag_info_html(iso, return_full_name=True).title()
                                if full_country_name not in bot_settings["stex_services"][srv]:
                                    bot_settings["stex_services"][srv][full_country_name] = []
                                    changed = True
                                    
                                rng_counts = Counter(t["real_range"] for t in recent_traffic if t.get("service", "").upper() == srv and t.get("iso") == iso and "real_range" in t)
                                top_rngs = [r for r, c in rng_counts.most_common(2)]
                                
                                for rng in top_rngs:
                                    if rng not in bot_settings["stex_services"][srv][full_country_name]:
                                        bot_settings["stex_services"][srv][full_country_name].append(rng)
                                        changed = True
                                        
                                    country_code = rng[:3]
                                    if country_code not in bot_settings["search_countries"]:
                                        bot_settings["search_countries"].append(country_code)
                                        changed = True

                        services_to_remove = []
                        for srv, countries in list(bot_settings["stex_services"].items()):
                            srv_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and "real_range" in t and current_time - t.get("time", 0) <= 120)
                            if srv_hit < 1:
                                services_to_remove.append(srv)
                                continue
                            
                            countries_to_remove = []
                            for country, ranges in list(countries.items()):
                                c_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and get_flag_info_html(t.get("iso"), return_full_name=True).title() == country and "real_range" in t and current_time - t.get("time", 0) <= 120)
                                if c_hit < 1:
                                    countries_to_remove.append(country)
                                    continue
                                
                                ranges_to_remove = []
                                for rng in ranges:
                                    r_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and t.get("real_range") == rng and current_time - t.get("time", 0) <= 120)
                                    if r_hit < 1: 
                                        ranges_to_remove.append(rng)
                                
                                for r in ranges_to_remove:
                                    ranges.remove(r)
                                    changed = True
                                    
                            for c in countries_to_remove:
                                del bot_settings["stex_services"][srv][c]
                                changed = True
                                
                        for s in services_to_remove:
                            del bot_settings["stex_services"][s]
                            changed = True
                            
                        if changed:
                            save_db()
                            
        except Exception as e:
            pass
        time.sleep(10)

def voltx_console_listener():
    global recent_traffic, bot_settings
    seen_console_hits = BoundedSet(5000)
    last_auto_update = time.time()
    
    while True:
        try:
            if not bot_settings.get("voltx_on", True):
                time.sleep(5)
                continue
            voltx_keys = bot_settings.get("voltx_keys", [])
            if voltx_keys:
                api_key = voltx_keys[0]
                headers = {"mauthapi": api_key}
                res = requests.get(f"{VOLTX_BASE_URL}/console", headers=headers, timeout=10)
                data = res.json()
                
                if data.get("meta", {}).get("code") == 200 and data.get("data", {}).get("hits"):
                    current_time = time.time()
                    new_hits = False
                    
                    for hit in data["data"]["hits"]:
                        range_str = str(hit.get("range", "")).replace("X", "")
                        if len(range_str) > 9:
                            range_str = range_str[:9] # 9 ডিজিটের বেশি হলে প্রথম 9 ডিজিট নেবে
                        sid = str(hit.get("sid", "Unknown"))
                        msg = str(hit.get("message", ""))
                        hit_time = hit.get("time", current_time * 1000) / 1000.0
                        
                        unique_hit = f"{range_str}_{sid}_{hit.get('time', 0)}"
                        if unique_hit not in seen_console_hits and range_str:
                            seen_console_hits.add(unique_hit)
                            
                            char, iso = get_flag_and_code(range_str)
                            app_full_name, _ = get_service_info_html(sid, msg)
                            
                            recent_traffic.append({
                                "service": app_full_name,
                                "iso": iso,
                                "flag": char,
                                "number": f"{range_str}XXX", 
                                "time": hit_time,
                                "real_range": range_str,
                                "source": "voltx"
                            })
                            new_hits = True
                            try:
                                post_live_range_card(sid, range_str, source="voltx", hits=1, sms_body=msg)
                            except Exception:
                                pass
                            
                    if new_hits:
                        recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                        save_local_db()
                        
                    # 🌟 Auto ADD and DELETE logic every 2 minutes (120 seconds)
                    if bot_settings.get("voltx_auto", False) and current_time - last_auto_update > 120:
                        last_auto_update = current_time
                        changed = False
                        
                        if "voltx_services" not in bot_settings: bot_settings["voltx_services"] = {}
                        if "voltx_search_countries" not in bot_settings: bot_settings["voltx_search_countries"] = []
                        
                        # --- ১. Auto ADD Logic ---
                        srv_counts = Counter(t["service"].upper() for t in recent_traffic if "real_range" in t)
                        top_srvs = [s for s, c in srv_counts.most_common(6)] # Top 6 Services
                        
                        for srv in top_srvs:
                            if srv not in bot_settings["voltx_services"]:
                                bot_settings["voltx_services"][srv] = {}
                                changed = True
                                
                            iso_counts = Counter(t["iso"] for t in recent_traffic if t.get("service", "").upper() == srv and "real_range" in t)
                            top_isos = [i for i, c in iso_counts.most_common(3)] # Top 3 Countries
                            
                            for iso in top_isos:
                                full_country_name = get_flag_info_html(iso, return_full_name=True).title()
                                if full_country_name not in bot_settings["voltx_services"][srv]:
                                    bot_settings["voltx_services"][srv][full_country_name] = []
                                    changed = True
                                    
                                rng_counts = Counter(t["real_range"] for t in recent_traffic if t.get("service", "").upper() == srv and t.get("iso") == iso and "real_range" in t)
                                top_rngs = [r for r, c in rng_counts.most_common(2)] # Top 2 Ranges
                                
                                for rng in top_rngs:
                                    if rng not in bot_settings["voltx_services"][srv][full_country_name]:
                                        bot_settings["voltx_services"][srv][full_country_name].append(rng)
                                        changed = True
                                        
                                    # 🌟 শুধুমাত্র ৩ ডিজিট কান্ট্রি কোড অ্যাড করবে
                                    country_code = rng[:3]
                                    if country_code not in bot_settings["voltx_search_countries"]:
                                        bot_settings["voltx_search_countries"].append(country_code)
                                        changed = True

                        # --- ২. Auto DELETE Logic (Only if NO traffic in the LAST 120 SECONDS) ---
                        services_to_remove = []
                        for srv, countries in list(bot_settings["voltx_services"].items()):
                            # চেক করবে গত ১২০ সেকেন্ডে কোনো ট্রাফিক আছে কি না
                            srv_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and "real_range" in t and current_time - t.get("time", 0) <= 120)
                            if srv_hit < 1:
                                services_to_remove.append(srv)
                                continue
                            
                            countries_to_remove = []
                            for country, ranges in list(countries.items()):
                                c_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and get_flag_info_html(t.get("iso"), return_full_name=True).title() == country and "real_range" in t and current_time - t.get("time", 0) <= 120)
                                if c_hit < 1:
                                    countries_to_remove.append(country)
                                    continue
                                
                                ranges_to_remove = []
                                for rng in ranges:
                                    r_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and t.get("real_range") == rng and current_time - t.get("time", 0) <= 120)
                                    if r_hit < 1: 
                                        ranges_to_remove.append(rng)
                                
                                for r in ranges_to_remove:
                                    ranges.remove(r)
                                    changed = True
                                    
                            for c in countries_to_remove:
                                del bot_settings["voltx_services"][srv][c]
                                changed = True
                                
                        for s in services_to_remove:
                            del bot_settings["voltx_services"][s]
                            changed = True
                            
                        if changed:
                            save_db()
                            
        except Exception as e:
            pass
        time.sleep(10)

def voltx_sms_listener():
    global processed_otps, recent_traffic, voltx_assigned_numbers
    while True:
        try:
            voltx_keys = bot_settings.get("voltx_keys", [])
            for api_key in voltx_keys:
                try:
                    headers = {"mauthapi": api_key}
                    res = requests.get(f"{VOLTX_BASE_URL}/success-otp", headers=headers, timeout=10)
                    resp_data = res.json()
                    
                    if resp_data.get("meta", {}).get("code") == 200 and "data" in resp_data and "otps" in resp_data["data"]:
                        for item in resp_data["data"]["otps"]:
                            num = str(item.get("number", "")).replace("+", "")
                            msg_text = str(item.get("message", ""))
                            otp = extract_otp_code(msg_text) or "CODE"
                            otp_id = str(item.get("otp_id", otp))
                            
                            app_name = "Voltx Service"
                            detected_app = detect_service(msg_text)
                            if detected_app: app_name = detected_app
                                
                            unique_id = f"VOLTX_{num}_{otp_id}"
                            
                            if num and processed_otps.add_if_new(unique_id):
                                processed_otps.add(unique_id)
                                
                                char, iso = get_flag_and_code(num)
                                app_full_name, prem_app_html = get_service_info_html(app_name, msg_text)
                                current_time = time.time()
                                
                                recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                                recent_traffic.append({"service": app_full_name, "iso": iso, "flag": char, "number": num, "time": current_time})
                                save_local_db()
                                
                                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                                masked = mask_number(display_num)
                                lang = detect_language(msg_text)
                                
                                lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                display_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {masked} | 💬 {lang_name}")
                                
                                for fw in bot_settings.get("fw_groups", []):
                                    kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                                    temp_row = []
                                    styles = ["danger", "success", "primary"]
                                    for i, btn in enumerate(fw.get("buttons", [])):
                                        b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                                        if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                                        temp_row.append(b_obj)
                                        if len(temp_row) == 2:
                                            kb.append(temp_row)
                                            temp_row = []
                                    if temp_row: kb.append(temp_row)
                                    send_message(fw["chat_id"], display_msg, reply_markup={"inline_keyboard": kb})
                                    
                                owner_id = None
                                for uid, session_data in user_active_sessions.items():
                                    for act_num in session_data.get("nums", []):
                                        if numbers_match(act_num, num):
                                            owner_id = uid
                                            break
                                    if owner_id: break
                                    
                                if not owner_id:
                                    for vtx_n, n_owner in voltx_assigned_numbers.items():
                                        if numbers_match(vtx_n, num):
                                            owner_id = n_owner
                                            break
                                    if not owner_id:
                                        for stex_n, n_owner in stex_assigned_numbers.items():
                                            if numbers_match(stex_n, num):
                                                owner_id = n_owner
                                                break
                                    if not owner_id:
                                        for znx_n, n_owner in zenex_assigned_numbers.items():
                                            if numbers_match(znx_n, num):
                                                owner_id = n_owner
                                                break
                                        
                                if owner_id:
                                    lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                    inbox_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {strip_cc(display_num)} | 💬 {lang_name}")
                                    inbox_kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": otp}, "style": "success"}]]
                                    
                                    reward = float(assigned_number_rates.get(_norm_num(num), bot_settings.get("otp_reward", 0.0)))
                                    _first = otp_reward_once(owner_id, num, otp)
                                    if reward > 0 and _first:
                                        update_balance(owner_id, reward, "otp", str(num))
                                        inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "ignore", "style": "primary"}])
                                    
                                    send_message(owner_id, inbox_msg, reply_markup={"inline_keyboard": inbox_kb})
                                    
                                    if _first:
                                        bump_otp_count(owner_id)
                except Exception: pass
        except Exception: pass
        time.sleep(5)

def zenex_sms_listener():
    global processed_otps, recent_traffic, zenex_assigned_numbers
    while True:
        try:
            zenex_keys = bot_settings.get("zenex_keys", [])
            for api_key in zenex_keys:
                try:
                    headers = {"mapikey": api_key}
                    res = requests.get(f"{ZENEX_BASE_URL}/v1/numsuccess/info", headers=headers, timeout=10)
                    resp_data = res.json()
                    
                    if resp_data.get("meta", {}).get("code") == 200 and "data" in resp_data and "otps" in resp_data["data"]:
                        for item in resp_data["data"]["otps"]:
                            num = str(item.get("number", "")).replace("+", "")
                            msg_text = str(item.get("otp", ""))
                            otp = extract_otp_code(msg_text) or "CODE"
                            otp_id = str(item.get("nid", otp))
                            
                            app_name = "Zenex Service"
                            detected_app = detect_service(msg_text)
                            if detected_app: app_name = detected_app
                                
                            unique_id = f"ZENEX_{num}_{otp_id}"
                            
                            if num and processed_otps.add_if_new(unique_id):
                                processed_otps.add(unique_id)
                                
                                char, iso = get_flag_and_code(num)
                                app_full_name, prem_app_html = get_service_info_html(app_name, msg_text)
                                current_time = time.time()
                                
                                recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                                recent_traffic.append({"service": app_full_name, "iso": iso, "flag": char, "number": num, "time": current_time})
                                save_local_db()
                                
                                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                                masked = mask_number(display_num)
                                lang = detect_language(msg_text)
                                
                                lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                display_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {masked} | 💬 {lang_name}")
                                
                                for fw in bot_settings.get("fw_groups", []):
                                    kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                                    temp_row = []
                                    styles = ["danger", "success", "primary"]
                                    for i, btn in enumerate(fw.get("buttons", [])):
                                        b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                                        if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                                        temp_row.append(b_obj)
                                        if len(temp_row) == 2:
                                            kb.append(temp_row)
                                            temp_row = []
                                    if temp_row: kb.append(temp_row)
                                    send_message(fw["chat_id"], display_msg, reply_markup={"inline_keyboard": kb})
                                    
                                owner_id = None
                                for uid, session_data in user_active_sessions.items():
                                    for act_num in session_data.get("nums", []):
                                        if numbers_match(act_num, num):
                                            owner_id = uid
                                            break
                                    if owner_id: break
                                    
                                if not owner_id:
                                    for znx_n, n_owner in zenex_assigned_numbers.items():
                                        if numbers_match(znx_n, num):
                                            owner_id = n_owner
                                            break
                                    if not owner_id:
                                        for stex_n, n_owner in stex_assigned_numbers.items():
                                            if numbers_match(stex_n, num):
                                                owner_id = n_owner
                                                break
                                    if not owner_id:
                                        for vtx_n, n_owner in voltx_assigned_numbers.items():
                                            if numbers_match(vtx_n, num):
                                                owner_id = n_owner
                                                break
                                        
                                if owner_id:
                                    inbox_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {strip_cc(display_num)} | 💬 {lang_name}")
                                    inbox_kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": otp}, "style": "success"}]]
                                    
                                    reward = float(assigned_number_rates.get(_norm_num(num), bot_settings.get("otp_reward", 0.0)))
                                    _first = otp_reward_once(owner_id, num, otp)
                                    if reward > 0 and _first:
                                        update_balance(owner_id, reward, "otp", str(num))
                                        inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "ignore", "style": "primary"}])
                                    
                                    send_message(owner_id, inbox_msg, reply_markup={"inline_keyboard": inbox_kb})
                                    
                                    if _first:
                                        bump_otp_count(owner_id)
                except Exception: pass
        except Exception: pass
        time.sleep(5)

def global_sms_listener():
    global processed_otps, recent_traffic, stex_assigned_numbers
    while True:
        try:
            stex_keys = bot_settings.get("stex_keys", [])
            for api_key in stex_keys:
                try:
                    headers = {"mauthapi": api_key}
                    res = requests.get(f"{STEX_BASE_URL}/success-otp", headers=headers, timeout=10)
                    resp_data = res.json()
                    
                    if resp_data.get("meta", {}).get("code") == 200 and "data" in resp_data and "otps" in resp_data["data"]:
                        for item in resp_data["data"]["otps"]:
                            num = str(item.get("number", "")).replace("+", "")
                            msg_text = str(item.get("message", ""))
                            otp = extract_otp_code(msg_text) or "CODE"
                            otp_id = str(item.get("otp_id", otp))
                            
                            app_name = "Stex Service"
                            detected_app = detect_service(msg_text)
                            if detected_app: app_name = detected_app
                                
                            unique_id = f"STEX_{num}_{otp_id}"
                            
                            if num and processed_otps.add_if_new(unique_id):
                                processed_otps.add(unique_id)
                                
                                char, iso = get_flag_and_code(num)
                                app_full_name, prem_app_html = get_service_info_html(app_name, msg_text)
                                current_time = time.time()
                                
                                recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                                recent_traffic.append({"service": app_full_name, "iso": iso, "flag": char, "number": num, "time": current_time})
                                save_local_db()
                                
                                display_num = f"+{num}" if not str(num).startswith("+") else str(num)
                                masked = mask_number(display_num)
                                lang = detect_language(msg_text)
                                
                                lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                display_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {masked} | 💬 {lang_name}")
                                
                                for fw in bot_settings.get("fw_groups", []):
                                    kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5296369303661067030", "copy_text": {"text": otp}, "style": "success"}]]
                                    temp_row = []
                                    styles = ["danger", "success", "primary"]
                                    for i, btn in enumerate(fw.get("buttons", [])):
                                        b_obj = {"text": btn["text"], "url": btn["url"], "style": styles[i % 3]}
                                        if "icon_custom_emoji_id" in btn: b_obj["icon_custom_emoji_id"] = btn["icon_custom_emoji_id"]
                                        temp_row.append(b_obj)
                                        if len(temp_row) == 2:
                                            kb.append(temp_row)
                                            temp_row = []
                                    if temp_row: kb.append(temp_row)
                                    send_message(fw["chat_id"], display_msg, reply_markup={"inline_keyboard": kb})
                                    
                                owner_id = None
                                for uid, session_data in user_active_sessions.items():
                                    for act_num in session_data.get("nums", []):
                                        if numbers_match(act_num, num):
                                            owner_id = uid
                                            break
                                    if owner_id: break
                                    
                                if not owner_id:
                                    for stex_n, n_owner in stex_assigned_numbers.items():
                                        if numbers_match(stex_n, num):
                                            owner_id = n_owner
                                            break
                                    if not owner_id:
                                        for vtx_n, n_owner in voltx_assigned_numbers.items():
                                            if numbers_match(vtx_n, num):
                                                owner_id = n_owner
                                                break
                                    if not owner_id:
                                        for znx_n, n_owner in zenex_assigned_numbers.items():
                                            if numbers_match(znx_n, num):
                                                owner_id = n_owner
                                                break
                                        
                                if owner_id:
                                    lang_name = LANG_MAP.get(lang, "English") if 'LANG_MAP' in globals() else lang.replace("#", "")
                                    inbox_msg = render_body_text(f"{get_flag_info_html(display_num)} {country_label(display_num)} | {prem_app_html} {strip_cc(display_num)} | 💬 {lang_name}")
                                    inbox_kb = [[{"text": f"{otp}", "icon_custom_emoji_id": "5353022963132174959", "copy_text": {"text": otp}, "style": "success"}]]
                                    
                                    reward = float(assigned_number_rates.get(_norm_num(num), bot_settings.get("otp_reward", 0.0)))
                                    _first = otp_reward_once(owner_id, num, otp)
                                    if reward > 0 and _first:
                                        update_balance(owner_id, reward, "otp", str(num))
                                        inbox_kb.append([{"text": f"Added {reward} tk", "icon_custom_emoji_id": "5420396762189831222", "callback_data": "ignore", "style": "primary"}])
                                    
                                    send_message(owner_id, inbox_msg, reply_markup={"inline_keyboard": inbox_kb})
                                    
                                    if _first:
                                        bump_otp_count(owner_id)
                except Exception: pass
        except Exception: pass
        time.sleep(5)

def zenex_console_listener():
    global recent_traffic, bot_settings
    last_auto_update = time.time()
    zenex_last_hits = {} 
    
    while True:
        try:
            zenex_keys = bot_settings.get("zenex_keys", [])
            if zenex_keys:
                api_key = zenex_keys[0]
                headers = {"mapikey": api_key}
                res = requests.get(f"{ZENEX_BASE_URL}/v1/active-ranges", headers=headers, timeout=10)
                data = res.json()
                
                if data.get("success") and "data" in data and "active_ranges" in data["data"]:
                    current_time = time.time()
                    new_hits_added = False
                    
                    for hit in data["data"]["active_ranges"]:
                        range_str = str(hit.get("range", "")).replace("X", "").replace("x", "")
                        sid = str(hit.get("service", "Unknown")).upper()
                        hits_count = int(hit.get("hits", 1))
                        
                        if range_str:
                            char, iso = get_flag_and_code(range_str)
                            app_full_name, _ = get_service_info_html(sid, "")
                            
                            range_key = f"{range_str}_{app_full_name}"
                            prev_hits = zenex_last_hits.get(range_key, 0)
                            
                            if hits_count > prev_hits:
                                new_hits = hits_count - prev_hits
                            else:
                                new_hits = hits_count
                                
                            zenex_last_hits[range_key] = hits_count
                            if new_hits > 150: new_hits = 150
                            
                            for _ in range(new_hits):
                                recent_traffic.append({
                                    "service": app_full_name,
                                    "iso": iso,
                                    "flag": char,
                                    "number": f"{range_str}XXX", 
                                    "time": current_time,
                                    "real_range": range_str,
                                    "source": "zenex"
                                })
                                new_hits_added = True
                            try:
                                if new_hits > 0:
                                    post_live_range_card(sid, range_str, source="zenex", hits=hits_count, sms_body=msg)
                            except Exception:
                                pass
                            
                    if new_hits_added:
                        recent_traffic = [t for t in recent_traffic if current_time - t.get("time", 0) <= 3600]
                        save_local_db()
                        
                    if bot_settings.get("zenex_auto", False) and current_time - last_auto_update > 120:
                        last_auto_update = current_time
                        changed = False
                        
                        if "zenex_services" not in bot_settings: bot_settings["zenex_services"] = {}
                        if "zenex_search_countries" not in bot_settings: bot_settings["zenex_search_countries"] = []
                        
                        srv_counts = Counter(t["service"].upper() for t in recent_traffic if t.get("source") == "zenex" and "real_range" in t)
                        top_srvs = [s for s, c in srv_counts.most_common(6)]
                        
                        for srv in top_srvs:
                            if srv not in bot_settings["zenex_services"]:
                                bot_settings["zenex_services"][srv] = {}
                                changed = True
                                
                            iso_counts = Counter(t["iso"] for t in recent_traffic if t.get("service", "").upper() == srv and t.get("source") == "zenex" and "real_range" in t)
                            top_isos = [i for i, c in iso_counts.most_common(3)]
                            
                            for iso in top_isos:
                                full_country_name = get_flag_info_html(iso, return_full_name=True).title()
                                if full_country_name not in bot_settings["zenex_services"][srv]:
                                    bot_settings["zenex_services"][srv][full_country_name] = []
                                    changed = True
                                    
                                rng_counts = Counter(t["real_range"] for t in recent_traffic if t.get("service", "").upper() == srv and t.get("iso") == iso and t.get("source") == "zenex" and "real_range" in t)
                                top_rngs = [r for r, c in rng_counts.most_common(2)]
                                
                                for rng in top_rngs:
                                    if rng not in bot_settings["zenex_services"][srv][full_country_name]:
                                        bot_settings["zenex_services"][srv][full_country_name].append(rng)
                                        changed = True
                                        
                                    country_code = rng[:3]
                                    if country_code not in bot_settings["zenex_search_countries"]:
                                        bot_settings["zenex_search_countries"].append(country_code)
                                        changed = True

                        services_to_remove = []
                        for srv, countries in list(bot_settings["zenex_services"].items()):
                            srv_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and t.get("source") == "zenex" and "real_range" in t and current_time - t.get("time", 0) <= 120)
                            if srv_hit < 1:
                                services_to_remove.append(srv)
                                continue
                            
                            countries_to_remove = []
                            for country, ranges in list(countries.items()):
                                c_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and get_flag_info_html(t.get("iso"), return_full_name=True).title() == country and t.get("source") == "zenex" and "real_range" in t and current_time - t.get("time", 0) <= 120)
                                if c_hit < 1:
                                    countries_to_remove.append(country)
                                    continue
                                
                                ranges_to_remove = []
                                for rng in ranges:
                                    r_hit = sum(1 for t in recent_traffic if t.get("service", "").upper() == srv.upper() and t.get("real_range") == rng and t.get("source") == "zenex" and current_time - t.get("time", 0) <= 120)
                                    if r_hit < 1: 
                                        ranges_to_remove.append(rng)
                                
                                for r in ranges_to_remove:
                                    ranges.remove(r)
                                    changed = True
                                    
                            for c in countries_to_remove:
                                del bot_settings["zenex_services"][srv][c]
                                changed = True
                                
                        for s in services_to_remove:
                            del bot_settings["zenex_services"][s]
                            changed = True
                            
                        if changed:
                            save_db()
                            
        except Exception as e:
            pass
        time.sleep(10)

# ==========================================
# Leaderboard + Daily Report System  (Bangladesh time, UTC+6)
# ==========================================
BD_TZ = timezone(timedelta(hours=6))
LB_PAGE = 10                                   # rows per page (admin full list)
DAILY_REPORT_HOUR, DAILY_REPORT_MIN = 23, 55   # night report time (BD time)
LB_TITLES = {"t": "TODAY", "w": "LAST 7 DAYS", "a": "ALL TIME"}


def _bd_now():
    return datetime.now(BD_TZ)


def _day_str(dt=None):
    return (dt or _bd_now()).strftime("%Y-%m-%d")


def _day_range_ts(day):
    s = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=BD_TZ).timestamp()
    return s, s + 86400


def _period_days(p):
    now = _bd_now()
    if p == "t":
        return _day_str(now), _day_str(now)
    if p == "y":
        y = _day_str(now - timedelta(days=1))
        return y, y
    if p == "w":
        return _day_str(now - timedelta(days=6)), _day_str(now)
    if p == "m":
        return _day_str(now.replace(day=1)), _day_str(now)
    return "0000-00-00", "9999-99-99"


def track_otp_daily(uid):
    """+1 OTP for this user on today's (BD) date. Called from bump_otp_count."""
    try:
        day = _day_str()
        db.collection('otp_daily').document(f"{day}_{uid}").set(
            {"day": day, "user_id": str(uid), "count": Increment(1)}, merge=True)
    except Exception:
        log.exception("track_otp_daily failed for %s", uid)


def track_withdraw_result(approved, amt):
    try:
        day = _day_str()
        k = "approved" if approved else "rejected"
        db.collection('daily_stats').document(day).set(
            {"day": day, f"{k}_count": Increment(1), f"{k}_amt": Increment(float(amt))}, merge=True)
    except Exception:
        log.exception("track_withdraw_result failed")


def backfill_otp_daily():
    """One time: rebuild past daily OTP counts from the 'otp' reward history."""
    if bot_settings.get("otp_daily_backfilled"):
        return
    try:
        rows = db.sql(
            "SELECT json_extract(data,'$.user_id'), date(json_extract(data,'$.ts') + 21600, 'unixepoch'), COUNT(*) "
            "FROM docs WHERE collection='transactions' AND json_extract(data,'$.kind')='otp' GROUP BY 1, 2")
        for uid, day, cnt in rows:
            if uid is None or not day:
                continue
            db.collection('otp_daily').document(f"{day}_{uid}").set(
                {"day": day, "user_id": str(uid), "count": int(cnt)}, merge=True)
        bot_settings["otp_daily_backfilled"] = True
        save_db()
        log.info("otp_daily backfilled from %s groups", len(rows))
    except Exception:
        log.exception("backfill_otp_daily failed")


# ---------- queries ----------
_OTP_WHERE = "collection='otp_daily' AND json_extract(data,'$.day')>=? AND json_extract(data,'$.day')<=?"


def _rows_between(a, b, limit, offset=0):
    return [(str(r[0]), int(r[1])) for r in db.sql(
        "SELECT json_extract(data,'$.user_id'), SUM(json_extract(data,'$.count')) AS c FROM docs "
        f"WHERE {_OTP_WHERE} GROUP BY 1 HAVING c>0 ORDER BY c DESC, 1 LIMIT ? OFFSET ?",
        (a, b, limit, offset))]


def lb_rows(period, limit, offset=0):
    """[(user_id, otp_count)] highest first. period: t / w / a"""
    if period == "a":
        out = []
        for u in db.collection('users').order_by('total_otps', direction="DESCENDING").limit(limit).offset(offset).stream():
            c = int((u.to_dict() or {}).get("total_otps", 0) or 0)
            if c > 0:
                out.append((str(u.id), c))
        return out
    a, b = _period_days(period)
    return _rows_between(a, b, limit, offset)


def lb_count(period):
    if period == "a":
        return db.sql("SELECT COUNT(*) FROM docs WHERE collection='users' AND json_extract(data,'$.total_otps')>0")[0][0]
    a, b = _period_days(period)
    return db.sql(f"SELECT COUNT(*) FROM (SELECT 1 FROM docs WHERE {_OTP_WHERE} "
                  "GROUP BY json_extract(data,'$.user_id') HAVING SUM(json_extract(data,'$.count'))>0)", (a, b))[0][0]


def lb_my(period, uid):
    """(rank or None, otp_count) of one user."""
    uid = str(uid)
    if period == "a":
        d = db.collection('users').document(uid).get().to_dict() or {}
        c = int(d.get("total_otps", 0) or 0)
        if c <= 0:
            return None, 0
        r = db.sql("SELECT COUNT(*) FROM docs WHERE collection='users' AND json_extract(data,'$.total_otps')>?", (c,))[0][0]
        return r + 1, c
    a, b = _period_days(period)
    c = int(db.sql(f"SELECT COALESCE(SUM(json_extract(data,'$.count')),0) FROM docs WHERE {_OTP_WHERE} "
                   "AND json_extract(data,'$.user_id')=?", (a, b, uid))[0][0] or 0)
    if c <= 0:
        return None, 0
    r = db.sql(f"SELECT COUNT(*) FROM (SELECT SUM(json_extract(data,'$.count')) AS s FROM docs WHERE {_OTP_WHERE} "
               "GROUP BY json_extract(data,'$.user_id') HAVING s>?)", (a, b, c))[0][0]
    return r + 1, c


def _sum_otp(a, b):
    r = db.sql(f"SELECT COALESCE(SUM(json_extract(data,'$.count')),0), "
               f"COUNT(DISTINCT json_extract(data,'$.user_id')) FROM docs WHERE {_OTP_WHERE}", (a, b))[0]
    return int(r[0] or 0), int(r[1] or 0)


def _lb_name(uid, link=False):
    try:
        d = db.collection('users').document(str(uid)).get().to_dict() or {}
    except Exception:
        d = {}
    nm = (f"{d.get('first_name', '')} {d.get('last_name', '')}".strip() or d.get("username") or f"User {str(uid)[-4:]}")[:16]
    nm = html.escape(nm)
    return f"<a href='tg://user?id={uid}'>{nm}</a>" if link else nm


def _rank_icon(n):
    return {1: "\U0001F947", 2: "\U0001F948", 3: "\U0001F949"}.get(n, f"{n}.")


# ---------- member leaderboard (top 10) ----------
def build_user_lb(chat_id, period="t"):
    if period not in LB_TITLES:
        period = "t"
    line = "\u2501" * 15
    lines = ""
    for i, (uid, c) in enumerate(lb_rows(period, 10), 1):
        me = "  \u2B05\uFE0F <b>You</b>" if str(uid) == str(chat_id) else ""
        lines += f"{_rank_icon(i)} {_lb_name(uid)} \u2794 <b>{c}</b> OTP{me}\n"
    if not lines:
        lines = "<i>No data yet.</i>\n"
    rank, cnt = lb_my(period, chat_id)
    mine = f"Your rank: <b>#{rank}</b> \u2022 <b>{cnt}</b> OTP" if rank else "Your rank: <i>no OTP yet</i>"
    txt = f"{line}\n\U0001F3C6 <b>LEADERBOARD \u2022 {LB_TITLES[period]}</b>\n{line}\n{lines}{line}\n{mine}"
    tabs = [{"text": ("\u2022 " if p == period else "") + LB_TITLES[p].title(), "callback_data": f"ulb_{p}",
             "style": "success" if p == period else "primary"} for p in ("t", "w", "a")]
    kb = {"inline_keyboard": [tabs, [{"text": "Close", "callback_data": "close_msg", "style": "danger"}]]}
    return txt, kb


# ---------- admin: full leaderboard (everyone, paginated) ----------
def build_admin_lb(period="a", page=0):
    if period not in LB_TITLES:
        period = "a"
    total = lb_count(period)
    pages = max(1, (total + LB_PAGE - 1) // LB_PAGE)
    page = max(0, min(int(page), pages - 1))
    lines = ""
    for i, (uid, c) in enumerate(lb_rows(period, LB_PAGE, page * LB_PAGE), page * LB_PAGE + 1):
        lines += f"{_rank_icon(i)} {_lb_name(uid, True)} (<code>{uid}</code>) \u2794 <b>{c}</b> OTP\n"
    if not lines:
        lines = "<i>No data yet.</i>\n"
    line = "\u2501" * 15
    txt = (f"{line}\n\U0001F3C6 <b>ALL LEADERBOARD \u2022 {LB_TITLES[period]}</b>\n{line}\n"
           f"Users with OTP: <b>{total}</b>\n<b>Page {page + 1}/{pages}</b>\n{line}\n{lines}{line}")
    tabs = [{"text": ("\u2022 " if p == period else "") + LB_TITLES[p].title(), "callback_data": f"alb_all_{p}_0",
             "style": "success" if p == period else "primary"} for p in ("t", "w", "a")]
    nav = []
    if page > 0:
        nav.append({"text": "Prev", "callback_data": f"alb_all_{period}_{page - 1}", "style": "primary"})
    if page < pages - 1:
        nav.append({"text": "Next", "callback_data": f"alb_all_{period}_{page + 1}", "style": "primary"})
    kb = {"inline_keyboard": [tabs] + ([nav] if nav else []) + [
        [{"text": "Back", "callback_data": "lb_main", "style": "danger"}]]}
    return txt, kb


# ---------- admin: total OTP received by the bot ----------
def build_total_otp():
    r = db.sql("SELECT COALESCE(SUM(json_extract(data,'$.total_otps')),0), "
               "COALESCE(SUM(CASE WHEN json_extract(data,'$.total_otps')>0 THEN 1 ELSE 0 END),0), COUNT(*) "
               "FROM docs WHERE collection='users'")[0]
    all_otp, with_otp, n_users = int(r[0] or 0), int(r[1] or 0), int(r[2] or 0)
    t, t_u = _sum_otp(*_period_days("t"))
    y, _ = _sum_otp(*_period_days("y"))
    w, w_u = _sum_otp(*_period_days("w"))
    m, _ = _sum_otp(*_period_days("m"))
    line = "\u2501" * 15
    txt = (f"{line}\n\U0001F4E9 <b>TOTAL OTP RECEIVED</b>\n{line}\n"
           f"\U0001F4C5 Today: <b>{t}</b> ({t_u} users)\n"
           f"\U0001F553 Yesterday: <b>{y}</b>\n"
           f"\U0001F4C6 Last 7 days: <b>{w}</b> ({w_u} users)\n"
           f"\U0001F5D3 This month: <b>{m}</b>\n"
           f"{line}\n\U0001F4CA <b>ALL TIME: {all_otp}</b>\n"
           f"\U0001F465 Users: {n_users} (got OTP: {with_otp})\n"
           f"\u23F1 Updated: {_bd_now().strftime('%Y-%m-%d %H:%M')} (BD)")
    kb = {"inline_keyboard": [[{"text": "Refresh", "callback_data": "alb_total", "style": "success"},
                               {"text": "Back", "callback_data": "lb_main", "style": "danger"}]]}
    return txt, kb


# ---------- admin: daily report ----------
def build_daily_report(day=None):
    day = day or _day_str()
    s, e = _day_range_ts(day)
    line = "\u2501" * 15
    new_users = db.sql("SELECT COUNT(*) FROM docs WHERE collection='users' AND json_extract(data,'$.joined_at')>=? "
                       "AND json_extract(data,'$.joined_at')<?", (s, e))[0][0]
    otp, active = _sum_otp(day, day)
    tx = {r[0]: (float(r[1] or 0), int(r[2] or 0)) for r in db.sql(
        "SELECT json_extract(data,'$.kind'), SUM(json_extract(data,'$.amount')), COUNT(*) FROM docs "
        "WHERE collection='transactions' AND json_extract(data,'$.ts')>=? AND json_extract(data,'$.ts')<? GROUP BY 1", (s, e))}
    net_change = sum(v[0] for v in tx.values())
    otp_pay, ref_pay = tx.get("otp", (0, 0)), tx.get("refer", (0, 0))
    other = sum(v[0] for k, v in tx.items() if k not in ("otp", "refer", "withdraw", "refund"))
    credited = otp_pay[0] + ref_pay[0]
    wd_req, refunded = tx.get("withdraw", (0, 0)), tx.get("refund", (0, 0))
    st = db.collection('daily_stats').document(day).get().to_dict() or {}
    ap_amt, ap_cnt = float(st.get("approved_amt", 0) or 0), int(st.get("approved_count", 0) or 0)
    rj_amt, rj_cnt = float(st.get("rejected_amt", 0) or 0), int(st.get("rejected_count", 0) or 0)
    with withdraw_lock:
        pend = list(pending_withdrawals.values())
    pend_amt = sum(float(p.get("amount", 0) or 0) for p in pend)
    top = ""
    for i, (uid, c) in enumerate(_rows_between(day, day, 5), 1):
        top += f"{_rank_icon(i)} {_lb_name(uid, True)} \u2794 <b>{c}</b>\n"
    all_otp = int(db.sql("SELECT COALESCE(SUM(json_extract(data,'$.total_otps')),0) FROM docs WHERE collection='users'")[0][0] or 0)
    txt = (
        f"{line}\n\U0001F4CB <b>DAILY REPORT</b> \u2022 {day}\n{line}\n"
        f"\U0001F465 <b>USERS</b>\n"
        f"\u2022 New users: <b>{new_users}</b>\n"
        f"\u2022 Total users: <b>{len(all_known_users)}</b>\n{line}\n"
        f"\U0001F4E9 <b>OTP</b>\n"
        f"\u2022 Today: <b>{otp}</b> ({active} users)\n"
        f"\u2022 All time: <b>{all_otp}</b>\n{line}\n"
        f"\U0001F4B8 <b>WITHDRAW</b>\n"
        f"\u2022 Requested: <b>{money(abs(wd_req[0]))} TK</b> ({wd_req[1]})\n"
        f"\u2022 Approved (paid): <b>{money(ap_amt)} TK</b> ({ap_cnt})\n"
        f"\u2022 Rejected: <b>{money(rj_amt)} TK</b> ({rj_cnt})\n"
        f"\u2022 Pending now: <b>{money(pend_amt)} TK</b> ({len(pend)})\n{line}\n"
        f"\U0001F4B0 <b>INCOME / EXPENSE</b>\n"
        f"\u2022 OTP reward given: {money(otp_pay[0])} TK ({otp_pay[1]})\n"
        f"\u2022 Referral reward given: {money(ref_pay[0])} TK ({ref_pay[1]})\n"
        f"\u2022 Admin/other adjust: {money(other)} TK\n"
        f"\u2796 <b>Total expense (user earnings): {money(credited + other)} TK</b>\n"
        f"\u2022 Cash paid out: <b>{money(ap_amt)} TK</b>\n"
        f"\u2022 Users' balance change: <b>{'+' if net_change >= 0 else '-'}{money(abs(net_change))} TK</b>\n{line}\n"
        f"\U0001F3C6 <b>TOP 5 TODAY</b>\n{top or '<i>No OTP today.</i>' + chr(10)}{line}"
    )
    return txt


def send_daily_report(day=None, to=None):
    txt = render_body_text(build_daily_report(day))
    targets = to or sorted(set(int(a) for a in bot_settings.get("admins", [])) | {OWNER_ID})
    kb = {"inline_keyboard": [[{"text": "Close", "callback_data": "close_msg", "style": "danger"}]]}
    for aid in targets:
        try:
            send_message(aid, txt, reply_markup=kb)
        except Exception:
            log.exception("daily report send failed for %s", aid)


def daily_report_loop():
    while True:
        try:
            now = _bd_now()
            today = _day_str(now)
            if (bot_settings.get("daily_report_on", True) and bot_settings.get("last_daily_report") != today
                    and (now.hour, now.minute) >= (DAILY_REPORT_HOUR, DAILY_REPORT_MIN)):
                bot_settings["last_daily_report"] = today
                save_db()
                send_daily_report(today)
        except Exception:
            log.exception("daily_report_loop error")
        time.sleep(30)


def lb_main_view():
    on = bot_settings.get("daily_report_on", True)
    txt = ("\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
           f"\u300A {PEM['admin']} <b>LEADER BOARD MENU</b> \u300B\n"
           "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
           "<i>Select a category to view the top performers or history.</i>\n"
           "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501")
    kb = [
        [{"text": "Top Referrers", "icon_custom_emoji_id": "5420145051336485498", "callback_data": "lb_top_refs", "style": "primary"},
         {"text": "Top OTP (All)", "icon_custom_emoji_id": "5353001161878182134", "callback_data": "lb_top_otps", "style": "primary"}],
        [{"text": "Today Top OTP", "callback_data": "alb_all_t_0", "style": "success"},
         {"text": "Weekly Top OTP", "callback_data": "alb_all_w_0", "style": "success"}],
        [{"text": "ALL Leaderboard (who got how many OTP)", "callback_data": "alb_all_a_0", "style": "primary"}],
        [{"text": "Total OTP of Bot", "callback_data": "alb_total", "style": "success"}],
        [{"text": "Daily Report (now)", "callback_data": "drpt_now", "style": "primary"},
         {"text": f"Auto Report: {'ON' if on else 'OFF'}", "callback_data": "drpt_toggle", "style": "success" if on else "danger"}],
        [{"text": "Withdrawal History", "icon_custom_emoji_id": "5348469219761626211", "callback_data": "lb_w_history", "style": "success"}],
        [{"text": "Back to Admin", "icon_custom_emoji_id": "5267490665117275176", "callback_data": "back_to_admin", "style": "danger"}],
    ]
    return render_body_text(txt), {"inline_keyboard": kb}



# ==========================================
# Low Stock Alert (admin)
# ==========================================
LOW_STOCK_REPEAT_SEC = 6 * 3600     # while stock stays low, remind every 6 hours


def stock_summary():
    """(total_available, {(service, country): count}) for the local number stock."""
    total, by = 0, {}
    for b in list(number_batches.values()):
        n = len(b.get("numbers", []))
        total += n
        key = (str(b.get("service", "?")), str(b.get("country", "?")))
        by[key] = by.get(key, 0) + n
    return total, by


def build_low_stock_text(alert=False):
    total, by = stock_summary()
    limit = int(bot_settings.get("low_stock_limit", 200) or 0)
    on = bot_settings.get("low_stock_on", True)
    line = "\u2501" * 15
    rows = "".join(f"\u2022 {html.escape(s)} {html.escape(c)}: <b>{n}</b>\n"
                   for (s, c), n in sorted(by.items(), key=lambda x: x[1]) if n > 0)
    head = ("\u26A0\uFE0F <b>LOW STOCK ALERT</b>" if total > 0 else "\U0001F6D1 <b>NUMBER STOCK EMPTY</b>") if alert \
        else "\U0001F4E6 <b>LOW STOCK ALERT SETTINGS</b>"
    status = "" if alert else f"Alert: <b>{'ON' if on else 'OFF'}</b>\n"
    return (f"{line}\n{head}\n{line}\n{status}"
            f"Available numbers: <b>{total}</b>\nAlert limit: <b>{limit}</b>\n{line}\n"
            f"{rows or '<i>No numbers in stock.</i>' + chr(10)}{line}")


def low_stock_keyboard():
    on = bot_settings.get("low_stock_on", True)
    return {"inline_keyboard": [
        [{"text": f"Alert: {'ON' if on else 'OFF'}", "callback_data": "ls_toggle", "style": "success" if on else "danger"},
         {"text": "Set Limit", "callback_data": "ls_set", "style": "primary"}],
        [{"text": "Refresh", "callback_data": "ls_menu", "style": "success"},
         {"text": "Back", "callback_data": "system_settings", "style": "danger"}]]}


def low_stock_loop():
    while True:
        try:
            if bot_settings.get("low_stock_on", True):
                total, _ = stock_summary()
                limit = int(bot_settings.get("low_stock_limit", 200) or 0)
                now = time.time()
                if total < limit:
                    if now - float(bot_settings.get("low_stock_last", 0) or 0) >= LOW_STOCK_REPEAT_SEC:
                        bot_settings["low_stock_last"] = now
                        save_db()
                        txt = render_body_text(build_low_stock_text(alert=True))
                        for aid in sorted(set(int(a) for a in bot_settings.get("admins", [])) | {OWNER_ID}):
                            try:
                                send_message(aid, txt)
                            except Exception:
                                log.exception("low stock alert send failed for %s", aid)
                elif bot_settings.get("low_stock_last"):
                    bot_settings["low_stock_last"] = 0     # stock is fine again -> next drop alerts at once
                    save_db()
        except Exception:
            log.exception("low_stock_loop error")
        time.sleep(60)


_err_seen = {}

def notify_owner_error(where, tb_text):
    lines_ = [x for x in tb_text.strip().splitlines() if x.strip()]
    sig = lines_[-1][:150] if lines_ else where
    now = time.time()
    if now - _err_seen.get(sig, 0) < 300:
        return
    _err_seen[sig] = now
    try:
        send_message(OWNER_ID, f"\u26A0\uFE0F <b>Bot error</b> in <code>{html.escape(where)}</code>\n<code>{html.escape(sig)}</code>")
    except Exception:
        pass

def _safe(fn, arg, where):
    try:
        fn(arg)
    except Exception:
        tb = traceback.format_exc()
        log.error("Unhandled error in %s:\n%s", where, tb)
        notify_owner_error(where, tb)

def main():
    global BOT_USERNAME
    api_call("deleteWebhook", {"drop_pending_updates": False})   # polling needs no webhook
    res = {}
    for _try in range(5):
        res = api_call("getMe")
        if res.get("ok"):
            break
        if res.get("error_code") == 401:
            raise SystemExit("BOT_TOKEN is invalid (401 Unauthorized). Check the BOT_TOKEN env variable.")
        time.sleep(3)
    if res.get("ok"): BOT_USERNAME = res["result"]["username"]
    print(f"🤖 Bot is starting... @{BOT_USERNAME}")
    
    backfill_otp_daily()
    threading.Thread(target=daily_report_loop, daemon=True).start()
    threading.Thread(target=low_stock_loop, daemon=True).start()
    threading.Thread(target=panel_monitor_thread, daemon=True).start()
    threading.Thread(target=global_sms_listener, daemon=True).start()
    threading.Thread(target=stex_console_listener, daemon=True).start()
    threading.Thread(target=voltx_sms_listener, daemon=True).start()
    threading.Thread(target=voltx_console_listener, daemon=True).start()
    threading.Thread(target=zenex_sms_listener, daemon=True).start()
    threading.Thread(target=zenex_console_listener, daemon=True).start()
    print("📡 Background APIs & Global SMS Listener Started!")
    
    # 🌟 PRO-LEVEL FAST SYSTEM: 500 Workers Pool
    executor = ThreadPoolExecutor(max_workers=500)
    
    offset = None
    fails = 0
    while True:
        try:
            payload = {"timeout": 50, "allowed_updates": ["message", "callback_query"]}
            if offset is not None:
                payload["offset"] = offset
            updates = tg_session.post(f"{BASE_URL}/getUpdates", json=payload, timeout=65).json()
            if not updates.get("ok"):
                code = updates.get("error_code")
                if code == 401:
                    log.error("BOT_TOKEN rejected by Telegram (401). Stopping.")
                    os._exit(1)
                if code == 409:
                    log.warning("409 Conflict: this bot is running somewhere else too. Keep only ONE instance.")
                else:
                    log.warning("getUpdates error: %s", str(updates)[:200])
                fails += 1
                time.sleep(min(30, 2 * fails))
                continue
            fails = 0
            for update in updates.get("result", []):
                offset = update["update_id"] + 1
                if "message" in update:
                    executor.submit(_safe, handle_message, update["message"], "handle_message")
                elif "callback_query" in update:
                    executor.submit(_safe, handle_callback, update["callback_query"], "handle_callback")
        except Exception as e:
            fails += 1
            log.warning("polling network error (retry %s): %s", fails, type(e).__name__)
            time.sleep(min(30, 2 * fails))

if __name__ == "__main__":
    main()    