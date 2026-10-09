DXF OTP BOT v2 (Firebase ছাড়া, SQLite)

চালানো:
  1) pip install -r requirements.txt
  2) export BOT_TOKEN="নতুন_টোকেন"   (অথবা token.txt ফাইলে টোকেন লিখুন)
     export OWNER_ID="আপনার_Telegram_numeric_ID"
  3) python number.py

ডাটা (DATA_DIR env না দিলে বটের ফোল্ডারে):
  bot_users.db      -> ইউজার, ব্যালেন্স, ট্রানজেকশন হিস্ট্রি, withdrawal
  bot_data.json     -> সেটিংস, নাম্বার স্টক (+ .bak কপি)
  backups/          -> প্রতি ২৪ ঘণ্টায় অটো ব্যাকআপ (শেষ ৭টা)
  bot_errors.log    -> সব এরর (মালিককেও টেলিগ্রামে জানায়)

অ্যাডমিন: Admin Panel > System > User Management
  All Balances (সবার ব্যালেন্স + মোট), Export CSV,
  User Profile / History (ID বা @username দিয়ে সার্চ)

হোস্টিং-এ ডিস্ক পার্মানেন্ট হতে হবে (ফ্রি প্ল্যানে রিস্টার্টে ফাইল মুছে যায়),
না হলে persistent volume মাউন্ট করে DATA_DIR=/data দিন।


Render (Background Worker): Build = pip install -r requirements.txt, Start = python number.py
Env: BOT_TOKEN, OWNER_ID, DATA_DIR=/data (Disk mount path /data)
