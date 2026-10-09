"""
ONE-TIME migration: copy users / withdrawals / settings from Firebase into bot_users.db.
Only needed if you want to keep existing user balances. Run it BEFORE you delete Firebase.

  pip install firebase-admin
  python migrate_firebase_to_sqlite.py serviceAccount.json
(serviceAccount.json = your Firebase service-account key file; delete it afterwards)
"""
import os, sys, datetime
import firebase_admin
from firebase_admin import credentials, firestore
from localdb import LocalDB

key = sys.argv[1] if len(sys.argv) > 1 else "serviceAccount.json"
firebase_admin.initialize_app(credentials.Certificate(key))
fs = firestore.client()
out = LocalDB(os.path.join(os.environ.get("DATA_DIR", "."), "bot_users.db"))

def clean(v):
    if isinstance(v, datetime.datetime): return v.timestamp()
    if isinstance(v, dict): return {k: clean(x) for k, x in v.items()}
    if isinstance(v, list): return [clean(x) for x in v]
    return v

for col in ("users", "withdrawals", "settings"):
    n = 0
    for doc in fs.collection(col).stream():
        out.collection(col).document(doc.id).set(clean(doc.to_dict()))
        n += 1
    print(f"{col}: {n} copied")
print("Done. Start the bot normally.")
