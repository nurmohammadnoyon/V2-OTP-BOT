"""
localdb.py - Firestore-style API on top of SQLite.

number.py uses db.collection(..).document(..).get/set/update, order_by/limit/stream,
Increment and SERVER_TIMESTAMP. This module provides the same calls backed by one
local SQLite file, so no Firebase / internet database is needed.

Safe for the bot's many threads: every write runs inside one global lock + one
SQLite transaction, so balance updates (Increment) are atomic.
"""
import json
import os
import shutil
import sqlite3
import threading
import time


class Increment:
    def __init__(self, n):
        self.n = n


class _ServerTimestamp:
    pass


SERVER_TIMESTAMP = _ServerTimestamp()


class NotFound(Exception):
    pass


class DocSnapshot:
    def __init__(self, doc_id, data):
        self.id = doc_id
        self._data = data

    @property
    def exists(self):
        return self._data is not None

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


def _apply(old, new):
    """Merge `new` into `old`, resolving Increment / SERVER_TIMESTAMP markers."""
    out = dict(old)
    for k, v in new.items():
        if isinstance(v, Increment):
            out[k] = round((out.get(k) or 0) + v.n, 6)
        elif v is SERVER_TIMESTAMP:
            out[k] = time.time()
        else:
            out[k] = v
    return out


class DocRef:
    def __init__(self, store, collection, doc_id):
        self._s = store
        self._c = collection
        self.id = doc_id

    def get(self):
        with self._s.lock:
            row = self._s.conn.execute(
                "SELECT data FROM docs WHERE collection=? AND id=?", (self._c, self.id)
            ).fetchone()
        return DocSnapshot(self.id, json.loads(row[0]) if row else None)

    def set(self, data, merge=False):
        with self._s.lock, self._s.conn:
            row = self._s.conn.execute(
                "SELECT data FROM docs WHERE collection=? AND id=?", (self._c, self.id)
            ).fetchone()
            base = json.loads(row[0]) if (row and merge) else {}
            new = _apply(base, data)
            self._s.conn.execute(
                "INSERT OR REPLACE INTO docs(collection,id,data) VALUES(?,?,?)",
                (self._c, self.id, json.dumps(new)),
            )

    def update(self, data):
        with self._s.lock, self._s.conn:
            row = self._s.conn.execute(
                "SELECT data FROM docs WHERE collection=? AND id=?", (self._c, self.id)
            ).fetchone()
            if not row:
                raise NotFound(f"{self._c}/{self.id} not found")
            new = _apply(json.loads(row[0]), data)
            self._s.conn.execute(
                "UPDATE docs SET data=? WHERE collection=? AND id=?",
                (json.dumps(new), self._c, self.id),
            )


class Query:
    def __init__(self, store, collection, order=None, lim=None, wh=None, off=0):
        self._s = store
        self._c = collection
        self._order = order
        self._lim = lim
        self._wh = wh or []
        self._off = off

    def _clone(self, **kw):
        d = dict(order=self._order, lim=self._lim, wh=list(self._wh), off=self._off)
        d.update(kw)
        return Query(self._s, self._c, **d)

    def order_by(self, field, direction="ASCENDING"):
        return self._clone(order=(field, str(direction).upper().startswith("DESC")))

    def limit(self, n):
        return self._clone(lim=n)

    def offset(self, n):
        return self._clone(off=n)

    def where(self, field, op, value):
        if op != "==":
            raise ValueError("only == is supported")
        return self._clone(wh=self._wh + [(field, value)])

    def select(self, fields):
        return self  # fields are ignored; callers only need the ids

    @staticmethod
    def _check(field):
        if not field.replace("_", "").isalnum():
            raise ValueError("bad field name")

    def count(self):
        sql, args = self._build(count=True)
        with self._s.lock:
            return self._s.conn.execute(sql, args).fetchone()[0]

    def _build(self, count=False):
        sql = "SELECT COUNT(*) FROM docs WHERE collection=?" if count else "SELECT id, data FROM docs WHERE collection=?"
        args = [self._c]
        for field, value in self._wh:
            self._check(field)
            sql += f" AND json_extract(data,'$.{field}') = ?"
            args.append(value)
        if count:
            return sql, args
        if self._order:
            field, desc = self._order
            self._check(field)
            sql += f" AND json_extract(data,'$.{field}') IS NOT NULL"
            sql += f" ORDER BY json_extract(data,'$.{field}') {'DESC' if desc else 'ASC'}, id"
        if self._lim:
            sql += " LIMIT ?"
            args.append(int(self._lim))
            if self._off:
                sql += " OFFSET ?"
                args.append(int(self._off))
        return sql, args

    def stream(self):
        sql, args = self._build()
        with self._s.lock:
            rows = self._s.conn.execute(sql, args).fetchall()
        return [DocSnapshot(r[0], json.loads(r[1])) for r in rows]


class Collection(Query):
    def document(self, doc_id):
        return DocRef(self._s, self._c, str(doc_id))


class LocalDB:
    def __init__(self, path="bot_users.db", backup_dir=None, keep_backups=7):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS docs("
            "collection TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, "
            "PRIMARY KEY(collection,id))"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_docs_user ON docs(collection, json_extract(data,'$.user_id'))"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_docs_username ON docs(collection, json_extract(data,'$.username_lower'))"
        )
        self.conn.commit()
        self.backup_dir = backup_dir or os.path.join(d, "backups")
        self.keep_backups = keep_backups
        threading.Thread(target=self._backup_loop, daemon=True).start()

    def collection(self, name):
        return Collection(self, name)

    def sql(self, query, args=()):
        """Read-only helper for reports, e.g. SUM(json_extract(data,'$.balance'))."""
        with self.lock:
            return self.conn.execute(query, args).fetchall()

    # ---- backups: one consistent copy per day, newest N kept ----
    def backup_now(self):
        os.makedirs(self.backup_dir, exist_ok=True)
        dest = os.path.join(self.backup_dir, time.strftime("bot_users_%Y%m%d_%H%M%S.db"))
        with self.lock:
            out = sqlite3.connect(dest)
            try:
                self.conn.backup(out)
            finally:
                out.close()
        files = sorted(f for f in os.listdir(self.backup_dir) if f.startswith("bot_users_"))
        for old in files[:-self.keep_backups]:
            try:
                os.remove(os.path.join(self.backup_dir, old))
            except OSError:
                pass
        return dest

    def _backup_loop(self):
        while True:
            time.sleep(24 * 3600)
            try:
                self.backup_now()
            except Exception as e:
                print(f"Backup failed: {e}")
