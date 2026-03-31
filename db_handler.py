import sqlite3
import json
from datetime import datetime


class DBHandler:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS keywords ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "keyword TEXT, scope TEXT, target_id TEXT, "
                "content_json TEXT, creator TEXT, created_at TIMESTAMP)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS media_files ("
                "hash TEXT PRIMARY KEY, file_path TEXT, "
                "ref_count INTEGER DEFAULT 0, "
                "in_waitlist INTEGER DEFAULT 0, waitlist_time TIMESTAMP)"
            )
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS keyword_media ("
                "kw_id INTEGER, media_hash TEXT, "
                "PRIMARY KEY(kw_id, media_hash))"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_kw_match "
                "ON keywords (keyword, scope, target_id)"
            )

    def count_scope_keywords(self, scope, target):
        """统计某作用域下已有的关键字数量"""
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM keywords "
            "WHERE scope=? AND target_id=?",
            (scope, str(target))
        ).fetchone()
        return row['cnt'] if row else 0

    def save_keyword(self, kw, scope, target, content, creator, hashes):
        """保存关键字。同名已存在时返回 False，成功返回 True。"""
        t_str = str(target).strip()
        with self.conn:
            existing = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE keyword=? AND scope=? AND target_id=?",
                (kw.lower(), scope, t_str)
            ).fetchone()
            if existing:
                return False

            cursor = self.conn.execute(
                "INSERT INTO keywords "
                "(keyword, scope, target_id, content_json, creator, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (kw.lower(), scope, t_str,
                 json.dumps(content, ensure_ascii=False),
                 str(creator), datetime.now()))
            kw_id = cursor.lastrowid

            for h in set(hashes):
                self.conn.execute(
                    "INSERT OR IGNORE INTO keyword_media "
                    "(kw_id, media_hash) VALUES (?,?)", (kw_id, h))
                self.conn.execute(
                    "UPDATE media_files "
                    "SET ref_count = ref_count + 1, in_waitlist = 0 "
                    "WHERE hash = ?", (h,))
            return True

    def match_scope_keyword(self, kw, scope, target):
        t_str = str(target).strip()
        cursor = self.conn.execute(
            "SELECT content_json FROM keywords "
            "WHERE keyword=? AND scope=? "
            "AND CAST(target_id AS TEXT) = CAST(? AS TEXT)",
            (kw.lower(), scope, t_str))
        row = cursor.fetchone()
        return json.loads(row['content_json']) if row else None

    def delete_keyword(self, kw, scope, target) -> bool:
        t_str = str(target).strip()
        if not t_str or t_str == "None":
            return False
        with self.conn:
            row = self.conn.execute(
                "SELECT id FROM keywords "
                "WHERE keyword=? AND scope=? "
                "AND CAST(target_id AS TEXT) = CAST(? AS TEXT)",
                (kw.lower(), scope, t_str)).fetchone()
            if not row:
                return False
            kid = row['id']
            hashes = [
                r['media_hash'] for r in self.conn.execute(
                    "SELECT media_hash FROM keyword_media WHERE kw_id=?",
                    (kid,)).fetchall()
            ]
            for h in hashes:
                self.conn.execute(
                    "UPDATE media_files SET ref_count = ref_count - 1 "
                    "WHERE hash=?", (h,))
                self.conn.execute(
                    "UPDATE media_files SET in_waitlist=1, waitlist_time=? "
                    "WHERE hash=? AND ref_count<=0",
                    (datetime.now(), h))
            self.conn.execute("DELETE FROM keywords WHERE id=?", (kid,))
            self.conn.execute(
                "DELETE FROM keyword_media WHERE kw_id=?", (kid,))
            return True

    def get_total_count(self, scope, target):
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM keywords "
            "WHERE scope=? AND target_id=?",
            (scope, str(target))).fetchone()
        return row['cnt']

    def get_global_total_count(self):
        row = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM keywords WHERE scope='global'"
        ).fetchone()
        return row['cnt']

    def list_keywords(self, scope, target, page):
        offset = (max(1, page) - 1) * 10
        return self.conn.execute(
            "SELECT keyword FROM keywords "
            "WHERE scope=? AND target_id=? LIMIT 10 OFFSET ?",
            (scope, str(target), offset)).fetchall()

    def list_global_keywords(self, page):
        offset = (max(1, page) - 1) * 10
        return self.conn.execute(
            "SELECT keyword, creator FROM keywords "
            "WHERE scope='global' LIMIT 10 OFFSET ?",
            (offset,)).fetchall()
