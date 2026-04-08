import sqlite3
import json
import threading
from datetime import datetime
from astrbot.api import logger


class DBHandler:
    def __init__(self, db_path: str, enable_wal: bool = True):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_db(enable_wal)

    def _init_db(self, enable_wal: bool):
        with self._lock:
            with self.conn:
                # H-004: 增加配置控制，修复潜在的NFS兼容问题
                if enable_wal:
                    self.conn.execute("PRAGMA journal_mode=WAL")
                else:
                    self.conn.execute("PRAGMA journal_mode=DELETE")

                self.conn.execute("PRAGMA synchronous=NORMAL")
                self.conn.execute("PRAGMA busy_timeout=5000")

                self.conn.execute(
                    "CREATE TABLE IF NOT EXISTS keywords ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "keyword TEXT, scope TEXT, target_id TEXT, "
                    "content_json TEXT, creator TEXT, created_at TIMESTAMP, "
                    "UNIQUE(keyword, scope, target_id))"
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

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            logger.warning(f"关闭数据库连接失败: {e}")

    def count_scope_keywords(self, scope, target):
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM keywords "
                "WHERE scope=? AND target_id=?", (scope, str(target))
            ).fetchone()
            return row['cnt'] if row else 0

    def save_keyword(self, kw, scope, target, content, creator, hashes):
        t_str = str(target).strip()
        with self._lock:
            with self.conn:
                cursor = self.conn.execute(
                    "INSERT OR IGNORE INTO keywords "
                    "(keyword, scope, target_id, content_json, creator, created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (kw.lower(), scope, t_str, json.dumps(content, ensure_ascii=False), str(creator), datetime.now())
                )
                if cursor.rowcount == 0:
                    return False

                kw_id = cursor.lastrowid
                for h in set(hashes):
                    self.conn.execute(
                        "INSERT OR IGNORE INTO keyword_media (kw_id, media_hash) VALUES (?,?)",
                        (kw_id, h)
                    )
                    self.conn.execute(
                        "UPDATE media_files SET ref_count = ref_count + 1, in_waitlist = 0 WHERE hash = ?", (h,)
                    )
                return True

    def match_scope_keyword(self, kw, scope, target):
        with self._lock:
            t_str = str(target).strip()
            # M-001 修复：去除 CAST，利用索引加速查询
            row = self.conn.execute(
                "SELECT content_json FROM keywords "
                "WHERE keyword=? AND scope=? AND target_id=?",
                (kw.lower(), scope, t_str)
            ).fetchone()
            if not row:
                return None
            try:
                return json.loads(row['content_json'])
            except (json.JSONDecodeError, TypeError):
                logger.error(f"关键字 {kw} 的 content_json 解析失败(脏数据)")
                return None

    def delete_keyword(self, kw, scope, target) -> bool:
        t_str = str(target).strip()
        if not t_str or t_str == "None":
            return False
        with self._lock:
            with self.conn:
                # M-001 修复：去除 CAST
                row = self.conn.execute(
                    "SELECT id FROM keywords "
                    "WHERE keyword=? AND scope=? AND target_id=?",
                    (kw.lower(), scope, t_str)
                ).fetchone()
                if not row:
                    return False
                kid = row['id']
                hashes = [
                    r['media_hash'] for r in self.conn.execute(
                        "SELECT media_hash FROM keyword_media WHERE kw_id=?", (kid,)
                    ).fetchall()
                ]
                for h in hashes:
                    self.conn.execute("UPDATE media_files SET ref_count = ref_count - 1 WHERE hash=?", (h,))
                    self.conn.execute(
                        "UPDATE media_files SET in_waitlist=1, waitlist_time=? WHERE hash=? AND ref_count<=0",
                        (datetime.now(), h)
                    )
                self.conn.execute("DELETE FROM keywords WHERE id=?", (kid,))
                self.conn.execute("DELETE FROM keyword_media WHERE kw_id=?", (kid,))
                return True

    def get_total_count(self, scope, target):
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM keywords WHERE scope=? AND target_id=?", (scope, str(target))
            ).fetchone()
            return row['cnt'] if row else 0

    def get_global_total_count(self):
        with self._lock:
            row = self.conn.execute("SELECT COUNT(*) as cnt FROM keywords WHERE scope='global'").fetchone()
            return row['cnt'] if row else 0

    def list_keywords(self, scope, target, page):
        with self._lock:
            offset = (max(1, page) - 1) * 10
            return self.conn.execute(
                "SELECT keyword FROM keywords "
                "WHERE scope=? AND target_id=? ORDER BY id DESC LIMIT 10 OFFSET ?",
                (scope, str(target), offset)
            ).fetchall()

    def list_global_keywords(self, page):
        with self._lock:
            offset = (max(1, page) - 1) * 10
            return self.conn.execute(
                "SELECT keyword, creator FROM keywords "
                "WHERE scope='global' ORDER BY id DESC LIMIT 10 OFFSET ?",
                (offset,)
            ).fetchall()

    def get_media_path(self, file_hash: str):
        with self._lock:
            return self.conn.execute(
                "SELECT file_path FROM media_files WHERE hash=?", (file_hash,)
            ).fetchone()

    def save_media_record(self, file_hash: str, file_path: str):
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO media_files (hash, file_path) VALUES (?, ?)",
                (file_hash, file_path)
            )

    def cleanup_orphan_media(self, deadline):
        with self._lock:
            with self.conn:
                cursor = self.conn.execute(
                    "SELECT hash, file_path FROM media_files "
                    "WHERE ref_count <= 0 AND in_waitlist = 1 AND waitlist_time < ?", (deadline,)
                )
                to_delete = cursor.fetchall()
                for row in to_delete:
                    self.conn.execute("DELETE FROM media_files WHERE hash = ?", (row['hash'],))
                return [(row['hash'], row['file_path']) for row in to_delete]
