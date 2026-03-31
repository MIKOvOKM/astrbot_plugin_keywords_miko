import sqlite3
import json
import threading
from datetime import datetime


class DBHandler:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # 安全审核：引入线程锁，强制跨线程/线程池的串行访问，解决并发一致性风险
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._lock:
            with self.conn:
                self.conn.execute("PRAGMA journal_mode=WAL")
                self.conn.execute("PRAGMA synchronous=NORMAL")
                self.conn.execute("PRAGMA busy_timeout=5000")
                # 安全审核：增加 UNIQUE 约束，从数据库层面杜绝并发竞态导致的重复关键字
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
        """显式释放连接资源"""
        try:
            self.conn.close()
        except Exception:
            pass

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
                # INSERT OR IGNORE 配合 UNIQUE 约束，天然防并发重复，无需先查后插
                cursor = self.conn.execute(
                    "INSERT OR IGNORE INTO keywords "
                    "(keyword, scope, target_id, content_json, creator, created_at) "
                    "VALUES (?,?,?,?,?,?)",
                    (kw.lower(), scope, t_str, json.dumps(content, ensure_ascii=False), str(creator), datetime.now())
                )
                if cursor.rowcount == 0:
                    return False  # 已存在被忽略

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
            row = self.conn.execute(
                "SELECT content_json FROM keywords "
                "WHERE keyword=? AND scope=? AND CAST(target_id AS TEXT) = CAST(? AS TEXT)",
                (kw.lower(), scope, t_str)
            ).fetchone()
            if not row:
                return None
            # 异常隔离：防止脏数据导致 json.loads 崩溃拖垮主链路
            try:
                return json.loads(row['content_json'])
            except (json.JSONDecodeError, TypeError):
                from astrbot.api import logger
                logger.error(f"关键字 {kw} 的 content_json 解析失败(脏数据)")
                return None

    def delete_keyword(self, kw, scope, target) -> bool:
        t_str = str(target).strip()
        if not t_str or t_str == "None":
            return False
        with self._lock:
            with self.conn:
                row = self.conn.execute(
                    "SELECT id FROM keywords "
                    "WHERE keyword=? AND scope=? AND CAST(target_id AS TEXT) = CAST(? AS TEXT)",
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
            # 规范修复：分页必须指定 ORDER BY，否则结果顺序随机导致翻页重复/遗漏
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
