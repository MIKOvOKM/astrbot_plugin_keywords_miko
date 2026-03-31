import os
import re
import shutil
import hashlib
import asyncio


class MediaService:
    def __init__(self, media_base: str, db_handler, timeout_small: int = 10, timeout_large: int = 60):
        self.media_base = media_base
        self.db = db_handler
        self.timeout_small = timeout_small
        self.timeout_large = timeout_large

    @staticmethod
    def get_clean_text(message_chain: list) -> str:
        from astrbot.api.message_components import Plain
        if not message_chain:
            return ""
        raw = "".join([c.text for c in message_chain if isinstance(c, Plain)])
        return raw.replace("\u200b", "").replace("\n", "").replace("\r", "").strip()

    @staticmethod
    def _calculate_md5(filepath):
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def _parse_cq_code(cq_str: str) -> list[dict]:
        """将 CQ 码字符串解析为 OB11 段列表"""
        if not cq_str:
            return []
        segments: list[dict] = []
        pattern = re.compile(r"\[CQ:(\w+)((?:,[^\]]*)?)\]")
        last_end = 0
        for match in pattern.finditer(cq_str):
            if match.start() > last_end:
                plain = cq_str[last_end:match.start()]
                if plain:
                    segments.append({"type": "text", "data": {"text": plain}})
            cq_type = match.group(1)
            params_str = match.group(2)
            params: dict = {}
            if params_str:
                for part in params_str.split(","):
                    if "=" in part:
                        k, v = part.split("=", 1)
                        params[k.strip()] = v.strip()
            segments.append({"type": cq_type, "data": params})
            last_end = match.end()
        if last_end < len(cq_str):
            remaining = cq_str[last_end:]
            if remaining:
                segments.append({"type": "text", "data": {"text": remaining}})
        return segments

    async def save_media(self, bot, m_type, file_name=None, file_id=None, local_path=None, md5=None, original_name=None,
                         max_size=0):
        """普通消息媒体保存逻辑（原封不动）"""
        try:
            loop = asyncio.get_running_loop()
            source_path = None

            if local_path:
                try:
                    exists = await loop.run_in_executor(None, os.path.exists, local_path)
                    if exists:
                        fsize = await loop.run_in_executor(None, os.path.getsize, local_path)
                        if fsize > 0:
                            source_path = local_path
                except OSError:
                    pass

            if not source_path:
                try:
                    # 【关键修复】为普通消息的 get_file 加上超时熔断，防止 NapCat 卡死拖垮主线程
                    if m_type == "file" and file_id:
                        res = await asyncio.wait_for(bot.api.call_action("get_file", file_id=file_id), timeout=15)
                    elif file_name and m_type in ("image", "record", "video"):
                        res = await asyncio.wait_for(bot.api.call_action("get_file", file=file_name), timeout=15)
                    else:
                        return None
                except asyncio.TimeoutError:
                    return None
                except Exception:
                    return None

                if isinstance(res, dict):
                    candidate = res.get("file")
                    if candidate and await loop.run_in_executor(None, os.path.exists, candidate):
                        source_path = candidate

            if not source_path:
                return None

            if max_size > 0:
                try:
                    fsize = await loop.run_in_executor(None, os.path.getsize, source_path)
                    if fsize > max_size:
                        return {"error": "too_large", "size": fsize}
                except OSError:
                    pass

            if not md5:
                md5 = await loop.run_in_executor(None, self._calculate_md5, source_path)
            try:
                # 包裹进线程池
                def db_check():
                    return self.db.conn.execute(
                        "SELECT file_path FROM media_files WHERE hash=?", (md5,)
                    ).fetchone()

                row = await loop.run_in_executor(None, db_check)
                if row and row['file_path'] and await loop.run_in_executor(None, os.path.exists, row['file_path']):
                    return {"hash": md5, "path": row['file_path'], "name": original_name or ""}
            except Exception:
                pass

            ext_map = {"image": "png", "record": "amr", "video": "mp4", "file": "dat"}
            ext = ext_map.get(m_type, "dat")
            save_dir = os.path.join(self.media_base, m_type)
            save_path = os.path.abspath(os.path.join(save_dir, f"{md5}.{ext}"))

            os.makedirs(save_dir, exist_ok=True)
            if await loop.run_in_executor(None, os.path.exists, save_path):
                await loop.run_in_executor(None, os.remove, save_path)
            await loop.run_in_executor(None, shutil.copy2, source_path, save_path)

            try:
                with self.db.conn:
                    self.db.conn.execute(
                        "INSERT OR IGNORE INTO media_files (hash, file_path) VALUES (?, ?)",
                        (md5, save_path))
            except Exception:
                pass

            return {"hash": md5, "path": save_path, "name": original_name or ""}
        except Exception:
            return None

    async def save_forwarded_media(self, bot, m_type: str, raw_data: dict, max_size: int = 0,
                                   bot_uid: str = None) -> dict | None:
        """
        合并转发专用媒体保存逻辑（严格三部曲，彻底解耦）
        """
        try:
            str_size = raw_data.get("file_size")
            if str_size and max_size > 0:
                try:
                    if int(str_size) > max_size:
                        return {"error": "too_large", "size": int(str_size)}
                except ValueError:
                    pass

            file_val = raw_data.get("file", "")
            url_val = raw_data.get("url", "")
            cq_str = f"[CQ:{m_type},file={file_val}"
            if url_val:
                cq_str += f",url={url_val}"
            cq_str += "]"

            timeout = self.timeout_small if m_type in ("image", "record") else self.timeout_large
            loop = asyncio.get_running_loop()

            # 安全审核+业务调整：直接使用外部传入的单例 bot_uid，删除了内部的 get_login_info
            if not bot_uid:
                return None

            send_res = await asyncio.wait_for(
                bot.api.call_action("send_private_msg", user_id=bot_uid, message=cq_str),
                timeout=timeout
            )
            msg_id = send_res.get("message_id")
            if not msg_id:
                return None

            msg_detail = await asyncio.wait_for(
                bot.api.call_action("get_msg", message_id=msg_id), timeout=10
            )
            real_file_id = None
            for seg in msg_detail.get("message", []):
                if seg.get("type") in ("image", "video", "record", "audio", "file"):
                    real_file_id = seg.get("data", {}).get("file")
                    break
            if not real_file_id:
                return None

            file_info = await asyncio.wait_for(
                bot.api.call_action("get_file", file=real_file_id), timeout=10
            )
            local_path = file_info.get("file")
            if not local_path or not os.path.exists(local_path):
                return None

            # 业务调整：默认信任 NapCat 给的 MD5，没给才本地算
            md5 = raw_data.get("md5") or raw_data.get("md5HexStr")
            if not md5:
                md5 = await loop.run_in_executor(None, self._calculate_md5, local_path)

            try:
                def db_check():
                    return self.db.conn.execute(
                        "SELECT file_path FROM media_files WHERE hash=?", (md5,)
                    ).fetchone()

                row = await loop.run_in_executor(None, db_check)
                if row and row['file_path'] and await loop.run_in_executor(None, os.path.exists, row['file_path']):
                    return {"hash": md5, "path": row['file_path'], "name": ""}
            except Exception:
                pass

            ext_map = {"image": "png", "record": "amr", "video": "mp4"}
            ext = ext_map.get(m_type, "dat")
            save_dir = os.path.join(self.media_base, m_type)
            save_path = os.path.abspath(os.path.join(save_dir, f"{md5}.{ext}"))

            os.makedirs(save_dir, exist_ok=True)
            if await loop.run_in_executor(None, os.path.exists, save_path):
                await loop.run_in_executor(None, os.remove, save_path)
            await loop.run_in_executor(None, shutil.copy2, local_path, save_path)

            try:
                with self.db.conn:
                    self.db.conn.execute(
                        "INSERT OR IGNORE INTO media_files (hash, file_path) VALUES (?, ?)",
                        (md5, save_path))
            except Exception:
                pass

            return {"hash": md5, "path": save_path, "name": ""}

        except asyncio.TimeoutError:
            return None
        except Exception:
            return None

    async def fetch_reply_content(self, bot, message_id, session_hashes):
        """获取被引用消息的内容，用于保存引用结构。失败返回 None。"""
        try:
            res = await bot.api.call_action("get_msg", message_id=message_id)
            if not res or "message" not in res:
                return None
            message_data = res["message"]
            if isinstance(message_data, str):
                message_data = self._parse_cq_code(message_data)
            if not isinstance(message_data, list):
                return None

            parsed = []
            for seg in message_data:
                if not isinstance(seg, dict):
                    continue
                st = seg.get("type")
                sd = seg.get("data") or {}
                if not isinstance(sd, dict):
                    sd = {}
                if st == "text":
                    text = sd.get("text", "").replace("\u200b", "")
                    if text.strip():
                        parsed.append({"type": "text", "data": text})
                elif st == "at":
                    qq = sd.get("qq", "")
                    if qq:
                        parsed.append({"type": "text", "data": f"@{qq} "})
                elif st == "face":
                    fid = sd.get("id")
                    if fid is not None:
                        parsed.append({"type": "face", "id": str(fid)})
                elif st in ("image", "record", "video"):
                    fn = sd.get("file") or sd.get("id")
                    if fn:
                        # 引用消息里的媒体走普通逻辑
                        r = await self.save_media(
                            bot, st, file_name=fn, md5=sd.get("md5") or sd.get("md5HexStr"))
                        if r and "error" not in r:
                            session_hashes.append(r["hash"])
                            parsed.append({"type": st, "file": r["path"]})
                elif st == "file":
                    fid = sd.get("file_id")
                    orig = sd.get("file") or ""
                    if fid:
                        r = await self.save_media(bot, "file", file_id=fid, original_name=orig)
                        if r and "error" not in r:
                            session_hashes.append(r["hash"])
                            parsed.append({"type": "file", "file": r["path"], "name": r.get("name", "")})
            return parsed if parsed else None
        except Exception:
            return None
