import os
import re
import time
import shutil
import hashlib
import asyncio
from astrbot.api import logger


class MediaService:
    def __init__(self, media_base: str, db_handler, timeout_small: int = 10, timeout_large: int = 60, download_wait_timeout: int = 60):
        self.media_base = media_base
        self.db = db_handler
        self.timeout_small = timeout_small
        self.timeout_large = timeout_large
        self.download_wait_timeout = download_wait_timeout

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
        """将 CQ 码字符串解析为 OB11 段列表 (增强转义兼容)"""
        if not cq_str:
            return []
        segments: list[dict] = []
        # 增加对 CQ 转义字符的基础支持
        cq_str = cq_str.replace("&#91;", "[").replace("&#93;", "]").replace("&#44;", ",").replace("&amp;", "&")
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

    @staticmethod
    def _escape_cq(text: str) -> str:
        """CQ 码特殊字符转义，防止注入/破坏结构"""
        if not isinstance(text, str):
            return ""
        return text.replace("&", "&amp;").replace("[", "&#91;").replace("]", "&#93;").replace(",", "&#44;")

    async def _wait_for_file_ready(self, filepath: str, fsize: int, timeout: int):
        """死等文件大小达到预期值，最多等 timeout 秒"""
        loop = asyncio.get_running_loop()
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                curr_size = await loop.run_in_executor(None, os.path.getsize, filepath)
                if curr_size == fsize and fsize > 0:
                    return True
            except OSError:
                pass
            await asyncio.sleep(0.5)
        return False

    async def save_media(self, bot, m_type, file_name=None, file_id=None, local_path=None, md5=None, original_name=None, max_size=0, expected_size=0):
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
                    api_timeout = self.timeout_small if m_type in ("image", "record") else self.timeout_large

                    if m_type == "file" and file_id:
                        res = await asyncio.wait_for(bot.api.call_action("get_file", file_id=file_id), timeout=api_timeout)
                    elif file_name and m_type in ("image", "record", "video"):
                        res = await asyncio.wait_for(bot.api.call_action("get_file", file=file_name), timeout=api_timeout)
                    else:
                        return None
                except asyncio.TimeoutError:
                    logger.warning(f"获取 {m_type} 文件超时(>{api_timeout}s)")
                    return None
                except Exception as e:
                    logger.error(f"获取 {m_type} 文件异常: {e}")
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

            # 如果拿到了预期大小，就死等它达到这个大小
            if expected_size > 0:
                if not await self._wait_for_file_ready(source_path, expected_size, timeout=self.download_wait_timeout):
                    logger.warning(f"{m_type} 文件下载超时(>{self.download_wait_timeout}s): {source_path}")
                    return {"error": "download_timeout"}

            if not md5:
                md5 = await loop.run_in_executor(None, self._calculate_md5, source_path)

            try:
                row = await loop.run_in_executor(None, self.db.get_media_path, md5)
                if row and row['file_path'] and await loop.run_in_executor(None, os.path.exists, row['file_path']):
                    return {"hash": md5, "path": row['file_path'], "name": original_name or ""}
            except Exception as e:
                logger.error(f"查询媒体库异常: {e}")

            ext_map = {"image": "png", "record": "amr", "video": "mp4", "file": "dat"}
            ext = ext_map.get(m_type, "dat")
            save_dir = os.path.join(self.media_base, m_type)
            save_path = os.path.abspath(os.path.join(save_dir, f"{md5}.{ext}"))

            def do_copy():
                os.makedirs(save_dir, exist_ok=True)
                # 检查是否是同一个文件
                if os.path.exists(save_path):
                    if os.path.samefile(source_path, save_path):
                        return  # 同一个文件，无需操作
                    os.remove(save_path)
                shutil.copy2(source_path, save_path)

            await loop.run_in_executor(None, do_copy)

            try:
                await loop.run_in_executor(None, self.db.save_media_record, md5, save_path)
            except Exception as e:
                logger.error(f"写入媒体库异常: {e}")

            return {"hash": md5, "path": save_path, "name": original_name or ""}
        except Exception as e:
            logger.error(f"save_media 未知异常: {e}")
            return None

    async def save_forwarded_media(self, bot, m_type: str, raw_data: dict, max_size: int = 0, bot_uid: str = None) -> dict | None:
        try:
            str_size = raw_data.get("file_size")
            expected_size = 0
            if str_size:
                try:
                    expected_size = int(str_size)
                    if max_size > 0 and expected_size > max_size:
                        return {"error": "too_large", "size": expected_size}
                except ValueError:
                    pass

            file_val = raw_data.get("file", "")
            url_val = raw_data.get("url", "")

            # 规范修复：拼接 CQ 码必须转义，防止特殊字符破坏结构
            safe_file = self._escape_cq(file_val)
            safe_url = self._escape_cq(url_val)
            cq_str = f"[CQ:{m_type},file={safe_file}"
            if safe_url:
                cq_str += f",url={safe_url}"
            cq_str += "]"

            timeout = self.timeout_small if m_type in ("image", "record") else self.timeout_large
            loop = asyncio.get_running_loop()

            # 验证 bot_uid 是否合法
            if not bot_uid or not bot_uid.isdigit():
                logger.error(f"save_forwarded_media 无效的 bot_uid: {bot_uid}")
                return None

            send_res = await asyncio.wait_for(
                bot.api.call_action("send_private_msg", user_id=bot_uid, message=cq_str),
                timeout=timeout
            )

            # >>> 增加判空保护 <<<
            if not send_res or not isinstance(send_res, dict):
                logger.warning(f"保存转发媒体 {m_type} 发送私聊失败，返回值无效")
                return None

            msg_id = send_res.get("message_id")
            if not msg_id:
                return None

            msg_detail = await asyncio.wait_for(bot.api.call_action("get_msg", message_id=msg_id), timeout=10)

            # >>> 增加判空保护 <<<
            if not msg_detail or not isinstance(msg_detail, dict):
                logger.warning(f"保存转发媒体 {m_type} 获取消息详情失败")
                return None

            real_file_id = None
            for seg in msg_detail.get("message", []):
                if seg.get("type") in ("image", "video", "record", "audio", "file"):
                    real_file_id = seg.get("data", {}).get("file")
                    break
            if not real_file_id:
                return None

            file_info = await asyncio.wait_for(bot.api.call_action("get_file", file=real_file_id), timeout=10)

            # >>> 增加判空保护 <<<
            if not file_info or not isinstance(file_info, dict):
                logger.warning(f"保存转发媒体 {m_type} 获取文件信息失败")
                return None

            local_path = file_info.get("file")
            if not local_path or not await loop.run_in_executor(None, os.path.exists, local_path):
                return None

            if max_size > 0:
                try:
                    real_size = await loop.run_in_executor(None, os.path.getsize, local_path)
                    if real_size > max_size:
                        return {"error": "too_large", "size": real_size}
                except OSError:
                    pass

            # 如果协议没给大小，尝试读一次当前大小作为基准
            if expected_size == 0:
                try:
                    expected_size = await loop.run_in_executor(None, os.path.getsize, local_path)
                except OSError:
                    pass

            # 死等文件大小达到预期值
            if expected_size > 0:
                if not await self._wait_for_file_ready(local_path, expected_size, timeout=self.download_wait_timeout):
                    logger.warning(f"转发 {m_type} 文件下载超时(>{self.download_wait_timeout}s): {local_path}")
                    return {"error": "download_timeout"}

            md5 = raw_data.get("md5") or raw_data.get("md5HexStr")
            if not md5:
                md5 = await loop.run_in_executor(None, self._calculate_md5, local_path)

            try:
                row = await loop.run_in_executor(None, self.db.get_media_path, md5)
                if row and row['file_path'] and await loop.run_in_executor(None, os.path.exists, row['file_path']):
                    return {"hash": md5, "path": row['file_path'], "name": ""}
            except Exception as e:
                logger.error(f"转发媒体查库异常: {e}")

            ext_map = {"image": "png", "record": "amr", "video": "mp4"}
            ext = ext_map.get(m_type, "dat")
            save_dir = os.path.join(self.media_base, m_type)
            save_path = os.path.abspath(os.path.join(save_dir, f"{md5}.{ext}"))

            def do_copy():
                os.makedirs(save_dir, exist_ok=True)
                # 运行时兜底：防手欠删目录
                if os.path.exists(save_path):
                    os.remove(save_path)
                shutil.copy2(local_path, save_path)

            await loop.run_in_executor(None, do_copy)

            try:
                await loop.run_in_executor(None, self.db.save_media_record, md5, save_path)
            except Exception as e:
                logger.error(f"转发媒体入库异常: {e}")

            return {"hash": md5, "path": save_path, "name": ""}
        except asyncio.TimeoutError:
            logger.warning(f"保存转发媒体 {m_type} 网络超时")
            return None
        except Exception as e:
            logger.error(f"save_forwarded_media 未知异常: {e}")
            return None

    async def fetch_reply_content(self, bot, message_id, session_hashes, max_size=0):
        try:
            # 规范修复：必须加超时控制，防止异常网络挂起
            res = await asyncio.wait_for(bot.api.call_action("get_msg", message_id=message_id), timeout=15)
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
                        exp_sz = 0
                        sz_str = sd.get("file_size")
                        if sz_str:
                            try:
                                exp_sz = int(sz_str)
                            except ValueError:
                                pass
                        r = await self.save_media(bot, st, file_name=fn, md5=sd.get("md5") or sd.get("md5HexStr"), max_size=max_size, expected_size=exp_sz)
                        if r and "error" not in r:
                            session_hashes.append(r["hash"])
                            parsed.append({"type": st, "file": r["path"]})
                elif st == "file":
                    fid = sd.get("file_id")
                    orig = sd.get("file") or ""
                    if fid:
                        exp_sz = 0
                        sz_str = sd.get("file_size")
                        if sz_str:
                            try:
                                exp_sz = int(sz_str)
                            except ValueError:
                                pass
                        r = await self.save_media(bot, "file", file_id=fid, original_name=orig, max_size=max_size, expected_size=exp_sz)
                        if r and "error" not in r:
                            session_hashes.append(r["hash"])
                            parsed.append({"type": "file", "file": r["path"], "name": r.get("name", "")})
            return parsed if parsed else None
        except asyncio.TimeoutError:
            logger.warning("获取引用消息超时")
            return None
        except Exception as e:
            logger.error(f"fetch_reply_content 异常: {e}")
            return None
