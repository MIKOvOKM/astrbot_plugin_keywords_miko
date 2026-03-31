import os
import time
import asyncio
import traceback
from datetime import datetime, timedelta

from astrbot.api import logger
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api.message_components import (
    Plain, At, Image, Record, Video, File, Face, Reply
)
from astrbot.api.all import MessageChain

from .db_handler import DBHandler
from .media_service import MediaService

MEDIA_DOWNLOAD_TYPES = frozenset({"image", "record", "video", "file"})


@register(
    "astrbot_plugin_keywords_miko",
    "MIKOvOKM",
    "关键字回复系统",
    "1.0.1",
)
class KeywordPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.admin_users: set = set(str(u) for u in self.config.get("admin_users", []))

        self.plugin_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = self.config.get("database_path", "").strip()
        if not db_path:
            db_path = os.path.join(self.plugin_dir, "db", "sqlite.db")

        media_path = self.config.get("media_storage_path", "").strip()
        if not media_path:
            media_path = os.path.join(self.plugin_dir, "data", "media")

        # >>> 启动预检：一次性创建所有必需的目录（不再在运行时反复检查） <<<
        try:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            # 把以后可能用到的所有媒体子目录也一次性建好
            for sub_dir in ["image", "video", "record", "file"]:
                os.makedirs(os.path.join(media_path, sub_dir), exist_ok=True)
            logger.info(f"[关键字回复系统] 数据目录预检完成: {os.path.dirname(db_path)}, {media_path}")
        except Exception as e:
            logger.error(f"[关键字回复系统] !!! 致命错误 !!! 无法创建数据目录，请检查 Docker 挂载权限！详情: {e}")

        self.db_h = DBHandler(db_path)
        timeout_small = self.config.get("media_timeout_small", 10)
        timeout_large = self.config.get("media_timeout_large", 60)
        self.media_s = MediaService(media_path, self.db_h, timeout_small, timeout_large)

        self.adding_lock_user: str | None = None
        self.sessions: dict = {}
        self._maintenance_handle = None

        self.max_nested_depth: int = self.config.get("max_nested_depth", 3)
        self.max_forward_count: int = self.config.get("max_forward_msg_count", 99)
        self.max_file_size: int = self.config.get("max_file_size", 2147483648)
        self.forward_threshold: int = self.config.get("forward_threshold", 3)
        self.session_timeout: int = self.config.get("session_timeout_seconds", 300)
        self.multi_user_adding: bool = bool(self.config.get("multi_user_adding", False))
        self.maintenance_interval: int = self.config.get("maintenance_interval_seconds", 86400)

        # 安全审核：保护 sessions 的并发读写
        self.user_task_locks: dict[str, asyncio.Lock] = {}
        self._session_lock = asyncio.Lock()

        self._bot_uid: str | None = None
        self._bot_name: str | None = None

    def terminate(self):
        if self._maintenance_handle and not self._maintenance_handle.done():
            self._maintenance_handle.cancel()
        for uid, session in self.sessions.items():
            task = session.get("pending_task")
            if task and not task.done():
                task.cancel()
        self.sessions.clear()
        if self.db_h:
            self.db_h.close()
        logger.info("astrbot_plugin_keywords_miko 已安全卸载")

    def _ensure_maintenance(self):
        if not self._maintenance_handle or self._maintenance_handle.done():
            try:
                self._maintenance_handle = asyncio.create_task(self._maintenance_task())
            except RuntimeError:
                pass

    async def _ensure_bot_info(self, bot):
        if not self._bot_name:
            try:
                res = await bot.api.call_action("get_login_info")
                self._bot_uid = str(res.get("user_id", "0"))
                self._bot_name = res.get("nickname", "Bot")
            except Exception as e:
                logger.error(f"获取Bot信息失败: {e}")

    # ==================================================================
    # 一、指令区
    # ==================================================================
    @filter.command("开启群关键字")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_enable_group(self, event: AstrMessageEvent):
        gid = event.message_obj.group_id
        if not gid:
            yield event.plain_result("此指令仅限群聊使用")
            return
        wl = self.config.get("whitelist_groups", [])
        wl = list(set([str(i) for i in wl] + [str(gid)]))
        self.config["whitelist_groups"] = wl
        self.config.save_config()
        yield event.plain_result(f"已将 {gid} 加入群组白名单")

    @filter.command("关闭群关键字")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_disable_group(self, event: AstrMessageEvent):
        gid = event.message_obj.group_id
        # 边界修复：防止私聊触发污染白名单
        if not gid:
            yield event.plain_result("此指令仅限群聊使用")
            return
        gid = str(gid)
        wl = [i for i in self.config.get("whitelist_groups", []) if str(i) != gid]
        self.config["whitelist_groups"] = wl
        self.config.save_config()
        yield event.plain_result(f"已将 {gid} 移出群组白名单")

    @filter.command("开启群全局关键字")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_enable_global(self, event: AstrMessageEvent):
        gid = event.message_obj.group_id
        if not gid:
            yield event.plain_result("此指令仅限群聊使用")
            return
        gid = str(gid)
        if gid not in [str(i) for i in self.config.get("whitelist_groups", [])]:
            yield event.plain_result(f"{gid} 尚未加入群组白名单，请先开启群关键字")
            return
        gwl = self.config.get("global_whitelist_groups", [])
        gwl = list(set([str(i) for i in gwl] + [gid]))
        self.config["global_whitelist_groups"] = gwl
        self.config.save_config()
        yield event.plain_result(f"已将 {gid} 加入全局白名单")

    @filter.command("关闭群全局关键字")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_disable_global(self, event: AstrMessageEvent):
        gid = event.message_obj.group_id
        if not gid:
            yield event.plain_result("此指令仅限群聊使用")
            return
        gid = str(gid)
        gwl = [i for i in self.config.get("global_whitelist_groups", []) if str(i) != gid]
        self.config["global_whitelist_groups"] = gwl
        self.config.save_config()
        yield event.plain_result(f"已将 {gid} 移出全局白名单")

    @filter.command("群关键字插件白名单")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_list_group_wl(self, event: AstrMessageEvent):
        ids = [str(i) for i in self.config.get("whitelist_groups", [])]
        yield event.plain_result(f"群组白名单：\n{', '.join(ids) if ids else '暂无'}")

    @filter.command("全局群关键字插件白名单")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_list_global_wl(self, event: AstrMessageEvent):
        ids = [str(i) for i in self.config.get("global_whitelist_groups", [])]
        yield event.plain_result(f"全局白名单：\n{', '.join(ids) if ids else '暂无'}")

    @filter.command("添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_add(self, event: AstrMessageEvent, keyword: str):
        uid = str(event.message_obj.sender.user_id)
        gid = event.message_obj.group_id
        is_group = bool(gid and str(gid).strip())
        scope = "group" if is_group else "private"
        target = str(gid) if is_group else uid

        async with self._session_lock:
            if self.multi_user_adding:
                if uid in self.sessions:
                    yield event.plain_result("你已在添加模式中，请先 /结束添加 或 /取消添加")
                    return
            else:
                if self.adding_lock_user:
                    yield event.plain_result(f"管理员 {self.adding_lock_user} 正在添加中，请稍后再试")
                    return

        kw = keyword.strip().lower()
        if not kw:
            yield event.plain_result("关键字不能为空")
            return

        loop = asyncio.get_running_loop()
        if await loop.run_in_executor(None, self.db_h.match_scope_keyword, kw, scope, target) is not None:
            yield event.plain_result(f"关键字【{kw}】已存在，请先删除后再添加")
            return

        async with self._session_lock:
            self.sessions[uid] = {
                "kw": kw, "scope": scope, "target": target,
                "contents": [], "hashes": [], "last_time": time.time(),
                "umo": event.unified_msg_origin, "failed": False, "pending_task": None,
            }
            if not self.multi_user_adding:
                self.adding_lock_user = uid

        yield event.plain_result(
            f'正在添加关键字【{kw}】，请发送回复内容'
            f'（支持文本/图片/语音/视频/文件/表情/引用/合并转发），'
            f'发送 /结束添加 完成，/取消添加 取消')

    @filter.command("添加全局")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_add_global(self, event: AstrMessageEvent, keyword: str):
        if event.message_obj.group_id:
            yield event.plain_result("全局关键字只能在私聊中添加")
            return
        uid = str(event.message_obj.sender.user_id)

        async with self._session_lock:
            if self.multi_user_adding:
                if uid in self.sessions:
                    yield event.plain_result("你已在添加模式中，请先 /结束添加 或 /取消添加")
                    return
            else:
                if self.adding_lock_user:
                    yield event.plain_result(f"管理员 {self.adding_lock_user} 正在添加中，请稍后再试")
                    return

        kw = keyword.strip().lower()
        if not kw:
            yield event.plain_result("关键字不能为空")
            return

        loop = asyncio.get_running_loop()
        if await loop.run_in_executor(None, self.db_h.match_scope_keyword, kw, "global", "ALL") is not None:
            yield event.plain_result(f"全局关键字【{kw}】已存在，请先删除后再添加")
            return

        async with self._session_lock:
            self.sessions[uid] = {
                "kw": kw, "scope": "global", "target": "ALL",
                "contents": [], "hashes": [], "last_time": time.time(),
                "umo": event.unified_msg_origin, "failed": False, "pending_task": None,
            }
            if not self.multi_user_adding:
                self.adding_lock_user = uid

        yield event.plain_result(f'正在添加全局关键字【{kw}】，请发送回复内容，发送 /结束添加 完成')

    @filter.command("结束添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_finish(self, event: AstrMessageEvent):
        uid = str(event.message_obj.sender.user_id)

        async with self._session_lock:
            if uid not in self.sessions:
                yield event.plain_result("当前不在添加模式")
                return
            session = self.sessions[uid]

        user_lock = self.user_task_locks.get(uid)
        if user_lock:
            try:
                await asyncio.wait_for(user_lock.acquire(), timeout=60.0)
                user_lock.release()
            except asyncio.TimeoutError:
                yield event.plain_result("上一条消息处理超时，请重试")
                return

        if session.get("failed"):
            yield event.plain_result("保存失败或已超限，添加已取消")
        elif not session["contents"]:
            yield event.plain_result("未收到任何有效内容，添加已取消")
        else:
            try:
                max_kw = self.config.get("max_keywords_per_scope", 50)
                loop = asyncio.get_running_loop()
                current = await loop.run_in_executor(None, self.db_h.count_scope_keywords, session["scope"],
                                                     session["target"])
                if current >= max_kw:
                    yield event.plain_result(f"该作用域关键字数量已达上限({max_kw})，无法继续添加")
                else:
                    ok = await loop.run_in_executor(
                        None, self.db_h.save_keyword,
                        session["kw"], session["scope"], session["target"],
                        session["contents"], uid, session["hashes"])
                    if ok:
                        yield event.plain_result(f'关键字【{session["kw"]}】添加成功')
                    else:
                        yield event.plain_result(f'关键字【{session["kw"]}】已存在，添加失败')
            except Exception as e:
                logger.error(f"保存关键字异常: {traceback.format_exc()}")
                yield event.plain_result(f"保存失败: {str(e)}")

        async with self._session_lock:
            if not self.multi_user_adding:
                self.adding_lock_user = None
            self.sessions.pop(uid, None)
            self.user_task_locks.pop(uid, None)

    @filter.command("取消添加")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_cancel(self, event: AstrMessageEvent):
        uid = str(event.message_obj.sender.user_id)
        async with self._session_lock:
            if uid in self.sessions:
                kw = self.sessions[uid]["kw"]
                if not self.multi_user_adding:
                    self.adding_lock_user = None
                self.sessions.pop(uid, None)
                self.user_task_locks.pop(uid, None)
                yield event.plain_result(f"已取消关键字【{kw}】的添加")
                return
        yield event.plain_result("当前不在添加模式")

    @filter.command("删除")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_delete(self, event: AstrMessageEvent, keyword: str):
        uid = str(event.message_obj.sender.user_id)
        gid = event.message_obj.group_id
        is_group = bool(gid and str(gid).strip())
        scope = "group" if is_group else "private"
        target = str(gid) if is_group else uid
        kw = keyword.strip().lower()
        try:
            ok = await asyncio.get_running_loop().run_in_executor(None, self.db_h.delete_keyword, kw, scope, target)
        except Exception as e:
            logger.error(f"删除关键字异常: {e}")
            yield event.plain_result(f"删除失败: {str(e)}")
            return
        yield event.plain_result(f'关键字【{keyword}】已删除' if ok else f'关键字【{keyword}】不存在')

    @filter.command("删除全局")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_delete_global(self, event: AstrMessageEvent, keyword: str):
        kw = keyword.strip().lower()
        try:
            ok = await asyncio.get_running_loop().run_in_executor(None, self.db_h.delete_keyword, kw, "global", "ALL")
        except Exception as e:
            logger.error(f"删除全局关键字异常: {e}")
            yield event.plain_result(f"删除失败: {str(e)}")
            return
        yield event.plain_result(f'全局关键字【{keyword}】已删除' if ok else f'全局关键字【{keyword}】不存在')

    @filter.command("关键字列表")
    async def cmd_list(self, event: AstrMessageEvent, page: int = 1):
        gid = event.message_obj.group_id
        is_group = bool(gid and str(gid).strip())
        if is_group:
            scope, target, label = "group", str(gid), f"群 {gid}"
        else:
            uid = str(event.message_obj.sender.user_id)
            scope, target, label = "private", uid, "私聊"

        loop = asyncio.get_running_loop()
        total = await loop.run_in_executor(None, self.db_h.get_total_count, scope, target)
        if total == 0:
            yield event.plain_result(f"{label}关键字列表为空")
            return
        rows = await loop.run_in_executor(None, self.db_h.list_keywords, scope, target, page)
        total_pages = (total + 9) // 10
        body = "\n".join(f"- {r['keyword']}" for r in rows)
        txt = f"{label}关键字列表({page}/{total_pages}):\n{body}"
        if total_pages > page:
            txt += f"\n(发送 /关键字列表 {page + 1} 查看下一页)"
        yield event.plain_result(txt)

    @filter.command("全局关键字列表")
    async def cmd_list_global(self, event: AstrMessageEvent, page: int = 1):
        loop = asyncio.get_running_loop()
        total = await loop.run_in_executor(None, self.db_h.get_global_total_count)
        if total == 0:
            yield event.plain_result("全局关键字列表为空")
            return
        rows = await loop.run_in_executor(None, self.db_h.list_global_keywords, page)
        total_pages = (total + 9) // 10
        body = "\n".join(f"- {r['keyword']} (创建者: {r['creator']})" for r in rows)
        txt = f"全局关键字列表({page}/{total_pages}):\n{body}"
        if total_pages > page:
            txt += f"\n(发送 /全局关键字列表 {page + 1} 查看下一页)"
        yield event.plain_result(txt)

    # ==================================================================
    # 二、核心事件监听
    # ==================================================================
    @filter.event_message_type(filter.EventMessageType.ALL, priority=1)
    async def handle_everything(self, event: AstrMessageEvent):
        self._ensure_maintenance()

        now = time.time()
        dead_uids = []
        timeout_uid = None

        async with self._session_lock:
            if self.multi_user_adding:
                dead_uids = [uid for uid, s in self.sessions.items() if
                             now - s.get("last_time", 0) > self.session_timeout]
            else:
                timeout_uid = next(
                    (uid for uid, s in self.sessions.items() if now - s.get("last_time", 0) > self.session_timeout),
                    None)

            if dead_uids:
                tasks_to_notify = []
                for uid in dead_uids:
                    session_to_clean = self.sessions.pop(uid, None)
                    self.user_task_locks.pop(uid, None)
                    if session_to_clean:
                        kw = session_to_clean.get("kw", "")
                        umo = session_to_clean.get("umo")
                        if umo and kw:
                            tasks_to_notify.append((umo, kw))
                for umo, kw in tasks_to_notify:
                    asyncio.create_task(
                        self.context.send_message(umo, MessageChain([Plain(f"关键字【{kw}】的添加因超时已自动取消")])))

            if timeout_uid:
                notify_data = None
                async with self._session_lock:
                    self.adding_lock_user = None
                    session_to_clean = self.sessions.pop(timeout_uid, None)
                    self.user_task_locks.pop(timeout_uid, None)
                    if session_to_clean:
                        kw = session_to_clean.get("kw", "")
                        umo = session_to_clean.get("umo")
                        if umo and kw:
                            notify_data = (umo, kw)
                if notify_data:
                    umo, kw = notify_data
                    asyncio.create_task(
                        self.context.send_message(umo, MessageChain([Plain(f"关键字【{kw}】的添加因超时已自动取消")])))
        if event.get_platform_name() != "aiocqhttp":
            return

        uid = str(event.message_obj.sender.user_id)
        gid_raw = event.message_obj.group_id
        gid = str(gid_raw) if (gid_raw and str(gid_raw).strip()) else None
        msg_raw = event.message_str.strip()

        if msg_raw.startswith("/"):
            first_word = msg_raw.split()[0].lower()
            if first_word in ("/结束添加", "/取消添加", "/添加", "/添加全局", "/删除", "/删除全局"):
                return

        clean_text = self.media_s.get_clean_text(event.message_obj.message)

        async with self._session_lock:
            is_adding = (uid in self.sessions) if self.multi_user_adding else (self.adding_lock_user == uid)
            session_snapshot = self.sessions.get(uid)

        if is_adding and session_snapshot:
            if session_snapshot.get("failed"):
                async with self._session_lock:
                    if not self.multi_user_adding:
                        self.adding_lock_user = None
                    self.sessions.pop(uid, None)
                    self.user_task_locks.pop(uid, None)
                return
            if clean_text in ("结束添加", "取消添加"):
                return

            snapshot, reply_id, forward_id, ref_snapshot = self._parse_incoming_message(event)
            if snapshot or reply_id or forward_id or ref_snapshot:
                task = asyncio.create_task(
                    self._collect_task(event.bot, uid, gid, snapshot, reply_id, forward_id, ref_snapshot))
                async with self._session_lock:
                    if uid in self.sessions:
                        self.sessions[uid]["pending_task"] = task
                event.stop_event()
                return

        if gid:
            if gid not in [str(i) for i in self.config.get("whitelist_groups", [])]:
                return
        else:
            if uid not in self.admin_users:
                return

        if not clean_text:
            return

        loop = asyncio.get_running_loop()
        kw_lower = clean_text.lower()
        matched = False

        if gid and gid in [str(i) for i in self.config.get("global_whitelist_groups", [])]:
            g_match = await loop.run_in_executor(None, self.db_h.match_scope_keyword, kw_lower, "global", "ALL")
            if g_match:
                await self._do_send(event, g_match)
                matched = True

        target = gid if gid else uid
        scope = "group" if gid else "private"
        l_match = await loop.run_in_executor(None, self.db_h.match_scope_keyword, kw_lower, scope, target)
        if l_match:
            await self._do_send(event, l_match)
            matched = True

        if matched:
            event.stop_event()

    # ==================================================================
    # 三、消息解析
    # ==================================================================
    def _get_raw_elements(self, event: AstrMessageEvent):
        for source in (event, event.message_obj):
            for attr in ("_raw_event", "_raw", "_event", "_raw_nt", "_nt_raw"):
                val = getattr(source, attr, None)
                if isinstance(val, dict):
                    elements = val.get("elements")
                    if isinstance(elements, list) and elements:
                        return elements
        return None

    def _get_local_media_info(self, m_type, raw_elements):
        if not raw_elements:
            return "", ""
        et_map = {"image": 2, "file": 3, "record": 4, "video": 5}
        target_et = et_map.get(m_type)
        if not target_et:
            return "", ""
        for elem in raw_elements:
            if not isinstance(elem, dict) or elem.get("elementType") != target_et:
                continue
            if m_type == "image":
                pic = elem.get("picElement") or {}
                return pic.get("sourcePath", ""), pic.get("md5HexStr", "")
            elif m_type == "record":
                ptt = elem.get("pttElement") or {}
                return ptt.get("filePath", ""), ptt.get("md5HexStr", "")
            elif m_type == "video":
                vid = elem.get("videoElement") or {}
                return vid.get("filePath", ""), vid.get("videoMd5", "")
            elif m_type == "file":
                felem = elem.get("fileElement") or {}
                return (felem.get("filePath", ""), felem.get("fileMd5", "") or felem.get("file10MMd5", ""))
        return "", ""

    def _get_raw_ob11_segments(self, event: AstrMessageEvent):
        raw_msg = getattr(event.message_obj, "raw_message", None)
        if isinstance(raw_msg, dict):
            msg = raw_msg.get("message")
            if isinstance(msg, list) and msg and isinstance(msg[0], dict):
                return msg
        for attr in ("_raw_ob11", "_ob11_segments", "_raw_segments", "_raw_message_list", "segments"):
            val = getattr(event.message_obj, attr, None)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return val
        for attr in ("_event", "_raw_event", "_raw"):
            val = (getattr(event, attr, None) or getattr(event.message_obj, attr, None))
            if isinstance(val, dict):
                msg = val.get("message")
                if isinstance(msg, list) and msg and isinstance(msg[0], dict):
                    return msg
        if isinstance(raw_msg, str) and raw_msg.strip():
            parsed = MediaService._parse_cq_code(raw_msg)
            if parsed:
                return parsed
        for attr in ("_event", "_raw_event", "_raw"):
            val = (getattr(event, attr, None) or getattr(event.message_obj, attr, None))
            if isinstance(val, dict):
                raw_str = val.get("raw_message")
                if isinstance(raw_str, str) and raw_str.strip():
                    parsed = MediaService._parse_cq_code(raw_str)
                    if parsed:
                        return parsed
        return None

    def _parse_incoming_message(self, event: AstrMessageEvent):
        snapshot: list[dict] = []
        reply_id: str | None = None
        forward_id: str | None = None
        ref_snapshot: list[dict] = []

        raw_elements = self._get_raw_elements(event)
        raw_segs = self._get_raw_ob11_segments(event)

        has_raw_reply = False
        if raw_elements:
            for elem in raw_elements:
                if isinstance(elem, dict) and elem.get("elementType") == 7:
                    has_raw_reply = True
                    break
        has_ob11_reply = any(isinstance(s, dict) and s.get("type") == "reply" for s in (raw_segs or []))

        if has_raw_reply and not has_ob11_reply:
            raw_event = getattr(event, "_raw_event", None) or getattr(event.message_obj, "_raw_event", None)
            records = raw_event.get("records", []) if isinstance(raw_event, dict) else []
            if records and isinstance(records[0], dict):
                for elem in records[0].get("elements", []):
                    if not isinstance(elem, dict):
                        continue
                    et = elem.get("elementType")
                    if et == 1:
                        text = (elem.get("textElement") or {}).get("content", "")
                        if text.strip():
                            ref_snapshot.append({"type": "text", "val": text})
                    elif et == 2:
                        md5 = (elem.get("picElement") or {}).get("md5HexStr", "")
                        if md5:
                            ref_snapshot.append({"type": "media", "m_type": "image", "md5": md5})
                    elif et == 4:
                        md5 = (elem.get("pttElement") or {}).get("md5HexStr", "")
                        if md5:
                            ref_snapshot.append({"type": "media", "m_type": "record", "md5": md5})
                    elif et == 5:
                        md5 = (elem.get("videoElement") or {}).get("videoMd5", "")
                        if md5:
                            ref_snapshot.append({"type": "media", "m_type": "video", "md5": md5})

        if raw_segs:
            for seg in raw_segs:
                if not isinstance(seg, dict):
                    continue
                stype = seg.get("type", "")
                sdata = seg.get("data")
                if not isinstance(sdata, dict):
                    sdata = {}
                if stype == "forward":
                    forward_id = sdata.get("id")
                elif stype == "reply":
                    reply_id = sdata.get("id")
                elif stype == "text":
                    val = sdata.get("text", "")
                    if val and val.strip():
                        snapshot.append({"type": "text", "val": val})
                elif stype in MEDIA_DOWNLOAD_TYPES:
                    if stype == "file":
                        fid = sdata.get("file_id")
                        fn = sdata.get("file")
                    else:
                        fid = None
                        fn = sdata.get("file") or sdata.get("id")
                    if fn or fid:
                        lp, lm = self._get_local_media_info(stype, raw_elements)
                        snapshot.append({
                            "type": "media", "m_type": stype, "id": fn, "file_id": fid,
                            "md5": sdata.get("md5") or sdata.get("md5HexStr"),
                            "local_path": lp, "local_md5": lm, "file_size": sdata.get("file_size"),
                        })
                elif stype == "face":
                    face_id = sdata.get("id")
                    if face_id is not None:
                        snapshot.append({"type": "face", "id": str(face_id)})
                elif stype == "at":
                    qq = sdata.get("qq", "")
                    if qq:
                        snapshot.append({"type": "text", "val": f"@{qq} "})
        else:
            message_list = getattr(event.message_obj, "message", None) or []
            for comp in message_list:
                if isinstance(comp, Plain):
                    val = comp.text.strip()
                    if val:
                        snapshot.append({"type": "text", "val": val})
                elif isinstance(comp, Image) and comp.file:
                    lp, lm = self._get_local_media_info("image", raw_elements)
                    snapshot.append({"type": "media", "m_type": "image", "id": comp.file, "file_id": None,
                                     "md5": getattr(comp, "md5", None), "local_path": lp, "local_md5": lm,
                                     "file_size": None})
                elif isinstance(comp, Record) and comp.file:
                    lp, lm = self._get_local_media_info("record", raw_elements)
                    snapshot.append({"type": "media", "m_type": "record", "id": comp.file, "file_id": None,
                                     "md5": getattr(comp, "md5", None), "local_path": lp, "local_md5": lm,
                                     "file_size": None})
                elif isinstance(comp, Video) and comp.file:
                    lp, lm = self._get_local_media_info("video", raw_elements)
                    snapshot.append({"type": "media", "m_type": "video", "id": comp.file, "file_id": None,
                                     "md5": getattr(comp, "md5", None), "local_path": lp, "local_md5": lm,
                                     "file_size": None})
                elif isinstance(comp, File) and comp.file:
                    lp, lm = self._get_local_media_info("file", raw_elements)
                    snapshot.append({"type": "media", "m_type": "file", "id": comp.file, "file_id": None,
                                     "md5": getattr(comp, "md5", None), "local_path": lp, "local_md5": lm,
                                     "file_size": None})
                elif isinstance(comp, Face):
                    snapshot.append({"type": "face", "id": str(comp.id)})
                elif isinstance(comp, At):
                    qq = getattr(comp, "qq", None) or getattr(comp, "target", None)
                    if qq:
                        snapshot.append({"type": "text", "val": f"@{qq} "})
                elif isinstance(comp, Reply):
                    reply_id = getattr(comp, "id", None) or getattr(comp, "message_id", None)

        return snapshot, reply_id, forward_id, ref_snapshot

    # ==================================================================
    # 四、合并转发提取（递归）
    # ==================================================================
    async def _extract_forward_nodes(self, bot, forward_id, session_hashes, depth=1):
        if depth > self.max_nested_depth:
            return "depth_exceeded"
        try:
            res = await bot.api.call_action("get_forward_msg", message_id=str(forward_id))
            if isinstance(res, dict) and isinstance(res.get("data"), dict):
                messages = res["data"].get("messages")
            else:
                messages = res.get("messages") if isinstance(res, dict) else None

            if not isinstance(messages, list):
                return "api_error"
            if len(messages) > self.max_forward_count:
                return "count_exceeded"

            nodes = await self._process_forward_messages(bot, messages, session_hashes, depth)
            if nodes is None:
                return "unsupported_type"
            return nodes if nodes else "empty"
        except Exception as e:
            logger.error(f"获取合并转发异常: {e}")
            return "api_error"

    async def _process_forward_messages(self, bot, messages, session_hashes, depth):
        if depth > self.max_nested_depth:
            return None
        if not isinstance(messages, list) or len(messages) > self.max_forward_count:
            return None
        await self._ensure_bot_info(bot)
        nodes = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = msg.get("content") or msg.get("message") or []
            if isinstance(content, str):
                content = MediaService._parse_cq_code(content)
            if not isinstance(content, list):
                content = []
            processed = await self._process_forward_segments(bot, content, session_hashes, depth)
            if processed is None:
                return None
            if processed:
                nodes.append({
                    "uin": self._bot_uid or "0",
                    "name": self._bot_name or "Bot",
                    "content": processed
                })
        return nodes if nodes else None

    async def _process_forward_segments(self, bot, segments, session_hashes, depth):
        result = []
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            st = seg.get("type")
            sd = seg.get("data") or {}
            if not isinstance(sd, dict):
                sd = {}

            if st == "text":
                text = sd.get("text", "").replace("\u200b", "")
                if text.strip():
                    result.append({"type": "text", "data": text})
            elif st == "at":
                qq = sd.get("qq", "")
                if qq:
                    result.append({"type": "text", "data": f"@{qq} "})
            elif st == "face":
                fid = sd.get("id")
                if fid is not None:
                    result.append({"type": "face", "id": str(fid)})
            elif st in ("image", "record", "video", "file"):
                fn = sd.get("file")
                url_val = sd.get("url", "")

                need_download = True  # <<< 新增

                if isinstance(url_val, str) and url_val.startswith("/"):
                    url_md5 = None
                    base_name = os.path.basename(url_val)
                    name_part, _ = os.path.splitext(base_name)
                    if len(name_part) == 32:
                        url_md5 = name_part
                    if url_md5:
                        try:
                            loop = asyncio.get_running_loop()
                            row = await loop.run_in_executor(None, self.media_s.db.get_media_path, url_md5)
                            if row and row['file_path']:
                                exists = await loop.run_in_executor(None, os.path.exists, row['file_path'])
                                if exists:
                                    session_hashes.append(url_md5)
                                    result.append({"type": st, "file": row['file_path'], "name": ""})
                                    need_download = False  # <<< 命中缓存，不需要下载了
                        except Exception as e:
                            logger.error(f"本地缓存查库异常: {e}")

                if need_download and fn:
                    r = await self.media_s.save_forwarded_media(bot, st, raw_data=sd, max_size=self.max_file_size,
                                                                bot_uid=self._bot_uid)
                    if isinstance(r, dict) and r.get("error"):
                        return None
                    if not r:
                        return None
                    session_hashes.append(r["hash"])
                    result.append({"type": st, "file": r["path"]})
            elif st == "forward":
                nested_content = sd.get("content")
                nested_id = sd.get("id", "")
                nested_nodes = None
                if isinstance(nested_content, list) and nested_content:
                    nested_nodes = await self._process_forward_messages(bot, nested_content, session_hashes, depth + 1)
                elif nested_id:
                    nested_nodes = await self._extract_forward_nodes(bot, nested_id, session_hashes, depth + 1)
                if isinstance(nested_nodes, str):
                    return None
                if nested_nodes is None:
                    return None
                result.append({"type": "forward_node", "id": nested_id, "nodes": nested_nodes})
        return result

    # ==================================================================
    # 五、发送实现
    # ==================================================================
    async def _do_send(self, event: AstrMessageEvent, match_data: list):
        if not match_data:
            return
        if not self._bot_name:
            await self._ensure_bot_info(event.bot)
        bot_id, bot_name = self._bot_uid or "0", self._bot_name or "Bot"

        must_forward = []
        safe = []
        unsafe = []
        for block in match_data:
            types = {s.get("type") for s in block if isinstance(s, dict)}
            if "forward_node" in types:
                must_forward.append(block)
            elif types & {"file", "record"}:
                unsafe.append(block)
            else:
                safe.append(block)

        forward_blocks = list(must_forward)
        threshold = self.forward_threshold
        need_safe_forward = (threshold == 0 or len(safe) > threshold)

        if need_safe_forward:
            forward_blocks.extend(safe)
        if forward_blocks:
            await self._send_via_forward(event, forward_blocks)
        if not need_safe_forward:
            for block in safe:
                await self._send_via_normal(event, block)
        for block in unsafe:
            await self._send_via_normal(event, block)

    async def _send_via_forward(self, event, blocks: list):
        bot = event.bot
        api = bot.api
        bot_id, bot_name = self._bot_uid or "0", self._bot_name or "Bot"
        ob11_nodes = []
        for block in blocks:
            if not isinstance(block, list) or not block:
                continue
            is_forward = any(isinstance(s, dict) and s.get("type") == "forward_node" for s in block)
            if is_forward:
                inner_ob11_nodes = []
                for seg in block:
                    if isinstance(seg, dict) and seg.get("type") == "forward_node":
                        for inner_node in seg.get("nodes") or []:
                            if isinstance(inner_node, dict):
                                inner_content = self._segments_to_ob11(inner_node.get("content") or [])
                                if inner_content:
                                    inner_ob11_nodes.append({
                                        "type": "node",
                                        "data": {
                                            "uin": str(inner_node.get("uin") or bot_id),
                                            "name": str(inner_node.get("name") or bot_name),
                                            "content": inner_content,
                                        },
                                    })
                if inner_ob11_nodes:
                    ob11_nodes.append({
                        "type": "node",
                        "data": {"uin": bot_id, "name": bot_name, "content": inner_ob11_nodes},
                    })
            else:
                content = self._segments_to_ob11(block)
                if content:
                    ob11_nodes.append({
                        "type": "node",
                        "data": {"uin": bot_id, "name": bot_name, "content": content},
                    })
        if not ob11_nodes:
            return

        gid = event.get_group_id()
        uid = event.get_sender_id()
        try:
            await api.call_action(
                "send_forward_msg",
                message_type="group" if gid else "private",
                group_id=int(gid) if gid else None,
                user_id=int(uid) if uid else None,
                messages=ob11_nodes,
                prompt=bot_name,
            )
        except Exception as e:
            logger.warning(f"send_forward_msg 失败，尝试回退旧接口: {e}")
            try:
                if gid:
                    await api.call_action("send_group_forward_msg", group_id=int(gid), messages=ob11_nodes)
                elif uid:
                    await api.call_action("send_private_forward_msg", user_id=int(uid), messages=ob11_nodes)
            except Exception as e_fallback:
                logger.error(f"合并转发回退发送也失败: {e_fallback}")

    async def _send_via_normal(self, event, block: list):
        comps = self._to_comps(block)
        if comps:
            try:
                await self.context.send_message(event.unified_msg_origin, MessageChain(comps))
            except Exception as e:
                logger.error(f"普通消息发送失败: {e}")

    def _segments_to_ob11(self, segments):
        result = []
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            t = seg.get("type")
            if t == "forward_node":
                inner_nodes = seg.get("nodes") or []
                for inner in inner_nodes:
                    if not isinstance(inner, dict):
                        continue
                    inner_content = self._segments_to_ob11(inner.get("content") or [])
                    if inner_content:
                        result.append({
                            "type": "node",
                            "data": {"uin": str(inner.get("uin") or ""), "name": str(inner.get("name") or ""),
                                     "content": inner_content},
                        })
                continue
            elif t == "text":
                result.append({"type": "text", "data": {"text": seg.get("data", "")}})
            elif t == "face":
                result.append({"type": "face", "data": {"id": str(seg.get("id", ""))}})
            elif t in ("image", "video", "record"):
                f_path = seg.get("file", "").replace("\\", "/")
                result.append({"type": t, "data": {"file": f_path}})
            elif t == "file":
                f_path = seg.get("file", "").replace("\\", "/")
                d = {"type": "file", "data": {"file": f_path}}
                if seg.get("name"):
                    d["data"]["name"] = seg["name"]
                result.append(d)
            elif t == "reply":
                result.append({"type": "text", "data": {"text": "【引用了一条历史消息】\n"}})
            elif t == "mface":
                result.append({
                    "type": "mface",
                    "data": {
                        "emoji_id": seg.get("emoji_id", ""), "key": seg.get("key", ""),
                        "summary": seg.get("summary", ""), "emoji_package_id": str(seg.get("emoji_package_id", "")),
                    },
                })
            elif t == "at":
                result.append({"type": "at", "data": {"qq": str(seg.get("qq", ""))}})
        return result

    def _to_comps(self, block: list) -> list:
        res: list = []
        for i in block:
            if not isinstance(i, dict):
                continue
            t = i.get("type")
            f = (i.get("file") or "").replace("\\", "/")
            if t == "text":
                res.append(Plain(i.get("data", "")))
            elif t == "image":
                res.append(Image.fromFileSystem(f))
            elif t == "record":
                res.append(Record(file=f))
            elif t == "video":
                res.append(Video.fromFileSystem(f))
            elif t == "face":
                res.append(Face(id=i.get("id", "0")))
            elif t == "file":
                name = i.get("name", "") or os.path.basename(f)
                res.append(File(file=f, name=name))
            elif t == "reply":
                res.append(Reply(id=str(i.get("id", ""))))
        return res

    async def _send_collect_notice(self, bot, gid, uid, text):
        try:
            params = {"message": [{"type": "text", "data": {"text": text}}]}
            if gid:
                params["group_id"] = gid
                params["message_type"] = "group"
            else:
                params["user_id"] = uid
                params["message_type"] = "private"
            await bot.api.call_action("send_msg", **params)
        except Exception as e:
            logger.error(f"发送收集提示失败: {e}")

    # ==================================================================
    # 六、内容收集
    # ==================================================================
    async def _collect_task(self, bot, uid, gid, snapshot, reply_id, forward_id, ref_snapshot=None):
        async with self._session_lock:
            session = self.sessions.get(uid)
            if not session:
                return

        if uid not in self.user_task_locks:
            self.user_task_locks[uid] = asyncio.Lock()

        async with self.user_task_locks[uid]:
            async with self._session_lock:
                session = self.sessions.get(uid)
                if not session:
                    return
                session["last_time"] = time.time()
                session_hashes = session["hashes"]  # >>> 加上这一行，固定引用 <<<

            block: list[dict] = []

            # 致命缺陷修复：初始化 m_type 防止下方异常时出现 UnboundLocalError
            m_type = "unknown"

            if forward_id:
                await self._ensure_bot_info(bot)
                if not self._bot_uid:
                    await self._send_collect_notice(bot, gid, uid, "获取Bot信息失败，添加已取消")
                    async with self._session_lock:
                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                    return
                result = await self._extract_forward_nodes(bot, forward_id, session_hashes)
                error_msgs = {
                    "depth_exceeded": "超出嵌套深度限制",
                    "count_exceeded": "超出消息数量限制",
                    "unsupported_type": "包含不支持的内容类型（如文件）或媒体大小超限",
                    "api_error": "获取合并转发消息失败",
                    "empty": "合并转发内容为空",
                }
                if isinstance(result, str):
                    msg = error_msgs.get(result, "获取合并转发消息失败")
                    await self._send_collect_notice(bot, gid, uid, f"{msg}，添加已取消")
                    async with self._session_lock:
                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                    return
                block.append({"type": "forward_node", "id": str(forward_id), "nodes": result})
                await self._send_collect_notice(bot, gid, uid, "已记录合并转发内容")
                async with self._session_lock:
                    if uid in self.sessions: self.sessions[uid]["contents"].append(block)
                return

            if reply_id:
                try:
                    reply_data = await self.media_s.fetch_reply_content(bot, reply_id, session_hashes,
                                                                        max_size=self.max_file_size)
                    if reply_data:
                        block.append({"type": "reply", "id": str(reply_id)})
                        block.extend(reply_data)
                        block.append({"type": "text", "data": "\n—————这是回复消息—————\n"})
                        await self._send_collect_notice(bot, gid, uid, "已记录引用内容")
                    else:
                        await self._send_collect_notice(bot, gid, uid, "被引用消息不存在或获取失败，添加已取消")
                        async with self._session_lock:
                            if uid in self.sessions: self.sessions[uid]["failed"] = True
                        return
                except Exception as e:
                    logger.error(f"获取引用异常: {e}")
                    await self._send_collect_notice(bot, gid, uid, f"获取引用异常: {str(e)}，添加已取消")
                    async with self._session_lock:
                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                    return

            elif ref_snapshot:
                block.append({"type": "text", "data": "【以下为恢复的引用内容】\n"})
                saved_any = False
                for item in ref_snapshot:
                    if item["type"] == "text":
                        block.append({"type": "text", "data": item["val"]})
                        saved_any = True
                    elif item["type"] == "media":
                        md5 = item.get("md5")
                        if md5:
                            try:
                                loop = asyncio.get_running_loop()

                                row = await loop.run_in_executor(None, self.media_s.db.get_media_path, md5)

                                if row and row['file_path']:
                                    exists = await loop.run_in_executor(None, os.path.exists, row['file_path'])
                                    if exists:
                                        block.append({"type": item["m_type"], "file": row['file_path']})
                                        session_hashes.append(md5)  # >>> 改动 <<<
                                        saved_any = True
                            except Exception as e:
                                logger.error(f"MD5恢复引用异常: {e}")
                if saved_any:
                    block.append({"type": "text", "data": "\n—————这是回复消息—————\n"})
                    await self._send_collect_notice(bot, gid, uid, "已记录引用内容(通过本地缓存恢复)")
                else:
                    await self._send_collect_notice(bot, gid, uid, "被引用消息已过期且本地无缓存，添加已取消")
                    async with self._session_lock:
                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                    return

            for item in snapshot:
                try:
                    if item["type"] == "text":
                        block.append({"type": "text", "data": item["val"]})
                    elif item["type"] == "face":
                        block.append({"type": "face", "id": item["id"]})
                    elif item["type"] == "media":
                        m_type = item["m_type"]
                        ob11_size = item.get("file_size")
                        if ob11_size:
                            try:
                                if int(ob11_size) > self.max_file_size:
                                    await self._send_collect_notice(bot, gid, uid,
                                                                    f"{m_type} 文件大小超过限制，添加已取消")
                                    async with self._session_lock:
                                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                                    return
                            except (ValueError, TypeError):
                                pass

                        lp = item.get("local_path", "")
                        if lp:
                            try:
                                loop = asyncio.get_running_loop()
                                fsize = await loop.run_in_executor(None, os.path.getsize, lp)
                                if fsize > self.max_file_size:
                                    await self._send_collect_notice(bot, gid, uid,
                                                                    f"{m_type} 文件大小({fsize}字节)超过限制，添加已取消")
                                    async with self._session_lock:
                                        if uid in self.sessions: self.sessions[uid]["failed"] = True
                                    return
                            except OSError:
                                pass

                        orig_name = None
                        if m_type == "file":
                            orig_name = item.get("id") or None
                        r = await self.media_s.save_media(
                            bot, m_type, file_name=item.get("id"), file_id=item.get("file_id"),
                            local_path=item.get("local_path"), md5=item.get("md5") or item.get("local_md5"),
                            original_name=orig_name, max_size=self.max_file_size)

                        if isinstance(r, dict) and r.get("error") == "too_large":
                            fsize = r.get("size", 0)
                            await self._send_collect_notice(bot, gid, uid,
                                                            f"{m_type} 文件大小({fsize}字节)超过限制，添加已取消")
                            async with self._session_lock:
                                if uid in self.sessions: self.sessions[uid]["failed"] = True
                            return
                        elif r and "error" not in r:
                            saved = {"type": m_type, "file": r["path"]}
                            if m_type == "file" and r.get("name"):
                                saved["name"] = r["name"]
                            block.append(saved)
                            session_hashes.append(r["hash"])  # >>> 改动 <<<
                            await self._send_collect_notice(bot, gid, uid, f"已存入 {m_type}")
                        else:
                            await self._send_collect_notice(bot, gid, uid, f"{m_type} 保存失败，已跳过")
                except Exception as e:
                    item_type = item.get('type', 'unknown')  # 从 item 安全获取类型
                    logger.error(f"{item_type} 保存异常: {traceback.format_exc()}")
                    await self._send_collect_notice(bot, gid, uid, f"{m_type} 保存异常({type(e).__name__}: {e})，已跳过")

            if block:
                async with self._session_lock:
                    if uid in self.sessions:
                        self.sessions[uid]["contents"].append(block)

    # ==================================================================
    # 七、后台维护
    # ==================================================================
    async def _maintenance_task(self):
        while True:
            try:
                def _do_cleanup():
                    cleanup_days = int(self.config.get("cleanup_interval_days", 7))
                    deadline = datetime.now() - timedelta(days=cleanup_days)
                    to_delete = self.db_h.cleanup_orphan_media(deadline)  # 调用带锁的方法
                    for hash_val, fpath in to_delete:
                        if fpath and os.path.exists(fpath):
                            try:
                                os.remove(fpath)
                            except OSError:
                                pass
            except asyncio.CancelledError:
                logger.info("后台维护任务已被主动取消")
                break
            except Exception as e:
                logger.error(f"后台维护任务执行异常: {e}")

            try:
                await asyncio.sleep(self.maintenance_interval)
            except asyncio.CancelledError:
                break
