"""洛雪自定义源运行时：脚本下载/校验 + Node 沙箱进程管理 + musicUrl 解析。

对齐洛雪桌面版自定义源规范（https://lxmusic.toside.cn/desktop/custom-source）：
- 脚本必须以 ``/* ... */`` 头部注释块开头，元数据字段 @name/@description/@version/@author/@homepage；
- 脚本通过 ``globalThis.lx`` 与宿主交互，事件仅 inited / request / updateAlert；
- 非 local 源的 actions 仅支持 musicUrl（搜索/歌词/封面/榜单走本服务内置平台接口）。

与 js/bridge.js 的进程协议见该文件头注释。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx

logger = logging.getLogger("lxmusic_service.runtime")

SCRIPT_MAX_BYTES = 9_000_000
MAX_REDIRECTS = 3
# 服务端中转型脚本（如 pdone/lx-music-source 系）init 期间要先联系自家 API 服务器，
# 实测 1.4~6s、弱网更久；10s 会把"慢而活"的源掐死在激活环节，
# 30s 对齐桌面版"不设硬超时"的宽裕度，又不至于让激活请求无限挂起
INIT_TIMEOUT_S = 30.0
MUSIC_PLATFORMS = ("kw", "kg", "tx", "wy", "mg")

_META_LIMITS = {"name": 24, "description": 36, "author": 56, "homepage": 1024, "version": 36}
_HEADER_BLOCK_RE = re.compile(r"^/\*[\S\s]+?\*/")
_META_LINE_RE = re.compile(r"^\s?\*\s?@(\w+)\s(.+)$", re.M)
_URL_RE = re.compile(r"^https?://", re.I)
_FILE_URL_RE = re.compile(r"^file://", re.I)
# 上传脚本落盘目录（容器内 /data/lxmusic/uploads，随数据卷持久化）
UPLOAD_DIR_NAME = "uploads"

# tier（本服务内部档位）→ 期望的脚本档位（洛雪规范 qualitys 取值）
_TIER_QUALITY_PREFERENCE = {
    "lossless": ["flac", "flac24bit", "hires"],
    "high": ["320k"],
    "standard": ["128k"],
}


class SourceError(Exception):
    """带错误类别的源操作失败；category ∈ download/invalid/init/no_platform/resolve。"""

    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


def reap_orphan_bridges(
    bridge_path: "str | Path | None" = None, *, proc_root: "str | Path" = "/proc"
) -> list[int]:
    """清扫父进程已死亡的残留 bridge 子进程（issue #29「多进程叠加」根因）。

    服务被 SIGKILL/断电等异常终止时，Node 子进程被内核重挂到 PID 1 继续常驻；
    重启后 SourceManager 只管自己拉起的新进程，孤儿便在 ps 里叠加（#29 实测
    三个同名进程、etime 各为 26h/12m/4m，即多次异常重启的累计残留）。

    只清理同时满足以下条件的进程，其余一律不动：
      1. cmdline 恰好引用本服务 bridge.js 的绝对路径（不同部署路径互不影响）；
      2. 父进程已不存在（或已是 PID 1 托孤）——父进程尚在的不确定归属进程保守跳过；
      3. 不是本进程自己的孩子（孩子由 UserSource 生命周期管理）。
    proc_root 参数仅供测试注入伪 /proc 树。
    """
    try:
        target = str(
            (Path(bridge_path) if bridge_path else Path(__file__).resolve().parent / "js" / "bridge.js")
            .resolve()
        )
    except OSError:
        return []
    me = os.getpid()
    root = Path(proc_root)
    killed: list[int] = []
    try:
        entries = list(root.iterdir())
    except OSError:
        return []
    for entry in entries:
        name = entry.name
        if not name.isdigit() or int(name) == me:
            continue
        try:
            args = (entry / "cmdline").read_bytes().split(b"\0")
            if not any(
                os.path.realpath(a.decode("utf-8", "replace")) == target for a in args if a
            ):
                continue
            stat_text = (entry / "stat").read_text()
            ppid = int(stat_text[stat_text.rindex(")") + 2:].split()[1])
            if ppid == me:
                continue
            if ppid != 1 and (root / str(ppid)).exists():
                continue
            os.kill(int(name), signal.SIGTERM)
            killed.append(int(name))
            logger.warning("reaped orphan lx bridge process pid=%s (ppid=%s)", name, ppid)
        except (OSError, ValueError, IndexError):
            continue
    return killed


def parse_script_meta(script: str) -> dict:
    """解析脚本头部元数据；头部块或 @name 缺失视为无效脚本（对齐桌面版校验）。"""
    header = _HEADER_BLOCK_RE.match(script or "")
    if header is None:
        raise SourceError("invalid", "自定义源脚本必须以 /* ... */ 头部注释块开头")
    meta: dict[str, str] = {}
    for key, value in _META_LINE_RE.findall(header.group(0)):
        limit = _META_LIMITS.get(key)
        if limit is None:
            continue
        value = value.strip()
        meta[key] = (value[:limit] + "...") if len(value) > limit else value
    if not meta.get("name"):
        raise SourceError("invalid", "脚本头部必须声明 @name 元数据")
    return meta


def is_source_url(url: str) -> bool:
    """合法源地址：http(s):// 或 file://（本服务数据目录内的上传脚本）。"""
    raw = (url or "").strip()
    return bool(_URL_RE.match(raw) or _FILE_URL_RE.match(raw))


def _validate_script_text(script: str) -> None:
    """脚本文本统一校验：UTF-8 已由调用方解码；头部元数据必须合法。"""
    if len(script.encode("utf-8")) > SCRIPT_MAX_BYTES:
        raise SourceError("download", "脚本超过 9MB 大小上限")
    parse_script_meta(script)


async def download_script(url: str) -> str:
    """下载/读取源脚本（重定向≤3、≤9MB、UTF-8），并完成头部校验。

    file:// URL 直接读本地文件——仅用于本服务数据目录（uploads/）内的
    上传脚本与 state 缓存，路径已在落盘时净化。
    """
    url = (url or "").strip()
    if _FILE_URL_RE.match(url):
        raw_path = url[len("file://"):]
        unquoted = unquote(raw_path)
        path = unquoted if os.path.exists(unquoted) else raw_path
        if not path.startswith("/"):
            raise SourceError("download", "file:// 路径必须是绝对路径")
        try:
            raw = Path(path).read_bytes()
        except OSError as exc:
            raise SourceError("download", f"读取脚本文件失败: {exc}") from exc
        try:
            script = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SourceError("invalid", "脚本不是有效的 UTF-8 文本") from exc
        _validate_script_text(script)
        return script
    if not _URL_RE.match(url):
        raise SourceError("download", "源地址必须以 http:// 、https:// 或 file:// 开头")
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            max_redirects=MAX_REDIRECTS,
            timeout=httpx.Timeout(20.0, connect=8.0),
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            declared = resp.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > SCRIPT_MAX_BYTES:
                raise SourceError("download", "脚本超过 9MB 大小上限")
            raw = resp.content
    except SourceError:
        raise
    except httpx.TooManyRedirects as exc:
        raise SourceError("download", "重定向次数超过 3 次") from exc
    except Exception as exc:  # noqa: BLE001
        raise SourceError("download", f"下载失败: {exc}") from exc
    if len(raw) > SCRIPT_MAX_BYTES:
        raise SourceError("download", "脚本超过 9MB 大小上限")
    try:
        script = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceError("invalid", "脚本不是有效的 UTF-8 文本") from exc
    parse_script_meta(script)
    return script


_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._\-\u4e00-\u9fff]+")


def sanitize_upload_filename(filename: str) -> str:
    """上传文件名净化：basename、去控制字符、强制 .js 后缀。"""
    name = os.path.basename((filename or "").strip())
    name = _SAFE_FILENAME_RE.sub("_", name).strip("._") or "source"
    if not name.lower().endswith(".js"):
        name = f"{name}.js"
    return name[:120]


def save_upload(state_dir: "str | Path", filename: str, script: str) -> "tuple[str, str]":
    """把上传脚本写入数据目录 uploads/，返回 (容器内绝对路径, file:// URL)。

    同名冲突时追加时间戳；调用方负责先做脚本校验（parse_script_meta）。
    """
    _validate_script_text(script)
    uploads = Path(state_dir) / UPLOAD_DIR_NAME
    uploads.mkdir(parents=True, exist_ok=True)
    name = sanitize_upload_filename(filename)
    target = uploads / name
    if target.exists():
        stem = target.stem
        target = uploads / f"{stem}-{int(time.time() * 1000) % 10_000_000_000}.js"
    target.write_text(script, encoding="utf-8")
    return str(target), f"file://{target}"


def script_quality_for_tier(tier: str, declared: list[str]) -> "str | None":
    """tier → 脚本声明中最合适的档位；无匹配返回 None（由上层降档重试）。"""
    for quality in _TIER_QUALITY_PREFERENCE.get(tier or "standard", []):
        if quality in declared:
            return quality
    return None


def build_music_info(item: dict, platform: str) -> dict:
    """本服务条目 → 洛雪脚本期望的 musicInfo 结构。

    主键约定（与主流洛雪源一致）：kg 用 hash，其余平台用 songmid；
    tx 用 strMediaMid，mg 用 copyrightId，一并冗余提供。"""
    identifier = str(item.get("_identifier") or "")

    def seconds_str(seconds: Any) -> str:
        # 洛雪官方 musicInfo.interval 是纯秒数字符串（如 "253"），不是 MM:SS
        try:
            total = int(float(seconds) or 0)
        except (TypeError, ValueError):
            total = 0
        return str(total)

    info = {
        "songmid": identifier,
        "songId": identifier,
        "name": str(item.get("title") or item.get("name") or ""),
        "singer": str(item.get("artist") or item.get("singer") or ""),
        "source": platform,
        "interval": seconds_str(item.get("duration_s") or item.get("duration")),
        "albumName": str(item.get("album") or item.get("albumName") or ""),
        "meta": {
            "songId": identifier,
            "albumName": str(item.get("album") or item.get("albumName") or ""),
            "picUrl": str(item.get("cover_url") or item.get("cover") or item.get("picUrl") or "") or None,
        },
    }
    # identifier 即 "lx:<platform>:<identifier>" 的平台主键：条目缺字段时兜底，
    # 保证脚本总能拿到本平台的规范主键（kg 读 hash、kw 读 rid……）
    platform_key = {
        "kg": ("hash", item.get("hash") or identifier),
        "tx": ("songmid", item.get("songmid") or identifier),
        "mg": ("copyrightId", item.get("copyright_id") or identifier),
        "kw": ("rid", item.get("rid") or identifier),
        "wy": ("songId", item.get("song_id") or identifier),
    }.get(platform)
    if platform_key and platform_key[1]:
        info[platform_key[0]] = str(platform_key[1])
    if platform == "kg":
        kg_hash = str(item.get("hash") or identifier or "")
        if kg_hash:
            info["hash"] = kg_hash
            info["songmid"] = kg_hash
            info["id"] = kg_hash
        audio_id = str(item.get("mixsongid") or item.get("audio_id") or item.get("album_audio_id") or "").strip()
        if audio_id and audio_id not in ("0", "None"):
            info["audio_id"] = audio_id
            info["album_audio_id"] = audio_id
    # 社区源（六音酷狗、全豆要 QQ）按官方 musicInfo 读这些别名。
    # QQ 的 songmid 与 file.media_mid 经常不是同一个值，不能互相顶替。
    info["id"] = info.get("songmid") or identifier
    album_id = str(item.get("album_id") or "").strip()
    if album_id and album_id not in ("0", "None"):
        info["albumId"] = album_id
    media_mid = str(item.get("str_media_mid") or item.get("media_mid") or "").strip()
    if media_mid:
        info["strMediaMid"] = media_mid
    return info


class UserSource:
    """一个已加载的用户自定义源：持久 Node 子进程 + 请求多路复用。"""

    def __init__(self, script: str, meta: dict, *, script_dir: "str | None" = None):
        self.script = script
        self.meta = meta
        self.node_bin = os.environ.get("LX_NODE_BIN", "node")
        self._script_dir = Path(script_dir or tempfile.gettempdir())
        self._script_path: "Path | None" = None
        self._proc: "asyncio.subprocess.Process | None" = None
        self._pending: dict[str, asyncio.Future] = {}
        self._write_lock = asyncio.Lock()
        self._reader_task: "asyncio.Task | None" = None
        self._stderr_task: "asyncio.Task | None" = None
        self._next_id = 0
        self._started = False
        self._inited_event = asyncio.Event()
        self._inited_payload: Any = None
        self._init_error: "str | None" = None
        self.inited: dict = {}
        self.platforms: dict[str, dict] = {}
        self.started_at = 0.0

    # ------------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        if self._started:
            return
        bridge = Path(__file__).resolve().parent / "js" / "bridge.js"
        if not bridge.exists():
            raise SourceError("init", f"bridge script missing: {bridge}")
        try:
            self._script_dir.mkdir(parents=True, exist_ok=True)
            self._script_path = self._script_dir / f"lx-source-{os.getpid()}-{int(time.time() * 1000)}.js"
            self._script_path.write_text(self.script, encoding="utf-8")
        except OSError as exc:
            raise SourceError("init", f"无法写入脚本缓存: {exc}") from exc
        try:
            self._proc = await asyncio.create_subprocess_exec(
                self.node_bin,
                str(bridge),
                str(self._script_path),
                json.dumps(self.meta, ensure_ascii=False),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise SourceError("init", f"未找到 Node.js 运行时（{self.node_bin}）") from exc
        except Exception as exc:  # noqa: BLE001
            raise SourceError("init", f"启动 Node 沙箱失败: {exc}") from exc
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        try:
            self.inited = await asyncio.wait_for(self._wait_inited(), INIT_TIMEOUT_S)
        except asyncio.TimeoutError as exc:
            await self.stop()
            raise SourceError("init", f"脚本 {INIT_TIMEOUT_S:.0f}s 内未完成初始化（inited）") from exc
        except SourceError:
            await self.stop()
            raise
        sources = self.inited.get("sources")
        platforms: dict[str, dict] = {}
        if isinstance(sources, dict):
            for key, info in sources.items():
                code = str(key or "").lower()
                if code not in MUSIC_PLATFORMS or not isinstance(info, dict):
                    continue
                actions = [str(a) for a in (info.get("actions") or [])]
                if "musicUrl" not in actions:
                    continue
                platforms[code] = {
                    "name": str(info.get("name") or key),
                    "actions": actions,
                    "qualitys": [str(q) for q in (info.get("qualitys") or []) if q],
                }
        if not platforms:
            await self.stop()
            raise SourceError(
                "init", "脚本未声明任何可用的音乐平台（需 kw/kg/tx/wy/mg 之一且 actions 含 musicUrl）"
            )
        self.platforms = platforms
        self._started = True
        self.started_at = time.time()
        logger.info(
            "lx user source loaded: %s v%s (platforms=%s)",
            self.meta.get("name"), self.meta.get("version"), ",".join(sorted(platforms)),
        )

    async def _wait_inited(self) -> dict:
        await self._inited_event.wait()
        if self._init_error:
            raise SourceError("init", self._init_error)
        payload = self._inited_payload
        if not isinstance(payload, dict) or payload.get("status") is False:
            message = "脚本初始化失败（inited status=false）"
            if isinstance(payload, dict) and payload.get("message"):
                message = f"{message}: {payload['message']}"
            raise SourceError("init", message)
        return payload

    async def stop(self) -> None:
        self._started = False
        self._inited_event.set()
        for task in (self._reader_task, self._stderr_task):
            if task is not None:
                task.cancel()
        self._reader_task = None
        self._stderr_task = None
        proc = self._proc
        if proc is not None:
            try:
                if proc.returncode is None:
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), 3.0)
                    except asyncio.TimeoutError:
                        proc.kill()
            except ProcessLookupError:
                pass
        self._proc = None
        self._fail_pending("source stopped")
        if self._script_path is not None and self._script_path.exists():
            try:
                self._script_path.unlink()
            except OSError:
                pass
            self._script_path = None

    # ------------------------------------------------------------------ 协议读写

    async def _read_loop(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        try:
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(msg, dict):
                    self._dispatch(msg)
        finally:
            self._fail_pending("node process exited")
            if not self._inited_event.is_set():
                self._init_error = "Node 进程提前退出"
                self._inited_event.set()

    async def _read_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        try:
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    logger.warning("lx bridge stderr: %s", text[:500])
        except Exception:  # noqa: BLE001
            pass

    def _dispatch(self, msg: dict) -> None:
        mtype = msg.get("type")
        if mtype == "event":
            name = msg.get("name")
            payload = msg.get("payload")
            if name == "inited":
                if not self._inited_event.is_set():
                    self._inited_payload = payload
                    self._inited_event.set()
            elif name == "fatal":
                if not self._inited_event.is_set():
                    self._init_error = str((payload or {}).get("error") or "bridge fatal")
                    self._inited_event.set()
                else:
                    self._fail_pending("bridge fatal")
            elif name == "updateAlert":
                logger.info("lx source update alert: %s", str(payload or "")[:200])
        elif mtype == "log":
            level = str(msg.get("level") or "info")
            message = str(msg.get("message") or "")
            bound = getattr(logger, "warning" if level in ("warn", "error") else "info", logger.info)
            bound("[%s] %s", self.meta.get("name") or "lx-source", message[:500])
        elif mtype == "pong":
            pass
        elif "id" in msg:
            fut = self._pending.pop(str(msg["id"]), None)
            if fut is None or fut.done():
                return
            if msg.get("ok"):
                fut.set_result(msg.get("result"))
            else:
                fut.set_exception(SourceError("resolve", str(msg.get("error") or "script request failed")))

    def _fail_pending(self, reason: str) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(SourceError("resolve", reason))
        self._pending.clear()

    @property
    def running(self) -> bool:
        return self._started and self._proc is not None and self._proc.returncode is None

    def qualitys(self, platform: str) -> list[str]:
        return list(self.platforms.get(platform, {}).get("qualitys") or [])

    def music_platforms(self) -> list[str]:
        return [p for p in MUSIC_PLATFORMS if p in self.platforms]

    async def music_url(self, music_info: dict, quality: "str | None", *, platform: str,
                        timeout: float = 10.0) -> str:
        """调用脚本 musicUrl；quality 为洛雪规范档位（128k/320k/flac/...）。"""
        if not self.running:
            raise SourceError("init", "源运行时未就绪")
        self._next_id += 1
        req_id = f"r{self._next_id}"
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        payload = {
            "type": "request",
            "id": req_id,
            "source": platform,
            "action": "musicUrl",
            "info": {"type": quality, "musicInfo": music_info},
        }
        try:
            async with self._write_lock:
                proc = self._proc
                if proc is None or proc.stdin is None:
                    raise SourceError("init", "源运行时未就绪")
                proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
                await proc.stdin.drain()
            result = await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError as exc:
            self._pending.pop(req_id, None)
            raise SourceError("resolve", f"musicUrl 解析超时（{timeout:.0f}s）") from exc
        url = str(result or "")
        if not url.startswith(("http://", "https://")):
            raise SourceError("resolve", "脚本未返回有效的 http 直链")
        return url

    def describe(self) -> dict:
        # issue #29：暴露 bridge 进程 pid 与运行时长，便于用户上报"多进程/CPU 占用"时
        # 直接对照 healthz 与宿主 ps 输出（正常恒为单进程；多进程=容器/服务被外部拉起多次）
        pid = self._proc.pid if self._proc is not None else None
        uptime_s = round(time.time() - self.started_at, 1) if self.started_at else None
        return {
            "name": self.meta.get("name") or "",
            "version": self.meta.get("version") or "",
            "author": self.meta.get("author") or "",
            "description": self.meta.get("description") or "",
            "homepage": self.meta.get("homepage") or "",
            "platforms": {
                code: {"name": info["name"], "qualitys": info["qualitys"]}
                for code, info in sorted(self.platforms.items())
            },
            "running": self.running,
            "started_at": self.started_at,
            "pid": pid,
            "uptime_s": uptime_s,
        }


MAX_SOURCE_SLOTS = 3


class SourceManager:
    """当前激活源管理：支持最多 3 个槽位（主音源 + 2 个备用音源）。
    
    state.json 持久化（URL 列表 + 脚本缓存），env 仅作首次种子。
    遇单源不可播时按序回退到备用源寻找可用资源。
    """

    def __init__(
        self,
        state_dir: "str | None" = None,
        seed_url: str = "",
        seed_urls: "list[str] | None" = None,
    ):
        base = state_dir or os.environ.get("LX_DATA_DIR") or "/data/lxmusic"
        try:
            self.state_dir = Path(base)
            self.state_dir.mkdir(parents=True, exist_ok=True)
            _probe = self.state_dir / ".write-probe"
            _probe.write_text("", encoding="utf-8")
            _probe.unlink()
        except OSError:
            self.state_dir = Path(tempfile.mkdtemp(prefix="lxmusic-state-"))
            logger.warning("lx data dir %s not writable, falling back to %s", base, self.state_dir)
        self.state_path = self.state_dir / "state.json"
        
        # 3 个槽位的脚本缓存路径（slot 0 保留 source.js 保证向下兼容）
        self.script_caches: list[Path] = [
            self.state_dir / "source.js",
            self.state_dir / "source_1.js",
            self.state_dir / "source_2.js",
        ]
        self.script_cache = self.script_caches[0]

        # 种子 URL（env / 参数）
        seeds: list[str] = ["", "", ""]
        if seed_urls:
            for i, u in enumerate(seed_urls[:MAX_SOURCE_SLOTS]):
                seeds[i] = (u or "").strip()
        elif seed_url:
            seeds[0] = seed_url.strip()
        else:
            seeds[0] = (os.environ.get("LX_SOURCE_URL") or "").strip()
            seeds[1] = (os.environ.get("LX_SOURCE_URL_2") or "").strip()
            seeds[2] = (os.environ.get("LX_SOURCE_URL_3") or "").strip()
        self.seed_urls = seeds
        self.seed_url = seeds[0]

        self._runtimes: list[UserSource | None] = [None, None, None]
        self._lock = asyncio.Lock()
        self.active_urls: list[str] = ["", "", ""]
        self.last_errors: list[str] = ["", "", ""]

    @property
    def active_url(self) -> str:
        return self.active_urls[0]

    @active_url.setter
    def active_url(self, val: str) -> None:
        self.active_urls[0] = val

    @property
    def last_error(self) -> str:
        return self.last_errors[0]

    @last_error.setter
    def last_error(self, val: str) -> None:
        self.last_errors[0] = val

    # ------------------------------------------------------------------ 状态

    def read_state(self) -> dict:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (OSError, ValueError):
            return {}

    def _write_state(self) -> None:
        try:
            payload = {
                "url": self.active_urls[0],
                "urls": list(self.active_urls),
                "activated_at": time.time(),
            }
            self.state_path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("cannot persist lx source state: %s", exc)

    # ------------------------------------------------------------------ 激活/加载/停用

    async def activate(self, url: str, slot: int = 0, *, script: "str | None" = None) -> UserSource:
        """校验并切换到给定槽位的源（成功后旧槽位运行时立即停用）。"""
        if not (0 <= slot < MAX_SOURCE_SLOTS):
            raise SourceError("invalid", f"无效的槽位编号: {slot} (必须在 0~{MAX_SOURCE_SLOTS - 1} 之间)")
        async with self._lock:
            script = script if script is not None else await download_script(url)
            meta = parse_script_meta(script)
            runtime = UserSource(script, meta, script_dir=str(self.state_dir))
            await runtime.start()

            old = self._runtimes[slot]
            self._runtimes[slot] = runtime
            self.active_urls[slot] = url
            self.last_errors[slot] = ""
            
            # 持久化脚本缓存
            try:
                self.script_caches[slot].write_text(script, encoding="utf-8")
                if slot == 0 and (self.state_dir / "source_0.js") != self.script_caches[0]:
                    try:
                        (self.state_dir / "source_0.js").write_text(script, encoding="utf-8")
                    except OSError:
                        pass
            except OSError as exc:
                logger.warning("cannot cache lx script for slot %d: %s", slot, exc)

            self._write_state()

            if old is not None:
                await old.stop()
            return runtime

    async def deactivate(self, slot: int = 0) -> None:
        """停用指定槽位的源。"""
        if not (0 <= slot < MAX_SOURCE_SLOTS):
            return
        async with self._lock:
            old = self._runtimes[slot]
            self._runtimes[slot] = None
            self.active_urls[slot] = ""
            self.last_errors[slot] = ""
            try:
                self.script_caches[slot].unlink(missing_ok=True)
            except OSError:
                pass
            self._write_state()
            if old is not None:
                await old.stop()

    async def load_slot(self, slot: int, url: str) -> None:
        """加载单个槽位源脚本（优先下载，失败回退对应槽位缓存）。"""
        if not url:
            return
        script = None
        cache_file = self.script_caches[slot]
        try:
            script = await download_script(url)
        except SourceError as exc:
            self.last_errors[slot] = f"{exc.category}: {exc}"
            logger.warning("lx source slot %d download failed (%s), trying cache", slot, exc)
            if cache_file.exists():
                try:
                    script = cache_file.read_text(encoding="utf-8")
                    parse_script_meta(script)
                except (OSError, SourceError, UnicodeDecodeError):
                    script = None
        if script is None:
            logger.warning("lx source slot %d unavailable: no script (url=%s)", slot, url[:120])
            return
        try:
            meta = parse_script_meta(script)
            runtime = UserSource(script, meta, script_dir=str(self.state_dir))
            await runtime.start()
            self._runtimes[slot] = runtime
            self.active_urls[slot] = url
            logger.info("lx source slot %d loaded from %s", slot, "cache" if self.last_errors[slot] else "download")
        except SourceError as exc:
            self.last_errors[slot] = f"{exc.category}: {exc}"
            logger.warning("lx source slot %d init failed: %s", slot, exc)

    async def load(self) -> None:
        """服务启动时加载：支持最多 3 个槽位。state.json 优先，env 种子兜底。"""
        try:
            reap_orphan_bridges()
        except Exception as exc:  # noqa: BLE001
            logger.warning("orphan bridge reaper failed (ignored): %s", type(exc).__name__)
        async with self._lock:
            state = self.read_state()
            state_urls = state.get("urls")
            if not isinstance(state_urls, list) or len(state_urls) == 0:
                # 兼容旧版本仅有单 url 字段的 state.json
                single_url = str(state.get("url") or "").strip()
                state_urls = [single_url, "", ""]
            
            target_urls: list[str] = ["", "", ""]
            for i in range(MAX_SOURCE_SLOTS):
                u_state = str(state_urls[i] if i < len(state_urls) else "").strip()
                u_seed = str(self.seed_urls[i] if i < len(self.seed_urls) else "").strip()
                target_urls[i] = u_state or u_seed

            if not any(target_urls):
                logger.info("no lx sources configured (state/env both empty)")
                return

            for slot in range(MAX_SOURCE_SLOTS):
                if self._runtimes[slot] is not None:
                    continue
                url = target_urls[slot]
                if url:
                    await self.load_slot(slot, url)

    async def shutdown(self) -> None:
        async with self._lock:
            runtimes = list(self._runtimes)
            self._runtimes = [None, None, None]
            for r in runtimes:
                if r is not None:
                    await r.stop()

    def get(self, slot: int = 0) -> "UserSource | None":
        if 0 <= slot < MAX_SOURCE_SLOTS:
            runtime = self._runtimes[slot]
            if runtime is not None and runtime.running:
                return runtime
        return None

    def get_runtimes(self) -> "list[tuple[int, UserSource]]":
        """按优先级返回所有处于就绪状态的 (slot_index, runtime) 元组。"""
        res = []
        for i, r in enumerate(self._runtimes):
            if r is not None and r.running:
                res.append((i, r))
        return res

    def describe(self) -> dict:
        runtimes = self.get_runtimes()
        primary = self.get(0)
        return {
            "configured": bool(any(self.active_urls) or any(self.seed_urls)),
            "url": self.active_urls[0] or self.seed_urls[0],
            "urls": [self.active_urls[i] or self.seed_urls[i] for i in range(MAX_SOURCE_SLOTS)],
            "initialized": bool(runtimes),
            "last_error": self.last_errors[0],
            "last_errors": list(self.last_errors),
            "source": primary.describe() if primary is not None else None,
            "sources": [
                (r.describe() if (r and r.running) else None)
                for r in self._runtimes
            ],
        }

