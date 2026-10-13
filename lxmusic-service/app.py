"""lxmusic HTTP 服务：洛雪音乐 (LX Music) 风格音源 API.

统一曲目 ID 契约: "lx:<source>:<identifier>"，例如：
  - "lx:kg:<filehash>"    酷狗
  - "lx:wy:<song_id>"     网易云
  - "lx:mg:<copyrightId>" 咪咕

分工与洛雪桌面版一致：搜索/歌词/封面/热门榜单走内置平台免登录接口；
播放直链解析只由用户提供的洛雪自定义源脚本完成（source_runtime.py，
Node 沙箱执行，规范见 https://lxmusic.toside.cn/desktop/custom-source）。
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any

import httpx
from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from source_runtime import (
    SourceError,
    SourceManager,
    build_music_info,
    is_source_url,
    parse_script_meta,
    save_upload,
    script_quality_for_tier,
)

logger = logging.getLogger("lxmusic_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

SERVICE_VERSION = "2.0.0"

# 支持的音源别名归一化
_SOURCE_ALIASES = {
    "kg": "kg",
    "kugou": "kg",
    "wy": "wy",
    "netease": "wy",
    "163": "wy",
    "mg": "mg",
    "migu": "mg",
    "tx": "tx",
    "qq": "tx",
    "tencent": "tx",
    "kw": "kw",
    "kuwo": "kw",
}


def normalize_source(raw: str) -> str:
    return _SOURCE_ALIASES.get((raw or "").strip().lower(), "")


def _normalize_sources(raw_list: list) -> list:
    """LX_SOURCES → 规范平台代码，去重去非法；全部非法时回退默认。

    别名（kugou/kuwo 等）若不归一，会被 /api/v1/search 的 `_SEARCHERS[src]`
    查找静默跳过，表现为"配置了平台却搜不到结果"。"""
    out: list = []
    for item in raw_list:
        code = normalize_source(item)
        if code and code not in out:
            out.append(code)
    return out or ["kg", "wy", "mg", "kw"]


CONF = {
    "sources": _normalize_sources(
        [s.strip() for s in os.environ.get("LX_SOURCES", "kg,wy,mg,kw").split(",") if s.strip()]
    ),
    "search_timeout": float(os.environ.get("LX_SEARCH_TIMEOUT", "12")),
    "limit_per_source": int(os.environ.get("LX_LIMIT_PER_SOURCE", "20")),
    # 播放端 track/url 端点总预算：容纳单档解析（12s）+ 探活（5s）后再降档，
    # 需略低于 proxy 侧 resolve_lx_url 的单档 HTTP 超时（22s）
    "url_timeout": float(os.environ.get("LX_URL_TIMEOUT", "20")),
    "cache_max": int(os.environ.get("LX_CACHE_MAX", "2000")),
    "cache_ttl": int(os.environ.get("LX_CACHE_TTL", "1800")),
    # 用户自定义源脚本地址（支持最多 3 个源：主源 + 备用源 1 + 备用源 2）
    "source_url": (os.environ.get("LX_SOURCE_URL") or "").strip(),
    "source_url_2": (os.environ.get("LX_SOURCE_URL_2") or "").strip(),
    "source_url_3": (os.environ.get("LX_SOURCE_URL_3") or "").strip(),
    # 用户源单次 musicUrl 解析预算：野生源多为二级转发（脚本→中转服务→平台），
    # 实证水位在 3-8s（verify_source 用 12s），4s 会把慢源全部掐死
    "resolver_timeout": float(os.environ.get("LX_RESOLVER_TIMEOUT", "12.0")),
    "probe_timeout": float(os.environ.get("LX_PROBE_TIMEOUT", "5.0")),
    # 搜索期 VIP/第三方直链曲目的探活结果有效期（秒）：过期后 track/url 重新解析
    "probe_fresh_s": int(os.environ.get("LX_PROBE_FRESH_S", "900")),
    # 逐曲探活开关（beta）：默认关。开启时在搜索阶段逐曲调用用户源解析+Range探活；
    # 慢速用户源或多平台并发容易引发搜索超时，建议保持关闭（播放时实时解析直链）
    "search_probe": os.environ.get("FNMUSIC_SEARCH_PROBE", "false").lower() in ("true", "1", "yes"),
}

# 当前激活的用户自定义源（启动时从 state.json / LX_SOURCE_URL* 恢复）
SOURCE_MANAGER = SourceManager(
    seed_urls=[CONF["source_url"], CONF["source_url_2"], CONF["source_url_3"]]
)

UA_PC = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
UA_MOBILE = "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"

# id -> {"item": {...}, "ts": float}
_SONG_CACHE: dict[str, dict] = {}
_RESOLUTION_FAILURES: ContextVar[list | None] = ContextVar("lx_resolution_failures", default=None)


def _record_failure(exc: Exception) -> None:
    failures = _RESOLUTION_FAILURES.get()
    if failures is not None:
        failures.append(str(exc) or type(exc).__name__)


_SEARCH_PARTIAL: ContextVar[list | None] = ContextVar("lx_search_partial", default=None)
_SEARCH_PROBE_ENABLED: ContextVar[bool | None] = ContextVar("lx_search_probe_enabled", default=None)


def is_probe_enabled() -> bool:
    v = _SEARCH_PROBE_ENABLED.get()
    if v is not None:
        return v
    return bool(CONF.get("search_probe", False))


def _publish(item: dict) -> None:
    partial = _SEARCH_PARTIAL.get()
    if partial is not None:
        partial.append(item)


_STATS = {"searches": 0, "url_resolutions": 0, "errors": 0}

# 试运行（verify_source）只覆盖当前 asyncio 任务的解析运行时，
# 不替换进程级 SOURCE_MANAGER，避免安装/WebUI 校验打挂正在播的源。
_RUNTIME_OVERRIDE: ContextVar[Any] = ContextVar("lx_runtime_override", default=None)


def current_runtimes() -> list[tuple[int, Any]]:
    override = _RUNTIME_OVERRIDE.get()
    if override is not None:
        return [(0, override)]
    return SOURCE_MANAGER.get_runtimes()


def current_runtime():
    runtimes = current_runtimes()
    if runtimes:
        return runtimes[0][1]
    return None


def _lenient_json(resp: httpx.Response, tag: str = "") -> dict | list | None:
    """宽容解析 JSON：第三方接口可能返回 Content-Type text/html 但内容为合法 JSON。"""
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        pass
    text = (resp.text or "").strip()
    if not text or (not text.startswith("{") and not text.startswith("[")):
        logger.warning(
            "%s: upstream returned non-JSON body (HTTP %s, CT %s): %.80s",
            tag,
            resp.status_code,
            resp.headers.get("content-type"),
            text,
        )
        return None
    import json as _json
    try:
        return _json.loads(text)
    except Exception as e:  # noqa: BLE001
        logger.warning("%s: json parse failed: %s (%.80s)", tag, e, text)
        return None



def parse_track_id(track_id: str) -> "tuple[str, str]":
    """解析 "lx:<source>:<identifier>" -> ("kg", "<identifier>")；异常时返回 ("", "")."""
    parts = (track_id or "").strip().split(":", 2)
    if len(parts) == 3 and parts[0] == "lx":
        src = normalize_source(parts[1])
        if src and parts[2]:
            return src, parts[2]
    # 兼容 "kg:xxx" / "wy:xxx" 形式
    if len(parts) == 2:
        src = normalize_source(parts[0])
        if src and parts[1]:
            return src, parts[1]
    return "", ""


def _cache_put(item: dict) -> None:
    if not item.get("id"):
        return
    if len(_SONG_CACHE) >= CONF["cache_max"]:
        for k in sorted(_SONG_CACHE, key=lambda k: _SONG_CACHE[k]["ts"])[: len(_SONG_CACHE) // 2]:
            _SONG_CACHE.pop(k, None)
    _SONG_CACHE[item["id"]] = {"item": item, "ts": time.time()}


def _cache_get(track_id: str) -> dict | None:
    entry = _SONG_CACHE.get(track_id)
    if not entry:
        return None
    if time.time() - entry["ts"] > CONF["cache_ttl"]:
        _SONG_CACHE.pop(track_id, None)
        return None
    return entry["item"]


def _quality_tiers(quality: str) -> list[str]:
    q = (quality or "").strip().lower()
    if q in ("lossless", "flac", "sq", "hires", "hr"):
        return ["lossless", "high", "standard"]
    if q in ("high", "320", "exhigh", "hq"):
        return ["high", "standard"]
    return ["standard"]


# ------------------------------------------------------------------ 酷狗 kg ---

async def kg_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    # 检索更多条目以便剔除收费/VIP曲目后仍能满足 limit 数量
    fetch_size = max(limit * 3, 20)
    r = await client.get(
        "http://mobilecdn.kugou.com/api/v3/search/song",
        params={
            "keyword": keyword,
            "format": "json",
            "page": 1,
            "pagesize": fetch_size,
            "showtype": 1,
        },
        headers={"User-Agent": UA_MOBILE},
    )
    r.raise_for_status()
    data = r.json()
    raw = ((data or {}).get("data") or {}).get("info") or []
    items = []
    vip_candidates = []
    for it in raw:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        fhash = str(it.get("hash") or "")
        if not fhash:
            continue

        # 可播放性过滤：
        # 1. 收费/VIP 曲目不直接丢弃，转入探活队列（直链解析+Range探活通过才返回）
        pay_type = int(it.get("pay_type") or 0)
        is_vip = pay_type != 0 or int(it.get("pkg_price") or 0) != 0 or int(it.get("price") or 0) != 0
        # 2. 排除仅免费试听片段标记 (is_free_part=1) 及 VIP 拦截 (fail_process=4)，真不可播
        if int(it.get("is_free_part") or 0) != 0 or int(it.get("fail_process") or 0) == 4:
            continue

        singer = str(it.get("singername") or "")
        title = str(it.get("songname") or it.get("filename") or "").replace(f"{singer} - ", "")
        # 3. 标题带有试听片段标记的坚决不返回
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        sq = str(it.get("sqhash") or "")
        hq = str(it.get("hqhash") or "")
        # v3 搜索接口 duration 单位为秒（2026-09 实测：晴天=269，4:29）；
        # 按毫秒除以 1000 会把时长变成 0:00（下游按秒渲染）
        duration_s = int(it.get("duration") or 0)
        cover = str(it.get("origin_cover") or it.get("img") or "").replace("{size}", "480")
        item = {
            "id": f"lx:kg:{fhash}",
            "lx_source": "kg",
            "title": title,
            "artist": singer,
            "album": str(it.get("album_name") or ""),
            "duration_s": duration_s,
            "ext": "flac" if sq else "mp3",
            "cover_url": cover,
            "file_size": int(sq and it.get("sq_size") or it.get("filesize") or 0) or 0,
            "lyric": "",
            "hash": fhash,
            "hash_hq": hq,
            "hash_sq": sq,
            "album_id": str(it.get("album_id") or ""),
            "mixsongid": str(it.get("mixsongid") or ""),
            "pay_type": pay_type,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    # VIP 候选：开启逐曲探活时探活后补进；未开启时直接作为未验证候选补齐结果
    if vip_candidates and len(items) < limit:
        want = min(len(vip_candidates), limit - len(items) + limit // 2)
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "kg", vip_candidates[:want], limit - len(items)))
        else:
            for it in vip_candidates[:want]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[: limit + limit // 2]


async def kg_resolve_lyric(client: httpx.AsyncClient, item: dict) -> str:
    duration_ms = int(float(item.get("duration_s") or 0) * 1000)
    r = await client.get(
        "https://krcs.kugou.com/search",
        params={
            "ver": 1,
            "man": "yes",
            "client": "mobi",
            "keyword": f"{item.get('title','')} {item.get('artist','')}".strip(),
            "duration": duration_ms,
            "hash": item.get("hash") or "",
        },
        headers={"User-Agent": UA_MOBILE},
    )
    candidates = ((r.json() or {}).get("candidates") or [])
    if not candidates:
        return ""
    cand = candidates[0]
    r2 = await client.get(
        "http://lyrics.kugou.com/download",
        params={
            "ver": 1,
            "client": "pc",
            "id": cand.get("id"),
            "accesskey": cand.get("accesskey"),
            "fmt": "lrc",
            "charset": "utf8",
        },
        headers={"User-Agent": UA_PC},
    )
    content = (r2.json() or {}).get("content") or ""
    if not content:
        return ""
    try:
        return base64.b64decode(content).decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        return ""


# ------------------------------------------------------------------ 网易 wy ---

async def wy_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    fetch_limit = max(limit * 2, 20)
    r = await client.post(
        "https://music.163.com/api/search/get/web",
        data={"s": keyword, "type": 1, "offset": 0, "limit": fetch_limit, "total": "true"},
        headers={
            "User-Agent": UA_PC,
            "Referer": "https://music.163.com/",
            "Cookie": "os=pc; appver=9.1.15",
        },
    )
    r.raise_for_status()
    songs = ((r.json() or {}).get("result") or {}).get("songs") or []
    items = []
    vip_candidates = []
    for it in songs:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        sid = str(it.get("id") or "")
        if not sid:
            continue

        # 可播放性过滤：fee∉(0,8) 的 VIP/付费曲不直接丢弃，转探活队列验证
        fee = int(it.get("fee") or 0)
        is_vip = fee not in (0, 8)

        # 排除无版权（真不可播）
        if it.get("noCopyrightRcmd") is not None and it.get("noCopyrightRcmd") != 0:
            continue

        # 检查 privilege 状态
        priv = it.get("privilege")
        if isinstance(priv, dict):
            priv_fee = int(priv.get("fee", fee))
            if priv_fee not in (0, 8):
                is_vip = True
            if int(priv.get("pl") or 0) <= 0 and int(priv.get("st") or 0) < 0:
                continue
            if priv.get("freeTrialPrivilege") and priv.get("freeTrialPrivilege", {}).get("cannotListenReason"):
                continue

        title = str(it.get("name") or "")
        # 排除标题含试听片段标记
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue

        artists = it.get("artists") or []
        artist = " / ".join(
            str(a.get("name") or "") for a in artists if isinstance(a, dict)
        )
        album = it.get("album") or {}
        item = {
            "id": f"lx:wy:{sid}",
            "lx_source": "wy",
            "title": str(it.get("name") or ""),
            "artist": artist,
            "album": str(album.get("name") or "") if isinstance(album, dict) else "",
            "duration_s": int(it.get("duration") or 0) / 1000.0,
            "ext": "mp3",
            "cover_url": str(album.get("picUrl") or "") if isinstance(album, dict) else "",
            "file_size": 0,
            "lyric": "",
            "song_id": sid,
            "fee": fee,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    # VIP 候选：开启逐曲探活时探活后补进；未开启时直接作为未验证候选补齐结果
    if vip_candidates and len(items) < limit:
        want = min(len(vip_candidates), limit - len(items) + limit // 2)
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "wy", vip_candidates[:want], limit - len(items)))
        else:
            for it in vip_candidates[:want]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[: limit + limit // 2]


async def wy_resolve_lyric(client: httpx.AsyncClient, identifier: str) -> str:
    r = await client.get(
        "https://music.163.com/api/song/lyric",
        params={"id": identifier, "lv": 1, "tv": -1},
        headers={"User-Agent": UA_PC, "Referer": "https://music.163.com/", "Cookie": "os=pc"},
    )
    lrc = (r.json() or {}).get("lrc") or {}
    return str(lrc.get("lyric") or "")


# ------------------------------------------------------------------ 咪咕 mg ---

async def mg_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    fetch_size = max(limit * 2, 10)
    r = await client.get(
        "https://c.music.migu.cn/MIGUM2.0/v1.0/content/search_all.do",
        params={"text": keyword, "pageNo": 1, "pageSize": fetch_size, "resource": 1},
        headers={"User-Agent": UA_MOBILE, "Referer": "https://m.music.migu.cn/"},
    )
    r.raise_for_status()
    data = r.json() or {}
    raw = data.get("songs") or (data.get("songResultData") or {}).get("result") or []

    def _map_one(it: dict) -> dict | None:
        if not isinstance(it, dict):
            return None
        cid = str(it.get("copyrightId") or it.get("id") or "")
        if not cid:
            return None
        title = str(it.get("songName") or "")
        if _explicit_trial(it) or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            return None
        singers = it.get("singers") or []
        artist = " / ".join(str(s.get("name") or "") for s in singers if isinstance(s, dict))
        album = it.get("albums") or []
        album_name = str(album[0].get("albumName") or album[0].get("name") or "") if album and isinstance(album[0], dict) else ""
        covers = it.get("albumMaterialList") or []
        cover = str((covers[0] or {}).get("coverUrl") or "") if covers else ""
        tones = {str(t.get("toneType") or "").upper() for t in (it.get("toneFlags") or []) if isinstance(t, dict)}
        length_ms = int(it.get("length") or 0)
        item = {
            "id": f"lx:mg:{cid}",
            "lx_source": "mg",
            "title": title,
            "artist": artist,
            "album": album_name,
            "duration_s": length_ms / 1000.0,
            "ext": "flac" if tones & {"SQ", "ZQ", "ZQ24"} else "mp3",
            "cover_url": cover,
            "file_size": 0,
            "lyric": "",
            "lrc_url": str(it.get("lrcUrl") or ""),
            "copyright_id": cid,
        }
        return item

    candidates = [item for it in raw[:fetch_size] if (item := _map_one(it))]
    if is_probe_enabled():
        return await _probe_candidates(client, "mg", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


async def mg_resolve_lyric(client: httpx.AsyncClient, item: dict) -> str:
    lrc_url = str(item.get("lrc_url") or "")
    if not lrc_url:
        return ""
    r = await client.get(lrc_url, headers={"User-Agent": UA_MOBILE})
    return r.text or ""


# ------------------------------------------------------------ 用户源解析与熔断 ---
#
# 播放直链解析的唯一通道是用户提供的洛雪自定义源脚本（source_runtime.py）。
# 熔断器沿用原第三方链路机制：脚本连续解析失败达阈值后暂停调用一段时间，
# 避免每次搜索白等超时；媒体探活（probe_url）仍由本模块统一执行。


def _content_total_size(r: httpx.Response) -> int:
    cr = r.headers.get("content-range") or ""
    if "/" in cr:
        tail = cr.rsplit("/", 1)[-1].strip()
        if tail.isdigit():
            return int(tail)
    # A partial response's Content-Length is NOT the whole file size.
    cl = r.headers.get("content-length") or ""
    return int(cl) if r.status_code == 200 and cl.isdigit() else 0


_PROBE_BYTES = 4096


class ChainTransportError(Exception):
    """Infrastructure failure, unlike a healthy resolver's song miss."""


def _media_signature(body: bytes) -> str:
    """Positive signatures only; MIME and byte counts do not prove a full song."""
    if body.startswith(b"fLaC"):
        return "flac"
    if body.startswith(b"ID3"):
        return "mp3"
    if body.startswith(b"OggS"):
        return "ogg"
    if len(body) >= 12 and body[:4] == b"RIFF" and body[8:12] == b"WAVE":
        return "wav"
    if len(body) >= 12 and body[4:8] == b"ftyp":
        return "m4a"
    if len(body) >= 4 and body[0] == 0xff:
        if body[1] & 0xf6 == 0xf0:  # ADTS AAC
            return "aac"
        if (body[1] & 0xe0 == 0xe0 and body[1] & 6
                and body[2] & 0xf0 not in (0, 0xf0) and body[2] & 12 != 12):
            return "mp3"
    return ""


async def probe_url(
    client: httpx.AsyncClient, url: str, headers: "dict | None" = None,
    *, report_transport: bool = False,
) -> "tuple[bool, str, str, int]":
    """Bounded streaming prefix inspection, even when a CDN ignores Range.

    The tuple remains API-compatible. Success means media prefix verified, not
    complete-song verification. Streams close on success, failure and cancellation.
    """
    h = {k: v for k, v in (headers or {}).items()
         if k.lower() not in ("range", "accept-encoding")}
    h.setdefault("User-Agent", UA_PC)
    h.update({"Range": f"bytes=0-{_PROBE_BYTES - 1}", "Accept-Encoding": "identity"})
    try:
        async with asyncio.timeout(CONF["probe_timeout"]):
            for _ in range(6):  # at most five redirects, one shared deadline
                async with client.stream("GET", url, headers=h, follow_redirects=False,
                                         timeout=CONF["probe_timeout"]) as r:
                    if r.status_code in (301, 302, 303, 307, 308):
                        location = r.headers.get("location")
                        if not location:
                            return False, str(r.url), "", 0
                        target = r.url.join(location)
                        if target.scheme not in ("http", "https"):
                            return False, str(r.url), "", 0
                        if target.host != r.url.host:
                            h = {k: v for k, v in h.items()
                                 if k.lower() not in ("authorization", "cookie", "host")}
                        url = str(target)
                        # Leaving this context closes the intermediate response
                        # WITHOUT HTTPX's automatic redirect body draining.
                        continue
                    ct = (r.headers.get("content-type") or "").lower()
                    if r.status_code >= 500 or r.status_code in (408, 429):
                        raise ChainTransportError(f"media HTTP {r.status_code}")
                    if r.status_code not in (200, 206):
                        return False, str(r.url), ct, 0
                    if (ct.startswith("text/") or "json" in ct or "xml" in ct
                            or r.headers.get("content-encoding", "identity") != "identity"):
                        return False, str(r.url), ct, 0
                    prefix = bytearray()
                    # aiter_raw avoids decompression/buffering the full response. Slice
                    # each transport chunk and stop as soon as a signature is known.
                    if r.is_stream_consumed:  # in-memory transports (e.g. MockTransport)
                        prefix.extend(r.content[:_PROBE_BYTES])
                    else:
                        async for chunk in r.aiter_raw():
                            prefix.extend(chunk[:_PROBE_BYTES - len(prefix)])
                            ext = _media_signature(prefix)
                            if ext or len(prefix) >= _PROBE_BYTES:
                                break
                    ext = _media_signature(prefix)
                    return bool(ext), str(r.url), (f"audio/{ext}" if ext else ct), _content_total_size(r) if ext else 0
            raise ChainTransportError("media redirect limit exceeded")
    except (httpx.HTTPError, TimeoutError, ChainTransportError) as exc:
        if report_transport:
            raise ChainTransportError(str(exc)) from exc
        return False, url, "", 0


# 链路熔断器：连续失败达阈值后暂停该链路一段时间，避免每次搜索白等超时
_CHAIN_FAIL_THRESHOLD = 3
_CHAIN_OPEN_SECONDS = 600
# 熔断打开期间每 30s 放行一次 half-open 试探：第三方源 API 偶发抽风（连接重置等）
# 会让搜索探活连续失败触发熔断，若只能干等 10 分钟冷却，用户看到的就是"搜索全空"。
# 真实事故：换到健康源后熔断仍卡在 open，所有平台被 source_circuit_open 拦截。
_CHAIN_RETRY_SECONDS = 30
_CHAIN_HEALTH: dict[str, dict] = {}


def _chain_available(name: str) -> bool:
    h = _CHAIN_HEALTH.get(name)
    if not h:
        return True
    if h.get("half_open"):
        return False
    open_until = h.get("open_until", 0)
    if not open_until:
        return True
    # 冷却期内保留按需试探窗口：打开 RETRY 秒后即可 half-open 一次，
    # 成功立即闭合、失败仅顺延下一个试探窗口（每 30s 至多一个请求打到故障源）
    return time.time() >= open_until - (_CHAIN_OPEN_SECONDS - _CHAIN_RETRY_SECONDS)


def _chain_acquire(name: str) -> bool:
    if not _chain_available(name):
        return False
    h = _CHAIN_HEALTH.get(name)
    if h and h.get("open_until"):
        h["half_open"] = True  # synchronous claim: only one recovery request
    return True


def _chain_report(name: str, ok: bool) -> None:
    h = _CHAIN_HEALTH.setdefault(name, {"fails": 0, "open_until": 0.0, "breaks": 0})
    was_half_open = h.pop("half_open", False)
    if ok:
        h["fails"] = 0
        h["open_until"] = 0.0
        return
    h["fails"] = int(h.get("fails") or 0) + 1
    if was_half_open or h["fails"] >= _CHAIN_FAIL_THRESHOLD:
        h["open_until"] = time.time() + _CHAIN_OPEN_SECONDS
        h["fails"] = 0
        h["breaks"] = int(h.get("breaks") or 0) + 1


def chain_health_snapshot() -> dict:
    now = time.time()
    res = {}
    for slot in range(3):
        cname = f"user_source_{slot}"
        h = _CHAIN_HEALTH.get(cname, {})
        if slot == 0 and not h:
            h = _CHAIN_HEALTH.get("user_source", {})
        state = ("half_open" if h.get("half_open") else
                 "open" if h.get("open_until", 0) > now else
                 "recovery_ready" if h.get("open_until") else "closed")
        res[cname] = {"fails": h.get("fails", 0), "open": state == "open",
                      "breaks": h.get("breaks", 0), "state": state}
    res["user_source"] = res.get("user_source_0", {})
    return res


def source_capabilities() -> dict:
    active_runtimes = [r for _, r in current_runtimes()]
    ready = bool(active_runtimes)
    configured = bool(any(SOURCE_MANAGER.active_urls) or any(SOURCE_MANAGER.seed_urls))
    result = {}
    for src in _SEARCHERS:
        sup_runtimes = [r for r in active_runtimes if src in r.platforms]
        supported = bool(sup_runtimes)
        available = False
        qualitys: list[str] = []
        for slot, r in current_runtimes():
            if src in r.platforms:
                cname = f"user_source_{slot}"
                if _chain_available(cname) or (slot == 0 and _chain_available("user_source")):
                    available = True
                for q in r.qualitys(src):
                    if q not in qualitys:
                        qualitys.append(q)
        if available:
            reason = ""
        elif not ready:
            reason = "source_init_failed" if configured else "no_source_configured"
        elif not supported:
            reason = "platform_not_supported"
        else:
            reason = "source_circuit_open"
        result[src] = {"search_available": bool(available), "playback_available": bool(available),
                       "user_source": bool(supported),
                       "qualitys": qualitys,
                       "reason": reason,
                       "validation_status": "unverified", "completeness": "unknown"}
    return result


# ------------------------------------------------------------ 可播性验证 ---

_TRIAL_TITLE_MARKERS = ("(试听)", "（试听）", "试听片段", "片段试听", "试听版")


_TIER_RANK = {"standard": 0, "high": 1, "lossless": 2}


def _explicit_trial(data: dict) -> bool:
    """Only explicit clip metadata; fee/VIP and size are not trial proof."""
    for key in ("trial", "is_trial", "isTrial", "is_free_part", "isFreePart"):
        if str(data.get(key, "")).lower() in ("1", "true", "yes"):
            return True
    return bool(data.get("freeTrialInfo") or data.get("trialInfo")
                or data.get("trial_url") or data.get("trialUrl"))


def _actual_tier(result: dict) -> str:
    if result.get("ext") in ("flac", "wav", "ape"):
        return "lossless"
    br = int(result.get("br") or 0)
    if br >= 256000:
        return "high"
    return "standard" if br > 0 else "unknown"


async def _verify_result(client: httpx.AsyncClient, result: dict | None,
                         *, report_transport: bool = False) -> dict | None:
    if not result or not result.get("url") or _explicit_trial(result):
        return None
    result = dict(result)
    if not result.get("probed"):
        ok, final, ct, size = await probe_url(client, result["url"], result.get("headers"),
                                            report_transport=report_transport)
        if not ok:
            return None
        res_headers = dict(result.get("headers") or {})
        res_headers.setdefault("User-Agent", UA_PC)
        result.update(url=final, file_size=size or result.get("file_size") or 0,
                      ext=ct.split("/")[-1], probed=True, headers=res_headers)
    else:
        res_headers = dict(result.get("headers") or {})
        res_headers.setdefault("User-Agent", UA_PC)
        result["headers"] = res_headers
    result.update(validation_status="media_verified", completeness="unknown")
    result["actual_tier"] = _actual_tier(result)
    return result


def _fresh_probe(item: "dict | None", want_tier: str = "standard") -> "dict | None":
    """Reuse actual quality or a completed downgrade for this requested tier."""
    if not isinstance(item, dict) or _explicit_trial(item):
        return None
    p = item.get("_probe")
    if not (isinstance(p, dict) and p.get("url") and p.get("probed")
            and p.get("validation_status") == "media_verified"):
        return None
    if _explicit_trial(p):
        return None
    if time.time() - p.get("ts", 0) >= CONF["probe_fresh_s"]:
        return None
    want_tier = _quality_tiers(want_tier)[0]
    rank = _TIER_RANK.get(p.get("actual_tier"), -1)
    if rank < _TIER_RANK[want_tier] and want_tier not in p.get("attempted_tiers", []):
        return None
    return {k: v for k, v in p.items() if k not in ("ts", "tier")}


async def _fill_script_meta(client: httpx.AsyncClient, src: str, item: dict, identifier: str) -> None:
    """缓存过期后的播放只剩平台主键。QQ 的 media mid、酷狗的 albumId 要补回来再交给脚本。"""
    if not identifier:
        return
    try:
        if src == "tx" and (not item.get("str_media_mid") or not item.get("album_id")):
            r = await client.get(
                "https://c.y.qq.com/v8/fcg-bin/fcg_play_single_song.fcg",
                params={"songmid": identifier, "format": "json"},
                headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/"},
            )
            song = ((r.json() or {}).get("data") or [None])[0] or {}
            if isinstance(song, dict):
                file_obj = song.get("file") if isinstance(song.get("file"), dict) else {}
                album = song.get("album") if isinstance(song.get("album"), dict) else {}
                media = str(file_obj.get("media_mid") or "").strip()
                if media:
                    item["str_media_mid"] = media
                album_id = str(album.get("id") or "").strip()
                if album_id and album_id not in ("0", "None"):
                    item["album_id"] = album_id
                if not item.get("title"):
                    item["title"] = str(song.get("name") or song.get("title") or "")
                if not item.get("songmid"):
                    item["songmid"] = str(song.get("mid") or identifier)
        elif src == "kg":
            if not item.get("hash"):
                item["hash"] = identifier
            if not str(item.get("album_id") or "").strip() or not item.get("title"):
                try:
                    r = await client.get(
                        "http://m.kugou.com/app/i/getSongInfo.php",
                        params={"cmd": "playInfo", "hash": identifier},
                        headers={"User-Agent": UA_MOBILE},
                    )
                    data = r.json() or {}
                    if isinstance(data, dict):
                        album_id = str(data.get("albumid") or data.get("req_albumid") or "").strip()
                        if album_id and album_id not in ("0", "None"):
                            item["album_id"] = album_id
                        s_name = str(data.get("songName") or "").strip()
                        if s_name and not item.get("title"):
                            item["title"] = s_name
                        s_artist = str(data.get("singerName") or "").strip()
                        if s_artist and not item.get("artist"):
                            item["artist"] = s_artist
                except Exception:
                    pass
                if not item.get("title") or not str(item.get("album_id") or "").strip():
                    try:
                        r2 = await client.get(
                            "http://mobilecdn.kugou.com/api/v3/song/info",
                            params={"hash": identifier},
                            headers={"User-Agent": UA_MOBILE},
                        )
                        d2 = r2.json() or {}
                        if isinstance(d2, dict) and d2.get("data"):
                            info2 = d2.get("data") or {}
                            if not item.get("title"):
                                item["title"] = str(info2.get("songName") or "")
                            if not item.get("artist"):
                                item["artist"] = str(info2.get("singerName") or "")
                            album_id = str(info2.get("album_id") or "").strip()
                            if album_id and album_id not in ("0", "None"):
                                item["album_id"] = album_id
                    except Exception:
                        pass
    except Exception as exc:  # noqa: BLE001
        logger.warning("lx meta fill %s %s failed: %s", src, identifier, exc)


async def _resolve_and_probe(client: httpx.AsyncClient, src: str, item: dict,
                             tier: str = "standard", retained: dict | None = None) -> "dict | None":
    """Shared search/URL pipeline: user source musicUrl -> verify -> downgrade -> fallback to backup sources."""
    if _explicit_trial(item) or any(m in str(item.get("title") or "") for m in _TRIAL_TITLE_MARKERS):
        return None
    candidates = [(slot, r) for slot, r in current_runtimes() if src in r.platforms]
    if not candidates:
        return None
    tiers = _quality_tiers(tier)
    cached = _fresh_probe(item, tiers[0])
    if cached:
        return cached
    item.pop("_probe", None)
    item.update(verified=False, validation_status="unverified", completeness="unknown")
    identifier = parse_track_id(str(item.get("id") or ""))[1]
    item.setdefault("_identifier", identifier)
    await _fill_script_meta(client, src, item, identifier)
    music_info = build_music_info(item, src)
    attempted = []
    best = None
    failures = _RESOLUTION_FAILURES.get()

    for slot, runtime in candidates:
        circuit_name = f"user_source_{slot}"
        # 兼容旧 circuit 命名
        if not _chain_acquire(circuit_name) and not (slot == 0 and _chain_acquire("user_source")):
            _record_failure(ChainTransportError(f"user source slot {slot} circuit open"))
            continue
        recovering = bool(_CHAIN_HEALTH.get(circuit_name, {}).get("half_open"))
        slot_best = None

        for t in tiers:
            # Once a better known tier is retained, lower tiers cannot improve it.
            if slot_best and _TIER_RANK.get(slot_best["actual_tier"], -1) >= _TIER_RANK[t]:
                break
            before = len(failures or [])
            quality = script_quality_for_tier(t, runtime.qualitys(src))
            if quality is None:
                attempted.append(t)
                continue
            result = None
            try:
                url = await runtime.music_url(
                    music_info, quality, platform=src, timeout=CONF["resolver_timeout"]
                )
                result = await _verify_result(client, {"url": url}, report_transport=True)
            except asyncio.CancelledError:
                # Deadline/client cancellation says nothing about source health.
                _CHAIN_HEALTH.get(circuit_name, {}).pop("half_open", None)
                if slot == 0:
                    _CHAIN_HEALTH.get("user_source", {}).pop("half_open", None)
                raise
            except Exception as exc:  # transport, timeout, or script rejection
                _record_failure(exc)
                _chain_report(circuit_name, False)
                if slot == 0:
                    _chain_report("user_source", False)
            else:
                # A clean resolve without usable media is not a source outage.
                if recovering or not _CHAIN_HEALTH.get(circuit_name, {}).get("open_until"):
                    _chain_report(circuit_name, True)
                    if slot == 0:
                        _chain_report("user_source", True)
            if result:
                result["resolver"] = circuit_name
                if not slot_best or _TIER_RANK.get(result["actual_tier"], -1) > _TIER_RANK.get(slot_best["actual_tier"], -1):
                    slot_best = result
                    if retained is not None:
                        retained.update(best=slot_best, attempted=attempted)
            # Infrastructure-interrupted tiers must not be cached as exhausted.
            if len(failures or []) == before:
                attempted.append(t)

        if slot_best:
            best = slot_best
            if slot > 0:
                logger.info("lx resolve fallback to slot %d succeeded for %s:%s", slot, src, identifier)
            break
        else:
            logger.info("lx resolve on slot %d failed for %s:%s, trying next candidate source...", slot, src, identifier)

    if not best:
        # 智能跨平台搜源兜底（例如酷狗 kg、酷我 kw 原生解析失败时，用歌名和歌手自动在网易/QQ/咪咕等平台寻找同名直链）
        title = str(item.get("title") or "").strip()
        artist = str(item.get("artist") or "").strip()
        if title and not item.get("_cross_fallback"):
            fallback_platforms = [p for p in ("wy", "tx", "kw", "mg") if p != src]
            for fb_platform in fallback_platforms:
                try:
                    fb_candidates = [(s, r) for s, r in current_runtimes() if fb_platform in r.platforms]
                    if not fb_candidates:
                        continue
                    query = f"{title} {artist}".strip() if artist else title
                    matched_items = []
                    if fb_platform == "wy":
                        matched_items = await wy_search(client, query, limit=3)
                    elif fb_platform == "tx":
                        matched_items = await tx_search(client, query, limit=3)
                    elif fb_platform == "kw":
                        matched_items = await kw_search(client, query, limit=3)
                    elif fb_platform == "mg":
                        matched_items = await mg_search(client, query, limit=3)

                    for cand in matched_items:
                        cand_title = str(cand.get("title") or "").lower().strip()
                        if title.lower() in cand_title or cand_title in title.lower():
                            cand["_cross_fallback"] = True
                            fb_res = await _resolve_and_probe(client, fb_platform, cand, tier=tier, retained=retained)
                            if fb_res:
                                logger.info("Cross-platform fallback succeeded: %s -> %s for %s - %s", src, fb_platform, title, artist)
                                best = fb_res
                                break
                    if best:
                        break
                except Exception as fb_err:
                    logger.debug("Cross-platform fallback error %s -> %s: %s", src, fb_platform, fb_err)

    if best:
        best["attempted_tiers"] = attempted
        item["_probe"] = dict(best, ts=time.time(), tier=best["actual_tier"])
        item.update(verified=True, validation_status="media_verified", completeness="unknown")
        _cache_put(item)
    return best


async def resolve_and_probe(client: httpx.AsyncClient, src: str, item: dict,
                            tier: str = "standard", *, budget: float | None = None) -> dict | None:
    failures: list[str] = []
    retained: dict = {}
    token = _RESOLUTION_FAILURES.set(failures)
    try:
        deadline = asyncio.timeout(budget)
        try:
            async with deadline:
                result = await _resolve_and_probe(client, src, item, tier, retained)
        except TimeoutError:
            # Only our resolution budget may return retained media. External
            # caller cancellation remains CancelledError and propagates after
            # the awaited resolver/upgrade has completed cancellation cleanup.
            if not deadline.expired() or not retained.get("best"):
                raise
            result = dict(retained["best"], attempted_tiers=list(retained["attempted"]))
            item["_probe"] = dict(result, ts=time.time(), tier=result["actual_tier"])
            item.update(verified=True, validation_status="media_verified", completeness="unknown")
            _cache_put(item)
        if result is None and failures:
            raise ChainTransportError("resolution infrastructure exhausted: " + failures[-1])
        return result
    finally:
        _RESOLUTION_FAILURES.reset(token)


def _chunks(seq: list, n: int) -> list:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


async def _probe_candidates(
    client: httpx.AsyncClient, src: str, candidates: list[dict], limit: int
) -> list[dict]:
    """批量并发探活候选曲目，返回通过的条目（附带 verified/_probe 标记）。"""
    if limit <= 0:
        return []
    passed: list[dict] = []
    async def one(it):
        res = await resolve_and_probe(client, src, it)
        if res:
            it.update(verified=True, validation_status="media_verified", completeness="unknown",
                      ext=res.get("ext") or it.get("ext") or "mp3",
                      file_size=res.get("file_size") or 0)
            _cache_put(it)
            _publish(it)
            return it
        return None

    for batch in _chunks(candidates, 6):
        tasks = [asyncio.create_task(one(it)) for it in batch]
        try:
            for done in asyncio.as_completed(tasks):
                try:
                    it = await done
                except Exception:
                    continue
                if it:
                    passed.append(it)
                    if len(passed) >= limit:
                        return passed
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    return passed


# ------------------------------------------------------------------ QQ tx ---

TX_SEARCH_BODY = {
    "req_1": {
        "method": "DoSearchForQQMusicDesktop",
        "module": "music.search.SearchCgiService",
        "param": {"search_type": 0, "query": "", "page_num": 1, "num_per_page": 20},
    }
}


async def tx_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    """QQ 音乐官方免登录搜索（musicu.fcg）。直链由用户自定义源解析后探活。"""
    fetch_size = max(limit * 2, 20)
    body = {"req_1": {**TX_SEARCH_BODY["req_1"], "param": {**TX_SEARCH_BODY["req_1"]["param"], "query": keyword, "num_per_page": fetch_size}}}
    try:
        r = await client.post(
            "https://u.y.qq.com/cgi-bin/musicu.fcg",
            json=body,
            headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/"},
        )
        data = r.json() or {}
    except Exception:  # noqa: BLE001
        raise
    songs = ((((data.get("req_1") or {}).get("data") or {}).get("body") or {}).get("song") or {}).get("list") or []
    candidates = []
    for it in songs:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        mid = str(it.get("mid") or it.get("songmid") or "")
        title = str(it.get("title") or "")
        if not mid or not title:
            continue
        if any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        singers = it.get("singer") or []
        album = it.get("album") if isinstance(it.get("album"), dict) else {}
        file_obj = it.get("file") if isinstance(it.get("file"), dict) else {}
        pay = it.get("pay") or {}
        candidates.append(
            {
                "id": f"lx:tx:{mid}",
                "lx_source": "tx",
                "title": title,
                "artist": " / ".join(str(s.get("name") or "") for s in singers if isinstance(s, dict)),
                "album": str(album.get("name") or ""),
                "duration_s": int(it.get("interval") or 0),
                "ext": "mp3",
                "cover_url": (
                    f"https://y.gtimg.cn/music/photo_new/T002R300x300M000{album.get('mid')}.jpg"
                    if album.get("mid")
                    else ""
                ),
                "file_size": 0,
                "lyric": "",
                "songmid": mid,
                "str_media_mid": str(file_obj.get("media_mid") or ""),
                "album_id": str(album.get("id") or ""),
                "pay_type": int(pay.get("pay_play") or 0),
            }
        )
    # 直链由用户自定义源解析+探活；未开启逐曲探活时直接作为未验证条目返回
    if is_probe_enabled():
        return await _probe_candidates(client, "tx", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


async def tx_resolve_lyric(client: httpx.AsyncClient, identifier: str) -> str:
    import html as _html

    try:
        r = await client.get(
            "https://c.y.qq.com/lyric/fcgi-bin/fcg_query_lyric_new.fcg",
            params={
                "songmid": identifier,
                "g_tk": "5381",
                "loginUin": "0",
                "hostUin": "0",
                "format": "json",
                "inCharset": "utf8",
                "outCharset": "utf-8",
                "notice": "0",
                "platform": "yqq",
                "needNewCode": "0",
            },
            headers={"User-Agent": UA_PC, "Referer": "https://y.qq.com/portal/player.html"},
        )
        data = _lenient_json(r, f"tx lyric {identifier}")
        content = str((data or {}).get("lyric") or "")
        if not content:
            return ""
        return _html.unescape(base64.b64decode(content).decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        return ""


# ------------------------------------------------------------------ 酷我 kw ---

def _lenient_pydict(resp: httpx.Response, tag: str = "") -> "dict | None":
    """酷我 r.s 老接口返回 Python 字面量风格（单引号），宽容解析。"""
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        pass
    import ast as _ast

    text = (resp.text or "").strip()
    if text.startswith("{"):
        try:
            return _ast.literal_eval(text)
        except Exception as e:  # noqa: BLE001
            logger.warning("%s: py-dict parse failed: %s", tag, e)
    return None


def _title_relevance(title: str, keyword: str) -> int:
    """搜索候选排序：标题与关键词越接近越靠前（r.s 相关度常把原版排在伴奏/翻唱之后）。"""
    import re as _re

    def norm(s: str) -> str:
        s = _re.sub(r"\([^)]*\)|（[^）]*）|\[[^\]]*\]", "", s or "")
        return _re.sub(r"[\s\-—·・&]", "", s).lower()

    t, k = norm(title), norm(keyword)
    if not k:
        return 3
    if t == k:
        return 0
    if k.startswith(t) or t.startswith(k):
        return 1  # 原版：title 即关键词主体（"晴天" ⊂ "晴天 周杰伦"）
    if k in t or t in k:
        return 2
    return 3


async def kw_search(client: httpx.AsyncClient, keyword: str, limit: int) -> list[dict]:
    """酷我官方免登录搜索（r.s 老接口）。直链由用户自定义源解析后探活。"""
    import html as _html

    # r.s 相关度常把原版排在伴奏/翻唱之后，抓取窗口放大再按标题相关度重排
    fetch_size = max(limit * 4, 60)
    r = await client.get(
        "http://search.kuwo.cn/r.s",
        params={
            "all": keyword,
            "ft": "music",
            "itemset": "web_2013",
            "client": "kt",
            "pn": 0,
            "rn": fetch_size,
            "rformat": "json",
            "encoding": "utf8",
        },
        headers={"User-Agent": UA_PC, "Referer": "http://www.kuwo.cn/"},
    )
    r.raise_for_status()
    raw = _lenient_pydict(r, "kw r.s") or {}
    candidates = []
    for it in raw.get("abslist") or []:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        rid = str(it.get("MUSICRID") or "").replace("MUSIC_", "").strip()
        if not rid.isdigit():
            continue
        payinfo = it.get("payInfo") or {}
        # cannotOnlinePlay=1 表示无在线播放版权，真不可播，直接剔除
        if str(payinfo.get("cannotOnlinePlay") or "0") == "1":
            continue
        title = _html.unescape(str(it.get("SONGNAME") or "")).replace("\xa0", " ").strip()
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        cover_short = str(it.get("web_albumpic_short") or "").strip()
        candidates.append(
            {
                "id": f"lx:kw:{rid}",
                "lx_source": "kw",
                "title": title,
                "artist": _html.unescape(str(it.get("ARTIST") or "")).replace("\xa0", " ").strip(),
                "album": _html.unescape(str(it.get("ALBUM") or "")).replace("\xa0", " ").strip(),
                "duration_s": int(it.get("DURATION") or 0),
                "ext": "mp3",
                "cover_url": f"https://img1.kuwo.cn/star/albumcover/{cover_short}" if cover_short else "",
                "file_size": 0,
                "lyric": "",
                "rid": rid,
                "pay_type": int(it.get("PAY") or 0),
            }
        )
    # 标题相关度排序：原版（title≈keyword）优先于伴奏/DJ/翻唱版本
    candidates.sort(key=lambda c: _title_relevance(c["title"], keyword))
    if is_probe_enabled():
        return await _probe_candidates(client, "kw", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
        _publish(it)
    return items


async def kw_resolve_lyric(client: httpx.AsyncClient, item: dict) -> str:
    # 酷我免登录歌词接口已全部失效（2026-09 实测），暂返回空
    return ""


# ------------------------------------------------------------ 免登录榜单推荐 ---

# 实测存活的免登录榜单（2026-09）：kg 移动端 TOP500 / kw kbang 飙升榜 / wy 新歌速递
_KG_RANK_ID = 8888  # m.kugou.com/rank/info 必须带 page 参数，否则 songs.list 为空
_KW_BANG_ID = 93    # kbangserver.kuwo.cn 免签老接口


async def kg_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """酷狗移动端 TOP500 榜单（免登录）。免费曲直接收录，付费曲探活通过才返回。"""
    try:
        r = await client.get(
            "http://m.kugou.com/rank/info/",
            params={"rankid": _KG_RANK_ID, "page": 1, "json": "true"},
            headers={"User-Agent": UA_MOBILE},
            timeout=10.0,
        )
        r.raise_for_status()
        rows = ((r.json() or {}).get("songs") or {}).get("list") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("kg chart rank %s failed: %s", _KG_RANK_ID, e)
        return []
    items: list[dict] = []
    vip_candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict) or _explicit_trial(it):
            continue
        fhash = str(it.get("hash") or "")
        if not fhash:
            continue
        # 榜单曲目几乎全部带 pay_type/fail_process VIP 标记，但实测匿名 playInfo
        # 仍能解析出完整时长直链（2026-09 验证 3.6MB/225s），故不按标记硬过滤，
        # 一律以探活结果为准（与搜索侧"可播性验证取代收费过滤"同一原则）
        if int(it.get("is_free_part") or 0) != 0:
            continue
        singer = " / ".join(
            str(a.get("author_name") or "")
            for a in (it.get("authors") or [])
            if isinstance(a, dict) and a.get("author_name")
        )
        title = str(it.get("songname") or it.get("filename") or "").replace(f"{singer} - ", "")
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        pay_type = int(it.get("pay_type") or 0)
        is_vip = pay_type != 0 or int(it.get("pkg_price") or 0) != 0 or int(it.get("price") or 0) != 0
        sq = str(it.get("sqhash") or "")
        item = {
            "id": f"lx:kg:{fhash}",
            "lx_source": "kg",
            "title": title,
            "artist": singer,
            "album": "",  # rank 接口不返回专辑名
            "duration_s": int(it.get("duration") or 0),  # rank 接口 duration 单位为秒
            "ext": "flac" if sq else "mp3",
            "cover_url": str(it.get("album_sizable_cover") or "").replace("{size}", "480").replace("http://", "https://"),
            "file_size": int(it.get("sqfilesize") or it.get("320filesize") or 0) or 0,
            "lyric": "",
            "hash": fhash,
            "hash_hq": str(it.get("320hash") or ""),
            "hash_sq": sq,
            "mixsongid": str(it.get("album_audio_id") or ""),
            "pay_type": pay_type,
        }
        if is_vip:
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    if vip_candidates and len(items) < limit:
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "kg", vip_candidates, limit - len(items)))
        else:
            for it in vip_candidates[:limit - len(items)]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[:limit]


async def kw_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """酷我 kbang 免签飙升榜（老接口字段较旧）。直链由用户自定义源解析，全部探活。"""
    import html as _html

    try:
        r = await client.get(
            "http://kbangserver.kuwo.cn/ksong.s",
            params={
                "from": "pc",
                "fmt": "json",
                "pn": 0,
                "rn": max(limit * 2, 40),
                "id": _KW_BANG_ID,
                "nc": 1,
            },
            headers={"User-Agent": UA_PC},
            timeout=10.0,
        )
        r.raise_for_status()
        rows = (r.json() or {}).get("musiclist") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("kw bang %s failed: %s", _KW_BANG_ID, e)
        return []
    candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        rid = str(it.get("id") or "")
        if not rid.isdigit():
            continue
        payinfo = it.get("payInfo") or {}
        # cannotOnlinePlay=1 无在线版权；listen_fragment=1 仅有试听片段，均真不可播
        if str(payinfo.get("cannotOnlinePlay") or "0") == "1":
            continue
        if str(payinfo.get("listen_fragment") or "0") == "1":
            continue
        title = _html.unescape(str(it.get("name") or "")).replace("\xa0", " ").strip()
        if not title or any(marker in title for marker in _TRIAL_TITLE_MARKERS):
            continue
        formats = str(it.get("formats") or "")
        fee_type = payinfo.get("feeType") or {}
        candidates.append(
            {
                "id": f"lx:kw:{rid}",
                "lx_source": "kw",
                "title": title,
                "artist": _html.unescape(str(it.get("artist") or "")).replace("\xa0", " ").strip(),
                "album": _html.unescape(str(it.get("album") or "")).replace("\xa0", " ").strip(),
                "duration_s": int(float(it.get("song_duration") or 0)),
                "ext": "flac" if "ALFLAC" in formats else "mp3",
                "cover_url": "",
                "file_size": 0,
                "lyric": "",
                "rid": rid,
                "pay_type": 1 if str(fee_type.get("song") or "0") != "0" else 0,
            }
        )
    if is_probe_enabled():
        return await _probe_candidates(client, "kw", candidates, limit)
    items = []
    for it in candidates[:limit]:
        it.update(verified=False, validation_status="unverified", completeness="unknown")
        _cache_put(it)
        items.append(it)
    return items


async def wy_chart_songs(client: httpx.AsyncClient, limit: int) -> list[dict]:
    """网易新歌速递（免登录）。fee∉(0,8) 的付费曲转探活验证。"""
    try:
        r = await client.get(
            "https://music.163.com/api/personalized/newsong",
            params={"limit": max(limit * 2, 30)},
            headers={
                "User-Agent": UA_PC,
                "Referer": "https://music.163.com/",
                "Cookie": "os=pc; appver=9.1.15",
            },
            timeout=10.0,
        )
        r.raise_for_status()
        rows = (r.json() or {}).get("result") or []
    except Exception as e:  # noqa: BLE001
        logger.warning("wy newsong failed: %s", e)
        return []
    items: list[dict] = []
    vip_candidates: list[dict] = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        song = it.get("song") if isinstance(it.get("song"), dict) else {}
        sid = str(song.get("id") or it.get("id") or "")
        if not sid:
            continue
        name = str(song.get("name") or it.get("name") or "")
        if not name or any(marker in name for marker in _TRIAL_TITLE_MARKERS):
            continue
        artists = song.get("artists") or []
        artist = " / ".join(str(a.get("name") or "") for a in artists if isinstance(a, dict))
        album = song.get("album") or {}
        fee = int(song.get("fee") or 0)
        item = {
            "id": f"lx:wy:{sid}",
            "lx_source": "wy",
            "title": name,
            "artist": artist,
            "album": str(album.get("name") or "") if isinstance(album, dict) else "",
            "duration_s": int(song.get("duration") or 0) / 1000.0,
            "ext": "mp3",
            "cover_url": str(album.get("picUrl") or "") if isinstance(album, dict) else "",
            "file_size": 0,
            "lyric": "",
            "song_id": sid,
            "fee": fee,
        }
        if fee not in (0, 8):
            vip_candidates.append(item)
        else:
            item.update(verified=False, validation_status="unverified", completeness="unknown")
            _cache_put(item)
            items.append(item)
            _publish(item)
    if vip_candidates and len(items) < limit:
        if is_probe_enabled():
            items.extend(await _probe_candidates(client, "wy", vip_candidates, limit - len(items)))
        else:
            for it in vip_candidates[:limit - len(items)]:
                it.update(verified=False, validation_status="unverified", completeness="unknown")
                _cache_put(it)
                items.append(it)
                _publish(it)
    return items[:limit]


_CHARTERS: dict[str, Any] = {"kg": kg_chart_songs, "kw": kw_chart_songs, "wy": wy_chart_songs}


# --------------------------------------------------------------------- app ---

_SEARCHERS = {"kg": kg_search, "wy": wy_search, "mg": mg_search, "tx": tx_search, "kw": kw_search}


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    created = False
    if getattr(fastapi_app.state, "http", None) is None:
        fastapi_app.state.http = httpx.AsyncClient(
            timeout=httpx.Timeout(CONF["search_timeout"], connect=5.0),
            follow_redirects=True,
        )
        created = True
    loader = asyncio.create_task(SOURCE_MANAGER.load())
    try:
        yield
    finally:
        loader.cancel()
        await asyncio.gather(loader, return_exceptions=True)
        await SOURCE_MANAGER.shutdown()
        if created:
            await fastapi_app.state.http.aclose()


app = FastAPI(title="fnmusic-lxmusic", version=SERVICE_VERSION, lifespan=lifespan)


def get_http(fastapi_app: FastAPI) -> httpx.AsyncClient:
    client = getattr(fastapi_app.state, "http", None)
    if client is None:
        client = httpx.AsyncClient(timeout=CONF["search_timeout"], follow_redirects=True)
        fastapi_app.state.http = client
    return client


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "service": "fnmusic-lxmusic",
        "version": SERVICE_VERSION,
        "sources": CONF["sources"],
        "user_source": SOURCE_MANAGER.describe(),
        "circuit": chain_health_snapshot(),
        "capabilities": source_capabilities(),
        "charts": sorted(_CHARTERS),
    }


def _err(msg: str, code: int = 404) -> JSONResponse:
    return JSONResponse(content={"ok": False, "error": msg}, status_code=code)


class _LxSuperseded:
    pass


_LX_SUPERSEDED = _LxSuperseded()


class LxSearchGate:
    """同时只搜一个关键词。同一范围的新词取消正在跑的平台任务；其他范围排队。"""

    def __init__(self):
        self._epoch = 0
        self._running: dict | None = None
        self._queue: list[dict] = []

    def reset(self) -> None:
        self._epoch += 1
        job = self._running
        self._running = None
        queued = self._queue
        self._queue = []
        if job and job.get("task") and not job["task"].done():
            job["task"].cancel()
        for slot in queued:
            _lx_resolve(slot, _LX_SUPERSEDED)
        if job:
            _lx_resolve(job, _LX_SUPERSEDED)

    async def run(self, scope: str, keyword: str, factory):
        fut = asyncio.get_running_loop().create_future()
        running = self._running
        if running and running["scope"] == scope and running["keyword"] == keyword and not running.get("superseded"):
            running["waiters"].append(fut)
        elif running and running["scope"] == scope and running["keyword"] != keyword:
            running["superseded"] = True
            _lx_resolve(running, _LX_SUPERSEDED)
            task = running.get("task")
            if task and not task.done():
                task.cancel()
            self._upsert(scope, keyword, factory, fut, front=True)
        else:
            self._upsert(scope, keyword, factory, fut, front=False)
        self._pump()
        return await asyncio.shield(fut)

    def _upsert(self, scope, keyword, factory, fut, front: bool) -> None:
        for slot in self._queue:
            if slot["scope"] != scope:
                continue
            if slot["keyword"] != keyword:
                _lx_resolve(slot, _LX_SUPERSEDED)
                slot["keyword"] = keyword
                slot["factory"] = factory
                slot["waiters"] = [fut]
            else:
                slot["waiters"].append(fut)
            if front:
                self._queue.remove(slot)
                self._queue.insert(0, slot)
            return
        slot = {"scope": scope, "keyword": keyword, "factory": factory, "waiters": [fut]}
        self._queue.insert(0, slot) if front else self._queue.append(slot)

    def _pump(self) -> None:
        if self._running is not None or not self._queue:
            return
        slot = self._queue.pop(0)
        job = {
            "scope": slot["scope"], "keyword": slot["keyword"], "factory": slot["factory"],
            "waiters": slot["waiters"], "superseded": False, "epoch": self._epoch, "task": None,
        }
        self._running = job
        job["task"] = asyncio.get_running_loop().create_task(self._execute(job))

    async def _execute(self, job: dict) -> None:
        result = _LX_SUPERSEDED
        try:
            result = await job["factory"]()
        except asyncio.CancelledError:
            result = _LX_SUPERSEDED
        except Exception as exc:
            result = exc
        if job.get("epoch") != self._epoch:
            _lx_resolve(job, _LX_SUPERSEDED)
            return
        if self._running is job:
            self._running = None
        if job.get("superseded") or result is _LX_SUPERSEDED:
            _lx_resolve(job, _LX_SUPERSEDED)
        elif isinstance(result, Exception):
            _lx_resolve(job, result, error=True)
        else:
            _lx_resolve(job, result)
        self._pump()


def _lx_resolve(job: dict, result, error: bool = False) -> None:
    for fut in job.get("waiters", []):
        if not fut.done():
            if error:
                fut.set_exception(result)
            else:
                fut.set_result(result)


LX_SEARCH_GATE = LxSearchGate()


@app.get("/api/v1/search")
async def search(
    keyword: str = Query("", alias="keyword"),
    q: str = Query("", alias="q"),
    limit: int = Query(0),
    sources: str = Query(""),
    probe: int | None = Query(None, alias="probe"),
    x_fnmusic_scope: str = Header(default=""),
):
    kw = (keyword or q or "").strip()
    if not kw:
        return _err("keyword required", 400)
    _STATS["searches"] += 1
    if limit <= 0:
        limit = CONF["limit_per_source"]
    wanted_raw = [s.strip() for s in (sources or "").split(",") if s.strip()]
    wanted = [normalize_source(s) for s in wanted_raw]
    wanted = [s for s in wanted if s] or CONF["sources"]
    scope = (x_fnmusic_scope or "").strip()

    async def _run():
        return await _search_platforms(kw, limit, wanted, probe=probe)

    result = await LX_SEARCH_GATE.run(scope, kw, _run)
    if result is _LX_SUPERSEDED:
        return {"ok": True, "items": [], "superseded": True, "errors": {}}
    return result


async def _search_platforms(kw: str, limit: int, wanted: list[str], *, probe: int | None = None) -> dict:
    probe_val = bool(probe) if probe is not None else None
    probe_token = _SEARCH_PROBE_ENABLED.set(probe_val)
    try:
        client = get_http(app)
        tasks = {}
        partials = {}
        errors: dict[str, str] = {}
        capabilities = source_capabilities()

        async def run_source(src):
            token = _SEARCH_PARTIAL.set(partials[src])
            try:
                return await _SEARCHERS[src](client, kw, limit)
            finally:
                _SEARCH_PARTIAL.reset(token)

        for src in dict.fromkeys(wanted):
            if src not in _SEARCHERS:
                continue
            if not capabilities[src]["playback_available"]:
                errors[src] = capabilities[src]["reason"]
                continue
            partials[src] = []
            tasks[src] = asyncio.create_task(run_source(src))
        items: list[dict] = []
        try:
            if tasks:
                # One shared budget, below the proxy's default 15s timeout. Never
                # multiply the timeout by source count or wait in insertion order.
                await asyncio.wait(tasks.values(), timeout=max(0.001, min(CONF["search_timeout"], 13.0)))
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
        for src, task in tasks.items():
            if task.cancelled():
                errors[src] = "search deadline exceeded"
                results = partials[src]
            elif task.exception() is not None:
                errors[src] = str(task.exception()) or type(task.exception()).__name__
                results = partials[src]
            else:
                results = task.result()
            seen = set()
            for item in results:
                if item.get("id") not in seen:
                    items.append(item)
                    seen.add(item.get("id"))
                    if len(seen) >= limit:
                        break
        _STATS["errors"] += len(errors)
        return {"ok": True, "items": items, "errors": errors, "stats": dict(_STATS),
                "capabilities": capabilities}
    finally:
        _SEARCH_PROBE_ENABLED.reset(probe_token)


@app.get("/api/v1/recommend")
async def recommend_charts(
    limit: int = Query(0),
    sources: str = Query(""),
):
    """免登录榜单推荐：kg TOP500 / kw 飙升榜 / wy 新歌速递，按源顺序聚合凑满 limit。"""
    if limit <= 0:
        limit = CONF["limit_per_source"]
    wanted_raw = [s.strip() for s in (sources or "").split(",") if s.strip()]
    wanted = [normalize_source(s) for s in wanted_raw]
    wanted = [s for s in wanted if s in _CHARTERS] or [s for s in CONF["sources"] if s in _CHARTERS]

    client = get_http(app)
    capabilities = source_capabilities()
    items: list[dict] = []
    errors: dict[str, str] = {}
    for src in dict.fromkeys(wanted):
        if not capabilities.get(src, {}).get("playback_available"):
            errors[src] = capabilities.get(src, {}).get("reason") or "playback unavailable"
            continue
        try:
            chunk = await _CHARTERS[src](client, limit)
        except Exception as e:  # noqa: BLE001
            errors[src] = str(e) or type(e).__name__
            continue
        items.extend(chunk)
        if len(items) >= limit:
            break
    _STATS["errors"] += len(errors)
    return {"ok": True, "items": items[:limit], "errors": errors, "charts": sorted(_CHARTERS)}


@app.get("/api/v1/track/url")
async def track_url(
    id: str = Query("", alias="id"),
    guid: str = Query("", alias="guid"),
    quality: str = Query("lossless"),
    name: str = Query("", alias="name"),
    singer: str = Query("", alias="singer"),
    title: str = Query("", alias="title"),
    artist: str = Query("", alias="artist"),
    album: str = Query("", alias="album"),
    duration: float = Query(0.0, alias="duration"),
    cover: str = Query("", alias="cover"),
):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src or not identifier:
        return _err(f"invalid track id: {track_id}", 400)
    _STATS["url_resolutions"] += 1
    canonical_id = f"lx:{src}:{identifier}"
    s_name = (name or title or "").strip()
    s_singer = (singer or artist or "").strip()
    cached = _cache_get(canonical_id) or {"id": canonical_id, "lx_source": src}
    if s_name:
        cached["title"] = s_name
        cached["name"] = s_name
    if s_singer:
        cached["artist"] = s_singer
        cached["singer"] = s_singer
    if album:
        cached["album"] = album
        cached["albumName"] = album
    if duration > 0:
        cached["duration_s"] = duration
    if cover:
        cached["cover_url"] = cover
    _cache_put(cached)
    client = get_http(app)
    try:
        result = await resolve_and_probe(
            client, src, cached, quality, budget=CONF["url_timeout"]
        )
    except Exception as e:  # noqa: BLE001
        _STATS["errors"] += 1
        logger.warning("lx url resolve %s failed: %s", track_id, e)
        return _err(f"resolve failed: {e}", 502)

    if not result:
        return _err(source_capabilities()[src]["reason"] or "no playable url", 404)
    return {"ok": True, "data": {"id": track_id, "quality": quality, **{k: v for k, v in result.items() if k != "probed"}}}


@app.get("/api/v1/track/info")
async def track_info(id: str = Query("", alias="id"), guid: str = Query("", alias="guid")):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src:
        return _err(f"invalid track id: {track_id}", 400)
    cached = _cache_get(f"lx:{src}:{identifier}")
    if cached:
        return {"ok": True, "data": cached}
    return {"ok": True, "data": {"id": track_id, "source": "lx", "lx_source": src, "title": "", "artist": "", "album": "", "duration_s": 0, "ext": "mp3", "file_size": 0, "cover_url": "", "lyric": ""}}


@app.get("/api/v1/track/lyric")
async def track_lyric(
    id: str = Query("", alias="id"),
    guid: str = Query("", alias="guid"),
    name: str = Query("", alias="name"),
    singer: str = Query("", alias="singer"),
):
    track_id = (id or guid or "").strip()
    src, identifier = parse_track_id(track_id)
    if not src:
        return _err(f"invalid track id: {track_id}", 400)
    cached = _cache_get(f"lx:{src}:{identifier}") or {}
    item = dict(cached)
    if name and not item.get("title"):
        item["title"] = name
    if singer and not item.get("artist"):
        item["artist"] = singer
    if not item.get("hash") and src == "kg":
        item["hash"] = identifier
    client = get_http(app)
    text = ""
    try:
        if src == "kg":
            text = await kg_resolve_lyric(client, item)
        elif src == "wy":
            text = await wy_resolve_lyric(client, identifier)
        elif src == "mg":
            text = await mg_resolve_lyric(client, item)
        elif src == "tx":
            text = await tx_resolve_lyric(client, identifier)
        elif src == "kw":
            text = await kw_resolve_lyric(client, item)
    except Exception as e:  # noqa: BLE001
        logger.warning("lx lyric %s failed: %s", track_id, e)
    return {"ok": True, "data": {"id": track_id, "lyric": text or ""}}


# ------------------------------------------------------------ 用户源管理端点 ---

class SourceBody(BaseModel):
    url: str = ""
    script: str = ""  # 上传场景：脚本文本（与 url 二选一；url 可为 file:// 上传地址）
    slot: int = 0  # 槽位编号（0: 主音源, 1: 备用源 1, 2: 备用源 2）
    urls: list[str] | None = None  # 批量设置槽位 URL 列表


@app.get("/api/v1/source")
async def source_info():
    return {"ok": True, "data": SOURCE_MANAGER.describe(), "circuit": chain_health_snapshot()}


@app.post("/api/v1/source/verify")
async def source_verify(body: SourceBody):
    """端到端试运行一个源 URL（不改变当前激活源）。"""
    from verify_source import verify_url  # 延迟导入：verify_source 反向 import 本模块

    url = (body.url or "").strip()
    if not is_source_url(url):
        return _err("url 必须以 http:// 、https:// 或 file:// 开头", 400)
    try:
        report = await asyncio.wait_for(verify_url(url), timeout=120.0)
    except asyncio.TimeoutError:
        return _err("校验超时（120s）", 504)
    except SourceError as exc:
        return JSONResponse(content={"ok": False, "data": {"category": exc.category, "message": str(exc)}})
    except Exception as exc:  # noqa: BLE001  校验失败必须以结构化报告返回，绝不让 WebUI 收到裸 500 文本
        return JSONResponse(content={"ok": False, "data": {"category": "internal", "message": f"校验过程出现内部错误: {exc}"}})
    return {"ok": bool(report.get("ok")), "data": report}


class UploadBody(BaseModel):
    filename: str
    script: str


@app.post("/api/v1/source/upload")
async def source_upload(body: UploadBody):
    """落盘一段上传的脚本文本（不激活）：校验头部元数据后写入 uploads/，返回 file:// URL。"""
    try:
        meta = parse_script_meta(body.script)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    try:
        path, url = save_upload(SOURCE_MANAGER.state_dir, body.filename, body.script)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    except OSError as exc:
        return JSONResponse(
            content={"ok": False, "error": f"写入上传文件失败: {exc}", "category": "download"}, status_code=500
        )
    return {"ok": True, "data": {"path": path, "url": url, "meta": meta}}


@app.post("/api/v1/source")
async def source_set(body: SourceBody):
    """校验并切换当前激活源（state.json 持久化，热生效，支持 3 槽位）。"""
    slot = body.slot if (0 <= body.slot < 3) else 0

    if body.urls is not None:
        for s_idx, u in enumerate(body.urls[:3]):
            u_str = (u or "").strip()
            if u_str:
                if is_source_url(u_str):
                    await SOURCE_MANAGER.activate(u_str, slot=s_idx)
                    _CHAIN_HEALTH.pop(f"user_source_{s_idx}", None)
            else:
                await SOURCE_MANAGER.deactivate(slot=s_idx)
        _CHAIN_HEALTH.pop("user_source", None)
        return {"ok": True, "data": SOURCE_MANAGER.describe()}

    url = (body.url or "").strip()
    if not url and body.script:
        # 直接以脚本文本激活（先落盘为上传文件，再走统一 file:// 链路）
        try:
            meta = parse_script_meta(body.script)
        except SourceError as exc:
            return JSONResponse(
                content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
            )
        try:
            cache_name = "source.js" if slot == 0 else f"source_{slot}.js"
            _path, url = save_upload(SOURCE_MANAGER.state_dir, cache_name, body.script)
        except (SourceError, OSError) as exc:
            return JSONResponse(
                content={"ok": False, "error": f"保存上传脚本失败: {exc}", "category": "download"}, status_code=500
            )
    if not is_source_url(url):
        return _err("url 必须以 http:// 、https:// 或 file:// 开头", 400)
    try:
        await SOURCE_MANAGER.activate(url, slot=slot)
    except SourceError as exc:
        return JSONResponse(
            content={"ok": False, "error": str(exc), "category": exc.category}, status_code=400
        )
    # 旧源攒下的熔断状态不应连带拦截新源：换源即清零对应槽位健康度
    _CHAIN_HEALTH.pop(f"user_source_{slot}", None)
    if slot == 0:
        _CHAIN_HEALTH.pop("user_source", None)
    return {"ok": True, "data": SOURCE_MANAGER.describe()}


@app.delete("/api/v1/source")
async def source_clear(slot: int | None = None):
    """停用当前源并清除持久化状态（支持按 slot 停用或停用全部）。"""
    if slot is not None and 0 <= slot < 3:
        await SOURCE_MANAGER.deactivate(slot=slot)
        _CHAIN_HEALTH.pop(f"user_source_{slot}", None)
        if slot == 0:
            _CHAIN_HEALTH.pop("user_source", None)
        return {"ok": True, "data": SOURCE_MANAGER.describe()}

    await SOURCE_MANAGER.shutdown()
    SOURCE_MANAGER.active_urls = ["", "", ""]
    SOURCE_MANAGER.last_errors = ["", "", ""]
    for path in [SOURCE_MANAGER.state_path, *SOURCE_MANAGER.script_caches]:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    _CHAIN_HEALTH.clear()
    return {"ok": True, "data": SOURCE_MANAGER.describe()}
