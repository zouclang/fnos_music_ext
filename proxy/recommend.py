"""推荐歌单：采信音源原生推荐生成可播放在线歌单。

两个独立歌单，各自受开关门控（见 .env FNMUSIC_RECOMMEND_*）：
  - 每日推荐（daily，FNMUSIC_RECOMMEND_DAILY）：按账户隔离，逐级补齐至 PLAYLIST_SIZE 首：
      1. netease-daily  网易真·每日推荐（musicbox）。音源每天只有一份内容，
                        当天仅第一个构建的用户可用（占用"音源名额"，见 source-slot）
      2. llm            大模型候选 + 搜索匹配（配置了 FNMUSIC_LLM_* 即启用，种子按用户）
      3. local-random   本地曲库随机兜底（只读官方库；按账户播放历史选
                        "从未听过优先、最久未听补齐"的本地现存曲目，各账户不同）
  - 热门推荐（hot，FNMUSIC_RECOMMEND_HOT）：榜单原味（不排除已收藏/最近播放）：
      1. netease-charts 网易热歌榜（musicbox toplist，免登录）
      2. lx-charts      lxmusic 免登录榜单（kg TOP500 / kw 飙升榜 / wy 新歌速递）
歌单封面取曲：第一个带可用封面直链的在线曲目；无在线封面回落第一首带官方
coverId 的本地曲目（跳过酷我文本页假链接）。
密钥只从环境变量读取，绝不写入 CONF / 日志 / 缓存。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Any
from uuid import uuid4

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore

logger = logging.getLogger("fnmusic_proxy.recommend")

DAILY_GUID_PREFIX = "online:playlist:daily:"
HOT_GUID_PREFIX = "online:playlist:hot:"
CHART_GUID_PREFIX = "online:playlist:chart:"
# 酷我文本封面 host：该“封面 URL”实为含图片链接的文本页，不能当直链用（app.py 引用同一常量）
KW_TEXT_COVER_HOST = "artistpicserver.kuwo.cn"
DEFAULT_MODEL = "gpt-4o-mini"
SEED_LIMIT = 20


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.environ.get(name, "") or default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, lo: float, hi: float) -> float:
    try:
        return max(lo, min(hi, float(os.environ.get(name, "") or default)))
    except (TypeError, ValueError):
        return default


# issue #29：预算/候选数/超时全部可配（.env 热重载白名单同步放开）。
LLM_CANDIDATE_COUNT = _env_int("FNMUSIC_RECOMMEND_CANDIDATES", 36, 10, 60)
PLAYLIST_SIZE = 20
RECOMMEND_COUNT = PLAYLIST_SIZE  # 兼容旧引用
LLM_TIMEOUT_S = _env_float("FNMUSIC_LLM_TIMEOUT_S", 20.0, 5.0, 60.0)
BUILD_BUDGET_S = _env_float("FNMUSIC_RECOMMEND_BUDGET_S", 40.0, 10.0, 180.0)
# local-random 层候选池读取量：排除集（最近播放/收藏）在 SQL 后过滤，
# 池子放大几倍避免排除后不足 20 首；本地曲库本身很小时按库存返回
LOCAL_RANDOM_POOL = 200
# issue #23：推荐曲目逐首可播校验（解析直链 + 头部探活），默认开
VERIFY_PLAYABLE = (os.environ.get("FNMUSIC_RECOMMEND_VERIFY_PLAYABLE", "true").strip().lower()
                   in ("true", "1", "yes"))
VERIFY_TIMEOUT_S = _env_float("FNMUSIC_RECOMMEND_VERIFY_TIMEOUT_S", 8.0, 2.0, 20.0)
# 音源原生推荐抓取量（过滤已收藏/最近播放与不可播曲目后仍有余量）
NETEASE_DAILY_LIMIT = 40
NETEASE_TOPLIST_INDEX = 3  # 网易热歌榜
CHART_FETCH_COUNT = 40
RECOMMEND_SEARCH_CONCURRENCY = _env_int("FNMUSIC_REC_SEARCH_CONCURRENCY", 2, 1, 6)
RECOMMEND_SEARCH_INTERVAL = _env_float("FNMUSIC_REC_SEARCH_INTERVAL", 0.15, 0.0, 5.0)


# ---- 运行期可调读取（app.py 的 .env 热重载只更新 os.environ，模块常量不会跟着变，
# 所以推荐链路内部一律走这些 getter，WebUI/.env 改完立即生效无需重启） ----

def build_budget_s() -> float:
    return _env_float("FNMUSIC_RECOMMEND_BUDGET_S", BUILD_BUDGET_S, 10.0, 180.0)


def llm_candidate_count() -> int:
    return _env_int("FNMUSIC_RECOMMEND_CANDIDATES", LLM_CANDIDATE_COUNT, 10, 60)


def llm_timeout_s() -> float:
    return _env_float("FNMUSIC_LLM_TIMEOUT_S", LLM_TIMEOUT_S, 5.0, 60.0)


def verify_playable_enabled() -> bool:
    return (os.environ.get("FNMUSIC_RECOMMEND_VERIFY_PLAYABLE", "true").strip().lower()
            in ("true", "1", "yes"))


def verify_timeout_s() -> float:
    return _env_float("FNMUSIC_RECOMMEND_VERIFY_TIMEOUT_S", VERIFY_TIMEOUT_S, 2.0, 20.0)


def search_concurrency() -> int:
    # issue #29 反馈者实测的参数：同样放开为运行期读取，.env 热重载立即生效
    return _env_int("FNMUSIC_REC_SEARCH_CONCURRENCY", RECOMMEND_SEARCH_CONCURRENCY, 1, 6)


def search_interval_s() -> float:
    return _env_float("FNMUSIC_REC_SEARCH_INTERVAL", RECOMMEND_SEARCH_INTERVAL, 0.0, 5.0)

_CJK = re.compile(r"[\u4e00-\u9fff]")
_HIRA_KATA = re.compile(r"[\u3040-\u30ff]")
_HANGUL = re.compile(r"[\uac00-\ud7af]")
_CYRILLIC = re.compile(r"[\u0400-\u04ff]")
_LATIN = re.compile(r"[A-Za-z]")
_JSON_BLOCK = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_JSON_ARRAY = re.compile(r"\[[\s\S]*\]")


def home_dir() -> str:
    env = (os.environ.get("FNMUSIC_HOME") or "").strip()
    if env:
        return env
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def llm_api_key() -> str:
    return (os.environ.get("FNMUSIC_LLM_API_KEY") or "").strip()


def llm_base_url() -> str:
    return (os.environ.get("FNMUSIC_LLM_BASE_URL") or "").strip().rstrip("/")


def llm_model() -> str:
    return (os.environ.get("FNMUSIC_LLM_MODEL") or DEFAULT_MODEL).strip() or DEFAULT_MODEL


def llm_enabled() -> bool:
    return bool(llm_base_url() and llm_api_key())


def recommend_cache_dir() -> str:
    return os.environ.get("FNMUSIC_RECOMMEND_DIR") or os.path.join(home_dir(), "recommend_cache")


def play_history_dir() -> str:
    return os.environ.get("FNMUSIC_PLAY_HISTORY_DIR") or os.path.join(home_dir(), "play_history")


def music_db_path() -> str:
    return os.environ.get(
        "FNMUSIC_MUSIC_DB", "/usr/local/apps/@appdata/trim.music/db/music.db"
    )


def today_key(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d")


def recommend_playlist_guid(kind: str, day: str | None = None, user_guid: str = "") -> str:
    prefix = HOT_GUID_PREFIX if kind == "hot" else DAILY_GUID_PREFIX
    day = day or today_key()
    suffix = re.sub(r"[^A-Za-z0-9]", "", user_guid)[:12]
    if suffix:
        return f"{prefix}{day}:{suffix}"
    return f"{prefix}{day}"


def daily_playlist_guid(day: str | None = None, user_guid: str = "") -> str:
    return recommend_playlist_guid("daily", day, user_guid)


def hot_playlist_guid(day: str | None = None, user_guid: str = "") -> str:
    return recommend_playlist_guid("hot", day, user_guid)


def online_playlist_kind(guid: str | None) -> str:
    """解析推荐歌单 guid 的类型：daily / hot / chart，非推荐歌单返回空串。"""
    s = str(guid or "")
    if s.startswith(DAILY_GUID_PREFIX):
        return "daily"
    if s.startswith(HOT_GUID_PREFIX):
        return "hot"
    if s.startswith(CHART_GUID_PREFIX):
        return "chart"
    return ""


def is_recommend_playlist_guid(guid: str | None) -> bool:
    return online_playlist_kind(guid) != ""


def is_daily_playlist_guid(guid: str | None) -> bool:
    return str(guid or "").startswith(DAILY_GUID_PREFIX)


def pick_playlist_cover_track(tracks: list[dict] | None) -> dict | None:
    """歌单封面取曲：优先第一个带可用封面直链的在线曲目；
    无在线封面时优先取第一首带真实官方 cover_guid 的本地曲目（coverId != guid）；
    仍无时才回落普通本地曲目（由 static_cover 端点进行云端刮削兜底）。
    """
    local_with_cover = None
    local_fallback = None
    for t in tracks or []:
        if not isinstance(t, dict):
            continue
        url = str(t.get("cover_url") or "")
        if url and KW_TEXT_COVER_HOST not in url:
            return t
        cover_id = str(t.get("coverId") or "")
        if cover_id and not cover_id.startswith("online:"):
            guid = str(t.get("guid") or "")
            if cover_id != guid:
                if local_with_cover is None:
                    local_with_cover = t
            elif local_fallback is None:
                local_fallback = t
    return local_with_cover or local_fallback


def infer_language(title: str = "", artist: str = "", album: str = "") -> str:
    text = f"{title} {artist} {album}"
    if _HIRA_KATA.search(text):
        return "日语"
    if _HANGUL.search(text):
        return "韩语"
    if _CYRILLIC.search(text):
        return "俄语"
    if _CJK.search(text):
        return "中文"
    if _LATIN.search(text):
        return "英语"
    return "未知"


def _safe_user_name(user_guid: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9\-_]", "_", str(user_guid or "").strip())
    return safe or "shared"


def _atomic_write_json(path: str, payload: Any) -> bool:
    parent = os.path.dirname(path) or "."
    part = f"{path}.{uuid4().hex[:8]}.part"
    try:
        os.makedirs(parent, exist_ok=True)
        with open(part, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(part, path)
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.warning("failed to write %s: %s", path, e)
        if os.path.exists(part):
            try:
                os.remove(part)
            except Exception:
                pass
        return False


def load_online_play_history(user_guid: str) -> list[dict]:
    path = os.path.join(play_history_dir(), f"{_safe_user_name(user_guid)}.json")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    except Exception as e:
        logger.warning("failed to load play history for %s: %s", user_guid, e)
    return []


def save_online_play_history(user_guid: str, items: list[dict]) -> bool:
    path = os.path.join(play_history_dir(), f"{_safe_user_name(user_guid)}.json")
    return _atomic_write_json(path, {"items": items[-500:]})


def record_online_play(user_guid: str, guid: str, track: dict | None = None) -> None:
    if not guid:
        return
    now = int(time.time())
    items = load_online_play_history(user_guid)
    items = [it for it in items if it.get("guid") != guid]
    snapshot = dict(track or {})
    snapshot.setdefault("guid", guid)
    items.append({"guid": guid, "playedAt": now, "track": snapshot})
    save_online_play_history(user_guid, items)


def remove_online_play(user_guid: str, guid: str) -> bool:
    """从在线播放历史删除一条（guid 精确匹配）。返回是否有条目被删除。"""
    if not guid:
        return False
    items = load_online_play_history(user_guid)
    kept = [it for it in items if it.get("guid") != guid]
    if len(kept) == len(items):
        return False
    return save_online_play_history(user_guid, kept)


def user_id_from_guid(db_path: str, user_guid: str) -> int | None:
    if not db_path or not os.path.exists(db_path) or not user_guid:
        return None
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            row = con.execute("SELECT id FROM user WHERE guid = ? LIMIT 1", (user_guid,)).fetchone()
            return int(row[0]) if row else None
        finally:
            con.close()
    except Exception as e:
        logger.warning("failed to map user guid: %s", e)
        return None


def read_local_recent_tracks(db_path: str, user_guid: str, limit: int = SEED_LIMIT) -> list[dict]:
    """只读飞牛 play_history，不写官方库。"""
    uid = user_id_from_guid(db_path, user_guid)
    if uid is None:
        return []
    sql = """
        SELECT t.guid, t.title, t.year,
               ph.play_count, ph.updated_at,
               (SELECT GROUP_CONCAT(a.name, '/')
                  FROM track_artist ta JOIN artist a ON a.id = ta.artist_id
                 WHERE ta.track_id = t.id) AS artists,
               (SELECT GROUP_CONCAT(g.name, '/')
                  FROM track_genre tg JOIN genre g ON g.id = tg.genre_id
                 WHERE tg.track_id = t.id) AS genres,
               al.name AS album
          FROM play_history ph
          JOIN track t ON t.id = ph.track_id
          LEFT JOIN album al ON al.id = t.album_id
         WHERE ph.user_id = ?
         ORDER BY ph.updated_at DESC
         LIMIT ?
    """
    out: list[dict] = []
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            rows = con.execute(sql, (uid, limit)).fetchall()
        finally:
            con.close()
    except Exception as e:
        logger.warning("failed to read local play_history: %s", e)
        return []
    for guid, title, year, play_count, updated_at, artists, genres, album in rows:
        artist = str(artists or "")
        title_s = str(title or "")
        album_s = str(album or "")
        genre_s = str(genres or "")
        out.append({
            "guid": str(guid or ""),
            "title": title_s,
            "artist": artist,
            "album": album_s,
            "genre": genre_s,
            "year": year,
            "language": infer_language(title_s, artist, album_s),
            "play_count": play_count or 1,
            "playedAt": _coerce_ts(updated_at),
            "source": "local",
        })
    return out


def read_local_favorite_tracks(db_path: str, user_guid: str, limit: int = 200) -> list[dict]:
    """只读飞牛 favorite_track，不写官方库。"""
    uid = user_id_from_guid(db_path, user_guid)
    if uid is None:
        return []
    sql = """
        SELECT t.guid, t.title,
               (SELECT GROUP_CONCAT(a.name, '/')
                  FROM track_artist ta JOIN artist a ON a.id = ta.artist_id
                 WHERE ta.track_id = t.id) AS artists,
               al.name AS album
          FROM favorite_track ft
          JOIN track t ON t.id = ft.track_id
          LEFT JOIN album al ON al.id = t.album_id
         WHERE ft.user_id = ?
         ORDER BY ft.updated_at DESC
         LIMIT ?
    """
    out: list[dict] = []
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            rows = con.execute(sql, (uid, limit)).fetchall()
        finally:
            con.close()
    except Exception as e:
        logger.warning("failed to read local favorite_track: %s", e)
        return []
    for guid, title, artists, album in rows:
        title_s = str(title or "")
        artist = str(artists or "")
        album_s = str(album or "")
        out.append({
            "guid": str(guid or ""),
            "title": title_s,
            "artist": artist,
            "album": album_s,
            "source": "favorite-local",
        })
    return out


def read_local_random_tracks(
    db_path: str,
    user_guid: str,
    limit: int = PLAYLIST_SIZE,
    exclude_guids: set[str] | None = None,
    exclude_ta: set[tuple[str, str]] | None = None,
) -> list[dict]:
    """本地曲库随机选曲（只读官方库，不写）。每日推荐 local-random 层数据源。

    选取口径：
      - 仅选本地文件真实存在的曲目（track/audio_file 三级软删除标记全为 0）
      - 按账户自己的 play_history 判定"最近没有听过"：
        从未听过优先随机，不足再按"最久未听"随机补齐（各账户天然不同）
      - 排除集（最近播放/收藏 guid 与 歌名+歌手 身份键）在 SQL 后过滤，
        候选池放大到 LOCAL_RANDOM_POOL 吸收排除损耗
    返回的行是 build_local_track 的输入形状（含 audio_file/artist/album 明细）。
    """
    uid = user_id_from_guid(db_path, user_guid)
    if uid is None:
        return []
    sql = """
        SELECT t.guid, t.title, t.year, t.disc_no, t.track_no, t.isrc,
               t.duration_ms, t.is_cue, t.cover_guid,
               af.path, af.suffix, af.size, af.bitrate, af.sample_rate,
               af.bit_depth, af.channel, af.container, af.codec,
               (SELECT GROUP_CONCAT(a.name, '/')
                  FROM track_artist ta JOIN artist a ON a.id = ta.artist_id
                 WHERE ta.track_id = t.id) AS artists,
               (SELECT GROUP_CONCAT(a.guid, '/')
                  FROM track_artist ta JOIN artist a ON a.id = ta.artist_id
                 WHERE ta.track_id = t.id) AS artist_guids,
               al.guid AS album_guid, al.name AS album, al.release_date,
               ph.updated_at AS last_played_at
          FROM track t
          JOIN audio_file af ON af.id = t.audio_file_id
          LEFT JOIN album al ON al.id = t.album_id
          LEFT JOIN play_history ph ON ph.track_id = t.id AND ph.user_id = ?
         WHERE t.is_audio_file_deleted = 0 AND t.is_admin_deleted = 0
           AND af.is_physical_file_deleted = 0
         ORDER BY (ph.id IS NULL) DESC, ph.updated_at ASC, RANDOM()
         LIMIT ?
    """
    try:
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            rows = con.execute(sql, (uid, LOCAL_RANDOM_POOL)).fetchall()
        finally:
            con.close()
    except Exception as e:
        logger.warning("failed to read local random tracks: %s", e)
        return []
    skip_ids = set(exclude_guids or ())
    skip_ta = set(exclude_ta or ())
    out: list[dict] = []
    for (guid, title, year, disc_no, track_no, isrc, duration_ms, is_cue, cover_guid,
         af_path, suffix, size, bitrate, sample_rate, bit_depth, channel, container, codec,
         artists, artist_guids, album_guid, album, release_date, last_played_at) in rows:
        guid_s = str(guid or "")
        title_s = str(title or "")
        artist_s = str(artists or "")
        if not guid_s:
            continue
        if guid_s in skip_ids:
            continue
        key = identity_key(title_s, artist_s)
        if key != ("", "") and key in skip_ta:
            continue
        out.append({
            "guid": guid_s,
            "title": title_s,
            "artist": artist_s,
            "artist_guids": str(artist_guids or ""),
            "album": str(album or ""),
            "album_guid": str(album_guid or ""),
            "release_date": str(release_date or ""),
            "year": year,
            "disc_no": disc_no,
            "track_no": track_no,
            "isrc": str(isrc or ""),
            "duration_ms": int(duration_ms or 0),
            "is_cue": bool(is_cue),
            "cover_guid": str(cover_guid or ""),
            "path": str(af_path or ""),
            "suffix": str(suffix or ""),
            "size": int(size or 0),
            "bitrate": int(bitrate or 0),
            "sample_rate": int(sample_rate or 0),
            "bit_depth": int(bit_depth or 0) if bit_depth else 0,
            "channel": int(channel or 0),
            "container": str(container or ""),
            "codec": str(codec or ""),
            "last_played_at": str(last_played_at or ""),
            "source": "local",
        })
        if len(out) >= limit:
            break
    return out


def build_local_track(row: dict) -> dict:
    """官方库曲目行（read_local_random_tracks 输出）-> 客户端官方形态 track 对象。

    本地曲目 guid 是真实官方 32-hex：下发链路（disguise_client_json /
    resolve_real_guid / 播放端点）对非 online: guid 原样穿透，播放与封面走
    官方原生链路。字段形状对齐 app.py build_favorite_track_obj 的官方严格
    形状（App 反序列化严格：artists/album.name 永不为空、artist.coverId 为
    null、album 无 artists 键），audioSpec 用 DB 真实值。
    """
    guid = str(row.get("guid") or "")
    ts = int(time.time())
    title = str(row.get("title") or "").strip()
    artist_names = [n.strip() for n in str(row.get("artist") or "").split("/") if n.strip()]
    artist_guids = [g.strip() for g in str(row.get("artist_guids") or "").split("/") if g.strip()]
    artists_list = []
    for i, name in enumerate(artist_names or ["未知艺术家"]):
        artists_list.append({
            "guid": artist_guids[i] if i < len(artist_guids) else f"{guid}:artist",
            "name": name,
            "coverId": None,
            "createdAt": ts,
            "updatedAt": ts,
        })
    album_name = str(row.get("album") or "").strip() or "未知专辑"
    cover_guid = str(row.get("cover_guid") or "")
    duration_ms = int(row.get("duration_ms") or 0)
    suffix = str(row.get("suffix") or "").lstrip(".").lower()
    fmt = suffix or "mp3"
    size = int(row.get("size") or 0)
    bitrate = int(row.get("bitrate") or 0)
    sample_rate = int(row.get("sample_rate") or 0)
    bit_depth = int(row.get("bit_depth") or 0)
    channel = int(row.get("channel") or 0)
    codec = str(row.get("codec") or "").strip() or fmt
    container = str(row.get("container") or "").strip() or fmt
    audio_spec = {
        "path": str(row.get("path") or ""),
        "format": fmt,
        "codec": codec,
        "container": container,
        "duration": duration_ms,
        "size": size,
        "channel": channel or 2,
        "sampleRate": sample_rate or 44100,
        "bitDepth": bit_depth if bit_depth else (16 if fmt in ("wav", "flac", "aiff") else None),
        "bitrate": bitrate or (1411000 if fmt in ("flac", "wav", "ape", "wv") else 320000),
    }
    audio_spec = {k: v for k, v in audio_spec.items() if v is not None}
    release_date = str(row.get("release_date") or "").strip()
    year = row.get("year")
    return {
        "guid": guid,
        "id": guid,
        "title": title,
        "name": title,
        "artist": "/".join(a["name"] for a in artists_list),
        "artists": artists_list,
        "album": {
            "guid": str(row.get("album_guid") or "") or f"{guid}:album",
            "name": album_name,
            "coverId": cover_guid or guid,
            "releaseDate": release_date or None,
            "barcode": None,
            "createdAt": ts,
            "updatedAt": ts,
        },
        "albumName": album_name,
        "audioSpec": audio_spec,
        "duration": duration_ms,
        "duration_ms": duration_ms,
        "durationMs": duration_ms,
        "duration_s": duration_ms / 1000.0,
        "codec": fmt,
        "codecName": fmt,
        "format": fmt,
        "ext": fmt,
        "size": size,
        "file_size": size,
        "coverId": cover_guid or guid,
        "cover_url": "",
        "source": "local",
        "is_local": True,
        "isFavorite": False,
        "isCue": bool(row.get("is_cue")),
        "hasLyric": True,
        "genres": [],
        "year": year,
        "discNo": row.get("disc_no"),
        "trackNo": row.get("track_no"),
        "isrc": str(row.get("isrc") or ""),
        "accessStatus": 0,
        "recommendDimension": "library",
        "recommendReason": "最近没有听过的本地曲目",
    }


def _item_title_artist(item: dict) -> tuple[str, str]:
    track = item.get("track") if isinstance(item.get("track"), dict) else item
    title = str(track.get("title") or track.get("name") or "").strip()
    artist = str(track.get("artist") or "").strip()
    if not artist:
        artists = track.get("artists")
        if isinstance(artists, list) and artists:
            first = artists[0]
            artist = str(first.get("name") if isinstance(first, dict) else first).strip()
    return title, artist


def identity_key(title: str, artist: str) -> tuple[str, str]:
    return (title.strip().lower(), artist.strip().lower())


def collect_exclude_sets(*groups: list[dict] | None) -> tuple[set[str], set[tuple[str, str]]]:
    guids: set[str] = set()
    tas: set[tuple[str, str]] = set()
    for group in groups:
        for it in group or []:
            if not isinstance(it, dict):
                continue
            guid = str(it.get("guid") or "")
            if guid:
                guids.add(guid)
            nested = it.get("track") if isinstance(it.get("track"), dict) else None
            if nested:
                ng = str(nested.get("guid") or "")
                if ng:
                    guids.add(ng)
            title, artist = _item_title_artist(it)
            key = identity_key(title, artist)
            if key != ("", ""):
                tas.add(key)
    return guids, tas


def _coerce_ts(value: Any) -> int:
    if isinstance(value, (int, float)):
        n = int(value)
        return n // 1000 if n > 10_000_000_000 else n
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(float(text))
    except (TypeError, ValueError):
        pass
    try:
        normalized = text.replace("Z", "+00:00")
        if re.search(r"[+-]\d{2}:\d{2}$", normalized):
            pass
        elif re.search(r"[+-]\d{4}$", normalized):
            normalized = normalized[:-2] + ":" + normalized[-2:]
        return int(datetime.fromisoformat(normalized).timestamp())
    except Exception:
        return 0


def seeds_from_online_history(user_guid: str) -> list[dict]:
    items = load_online_play_history(user_guid)
    out: list[dict] = []
    for it in reversed(items):
        track = it.get("track") if isinstance(it.get("track"), dict) else {}
        title = str(track.get("title") or "")
        artist = str(track.get("artist") or "")
        if not artist:
            artists = track.get("artists")
            if isinstance(artists, list) and artists:
                first = artists[0]
                artist = str(first.get("name") if isinstance(first, dict) else first)
        album = str(track.get("albumName") or "")
        if not album and isinstance(track.get("album"), dict):
            album = str(track["album"].get("name") or "")
        genres = track.get("genres") if isinstance(track.get("genres"), list) else []
        genre = "/".join(str(g) for g in genres if g)
        out.append({
            "guid": str(it.get("guid") or track.get("guid") or ""),
            "title": title,
            "artist": artist,
            "album": album,
            "genre": genre,
            "language": infer_language(title, artist, album),
            "playedAt": int(it.get("playedAt") or 0),
            "source": "online",
        })
    return out


def merge_recent_seeds(
    local_seeds: list[dict],
    online_seeds: list[dict],
    extra: list[dict] | None = None,
    limit: int = SEED_LIMIT,
) -> list[dict]:
    merged = list(online_seeds) + list(local_seeds) + list(extra or [])
    merged.sort(key=lambda x: int(x.get("playedAt") or 0), reverse=True)
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for it in merged:
        key = (
            str(it.get("title") or "").strip().lower(),
            str(it.get("artist") or "").strip().lower(),
        )
        if key == ("", ""):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
        if len(out) >= limit:
            break
    return out


def build_llm_prompt(
    play_seeds: list[dict],
    favorite_seeds: list[dict],
    count: int = LLM_CANDIDATE_COUNT,
) -> str:
    def _block(items: list[dict], empty: str) -> str:
        lines = []
        for i, s in enumerate(items, 1):
            lines.append(
                f"{i}. 《{s.get('title') or ''}》 / {s.get('artist') or '未知'} / "
                f"专辑:{s.get('album') or '-'} / "
                f"语言:{s.get('language') or infer_language(str(s.get('title') or ''), str(s.get('artist') or ''))} / "
                f"曲风:{s.get('genre') or '-'} / 来源:{s.get('source') or '-'}"
            )
        return "\n".join(lines) if lines else empty

    history_block = _block(play_seeds, "（暂无最近播放）")
    fav_block = _block(favorite_seeds, "（暂无收藏）")
    return f"""你是音乐推荐引擎。根据用户最近收听和收藏口味，生成 {count} 首可在线检索的「每日推荐」候选。

必须同时覆盖这些维度（每首标注 dimension）：
- artist：同歌手或高度相关合作歌手的其他作品
- language：同一语种的其他歌手
- genre：相同或相邻曲风（流行/摇滚/民谣/电子/古风/R&B 等）
- type：歌曲类型气质（抒情、热血、怀旧、舞曲、ACG 等）

约束：
1. 不要推荐用户已经收藏或最近听过的「歌名+歌手」组合。
2. 不要推荐仅存在于用户本地曲库、网上很难搜到的冷门私货。
3. 结果要多样：四个 dimension 都要出现，避免全是同一歌手。
4. 必须正好输出 {count} 首。
5. 只输出 JSON 数组，不要 markdown，不要解释。
6. 每项字段：title, artist, album, language, genre, type, dimension, reason。

最近收听：
{history_block}

收藏：
{fav_block}
"""


def parse_llm_recommendations(text: str) -> list[dict]:
    raw = (text or "").strip()
    if not raw:
        return []
    candidates: list[str] = []
    fenced = _JSON_BLOCK.search(raw)
    if fenced:
        candidates.append(fenced.group(1).strip())
    arr = _JSON_ARRAY.search(raw)
    if arr:
        candidates.append(arr.group(0))
    candidates.append(raw)
    data = None
    for c in candidates:
        try:
            data = json.loads(c)
            break
        except Exception:
            continue
    if isinstance(data, dict):
        for key in ("recommendations", "songs", "tracks", "items", "data"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
    if not isinstance(data, list):
        return []
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for it in data:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or it.get("name") or it.get("song") or "").strip()
        artist = str(it.get("artist") or it.get("singer") or it.get("singers") or "").strip()
        if not title:
            continue
        key = (title.lower(), artist.lower())
        if key in seen:
            continue
        seen.add(key)
        dim = str(it.get("dimension") or "").strip().lower()
        if dim not in ("artist", "language", "genre", "type"):
            dim = "genre"
        out.append({
            "title": title,
            "artist": artist,
            "album": str(it.get("album") or "").strip(),
            "language": str(it.get("language") or infer_language(title, artist)).strip(),
            "genre": str(it.get("genre") or "").strip(),
            "type": str(it.get("type") or "").strip(),
            "dimension": dim,
            "reason": str(it.get("reason") or "").strip(),
        })
    return out


async def call_llm(http_client: httpx.AsyncClient, prompt: str) -> list[dict]:
    base = llm_base_url()
    key = llm_api_key()
    if not base or not key:
        return []
    url = base if base.endswith("/chat/completions") else f"{base}/chat/completions"
    payload = {
        "model": llm_model(),
        "temperature": 0.8,
        "max_tokens": 4096,
        "messages": [
            {"role": "system", "content": "你只输出合法 JSON 数组，不要 markdown。"},
            {"role": "user", "content": prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    try:
        resp = await http_client.post(url, json=payload, headers=headers, timeout=llm_timeout_s())
        if resp.status_code >= 400:
            logger.warning("llm http %s", resp.status_code)
            return []
        body = resp.json()
        content = ""
        if isinstance(body, dict):
            choices = body.get("choices") or []
            if choices and isinstance(choices[0], dict):
                msg = choices[0].get("message") or {}
                content = str(msg.get("content") or "")
        return parse_llm_recommendations(content)
    except Exception as e:
        logger.warning("llm call failed: %s", e)
        return []


# ------------------------------------------------------- 音源原生推荐（平台 ID 直连） ---

def _musicbox_recommend_item(it: dict) -> dict:
    """musicbox 推荐/榜单详情行（batch_song_details 已过滤可播）-> 统一候选条目。"""
    sid = str(it.get("song_id") or it.get("id") or "")
    if not sid:
        return {}
    dur_ms = int(it.get("duration_ms") or 0)
    return {
        "id": f"netease:{sid}",
        "source": "netease",
        "title": str(it.get("name") or it.get("song_name") or ""),
        "artist": str(it.get("artist") or ""),
        "album": str(it.get("album_name") or it.get("album") or ""),
        "duration_s": dur_ms / 1000.0,
        "ext": "flac" if (it.get("has_sq") or it.get("has_hr")) else "mp3",
        "cover_url": str(it.get("album_pic_url") or ""),
    }


def _lx_recommend_item(it: dict) -> dict:
    """lxmusic 榜单条目 -> 统一候选条目（与 _search_keyword 的 lx 归一化一致）。"""
    tid = str(it.get("id") or "")
    if not tid:
        return {}
    cov = str(it.get("cover_url") or "")
    if cov.startswith("http://"):
        cov = "https://" + cov[len("http://"):]
    return {
        "id": tid if tid.startswith("lx:") else f"lx:{tid}",
        "source": "lx",
        "title": str(it.get("title") or it.get("name") or ""),
        "artist": str(it.get("artist") or ""),
        "album": str(it.get("album") or ""),
        "duration_s": float(it.get("duration_s") or 0) or 0,
        "ext": str(it.get("ext") or "mp3") or "mp3",
        "cover_url": cov,
    }


def resolve_source_candidates(
    items: list[dict],
    build_track,
    limit: int = PLAYLIST_SIZE,
    exclude_guids: set[str] | None = None,
    exclude_ta: set[tuple[str, str]] | None = None,
) -> list[dict]:
    """音源原生推荐条目（已含平台 ID，无需搜索匹配）直转 Track，跳过排除项凑满 limit 首。"""
    skip_ids = set(exclude_guids or ())
    skip_ta = set(exclude_ta or ())
    out: list[dict] = []
    for pick in items:
        if len(out) >= limit:
            break
        if not isinstance(pick, dict):
            continue
        pid = str(pick.get("id") or "")
        guid = f"online:{pid}" if pid and not pid.startswith("online:") else pid
        ptitle = str(pick.get("title") or pick.get("name") or "")
        partist = str(pick.get("artist") or "")
        if (guid and guid in skip_ids) or (pid and pid in skip_ids):
            continue
        key = identity_key(ptitle, partist)
        if key != ("", "") and key in skip_ta:
            continue
        track = build_track(pick)
        tg = str(track.get("guid") or guid)
        tt, ta = str(track.get("title") or ptitle), str(track.get("artist") or partist)
        if tg and tg in skip_ids:
            continue
        tkey = identity_key(tt, ta)
        if tkey != ("", "") and tkey in skip_ta:
            continue
        track["recommendDimension"] = str(pick.get("dimension") or "")
        track["recommendReason"] = str(pick.get("reason") or "")
        out.append(track)
    return out


async def fetch_musicbox_recommend(
    client: httpx.AsyncClient, path: str, params: dict
) -> list[dict]:
    """调 musicbox 推荐端点，返回标准化候选；服务不可用/未登录/失败一律返回 []（跳级）。"""
    try:
        r = await client.get(path, params=params, timeout=15.0)
        if r.status_code != 200:
            logger.debug("musicbox %s http %s", path, r.status_code)
            return []
        data = r.json()
    except Exception as e:
        logger.debug("musicbox %s failed: %s", path, e)
        return []
    if not (isinstance(data, dict) and data.get("ok") is True):
        # 未登录/CLI 错误等结构化错误 -> 直接跳级
        logger.debug("musicbox %s not ok: %.120s", path, data)
        return []
    rows = data.get("data")
    if not isinstance(rows, list):
        return []
    out: list[dict] = []
    for it in rows:
        if isinstance(it, dict):
            item = _musicbox_recommend_item(it)
            if item:
                out.append(item)
    return out


async def fetch_lx_charts(client: httpx.AsyncClient, limit: int, sources: "list[str] | None" = None) -> list[dict]:
    """调 lxmusic 免登录榜单端点，返回标准化候选。

    sources：lx 平台白名单（None=不限制）。榜单仅支持 kg/wy/kw，
    与白名单交集为空时直接跳过 lx 榜单（不回退到全部平台）。
    """
    chart_sources: list[str] | None = None
    if sources is not None:
        chart_sources = [s for s in sources if s in ("kg", "wy", "kw")]
        if not chart_sources:
            return []
    params: dict = {"limit": limit}
    if chart_sources:
        params["sources"] = ",".join(chart_sources)
    try:
        r = await client.get("/api/v1/recommend", params=params, timeout=20.0)
        if r.status_code != 200:
            logger.debug("lx charts http %s", r.status_code)
            return []
        data = r.json()
    except Exception as e:
        logger.debug("lx charts failed: %s", e)
        return []
    rows = data.get("items") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return []
    out: list[dict] = []
    for it in rows:
        if isinstance(it, dict):
            item = _lx_recommend_item(it)
            if item:
                out.append(item)
    return out


def _match_score(item: dict, title: str, artist: str) -> int:
    it = str(item.get("title") or item.get("name") or "").strip().lower()
    ia = str(item.get("artist") or "").strip().lower()
    t = title.strip().lower()
    a = artist.strip().lower()
    score = 0
    if t and it == t:
        score += 12
    elif t and (t in it or it in t):
        score += 6
    if a and ia == a:
        score += 10
    elif a and (a in ia or ia in a):
        score += 5
    return score


async def _search_keyword(
    keyword: str,
    musicdl_client: httpx.AsyncClient | None,
    musicbox_client: httpx.AsyncClient | None,
    netease_enabled: bool,
    lx_client: httpx.AsyncClient | None = None,
    lx_enabled: bool = False,
    lx_sources: "list[str] | None" = None,
) -> list[dict]:
    async def _mb() -> list[dict]:
        if not (netease_enabled and musicbox_client):
            return []
        try:
            r = await musicbox_client.get(
                "/api/v1/search",
                params={"keyword": keyword, "limit": 5, "type": "song"},
                timeout=8.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            rows = data.get("data") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                return []
            out: list[dict] = []
            for it in rows:
                if not isinstance(it, dict):
                    continue
                sid = str(it.get("song_id") or it.get("id") or "")
                if not sid:
                    continue
                quality = str(it.get("quality") or "").upper()
                dur = float(it.get("duration") or 0) or 0
                if dur > 10_000:
                    dur = dur / 1000.0
                out.append({
                    "id": f"netease:{sid}",
                    "source": "netease",
                    "title": str(it.get("song_name") or it.get("title") or ""),
                    "artist": str(it.get("artist") or ""),
                    "album": str(it.get("album_name") or it.get("album") or ""),
                    "duration_s": dur,
                    "ext": "flac" if any(q in quality for q in ("SQ", "HR", "无损")) else "mp3",
                })
            return out
        except Exception as e:
            logger.debug("musicbox recommend search failed: %s", e)
            return []

    async def _mdl() -> list[dict]:
        if not musicdl_client:
            return []
        try:
            r = await musicdl_client.get(
                "/search",
                params={"keyword": keyword, "limit": 5},
                timeout=8.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            rows = data.get("items") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                return []
            return [x for x in rows if isinstance(x, dict)]
        except Exception as e:
            logger.debug("musicdl recommend search failed: %s", e)
            return []

    async def _lx() -> list[dict]:
        if not (lx_enabled and lx_client):
            return []
        try:
            params: dict = {"keyword": keyword, "limit": 5}
            if lx_sources:
                params["sources"] = ",".join(lx_sources)
            r = await lx_client.get(
                "/api/v1/search",
                params=params,
                timeout=8.0,
            )
            if r.status_code != 200:
                return []
            data = r.json()
            rows = data.get("items") if isinstance(data, dict) else None
            if not isinstance(rows, list):
                return []
            out: list[dict] = []
            for it in rows:
                if not isinstance(it, dict):
                    continue
                tid = str(it.get("id") or "")
                if not tid:
                    continue
                out.append({
                    "id": tid if tid.startswith("lx:") else f"lx:{tid}",
                    "source": "lx",
                    "title": str(it.get("title") or it.get("name") or ""),
                    "artist": str(it.get("artist") or ""),
                    "album": str(it.get("album") or ""),
                    "duration_s": float(it.get("duration_s") or 0) or 0,
                    "ext": str(it.get("ext") or "mp3") or "mp3",
                    "cover_url": str(it.get("cover_url") or ""),
                })
            return out
        except Exception as e:
            logger.debug("lxmusic recommend search failed: %s", e)
            return []

    jobs = [asyncio.create_task(coro) for coro in (_mb(), _mdl(), _lx())]
    try:
        for fut in asyncio.as_completed(jobs):
            try:
                items = await fut
            except Exception:
                continue
            if items:
                return items
        return []
    finally:
        for t in jobs:
            if not t.done():
                t.cancel()


async def verify_track_playable(
    track: dict,
    musicdl_client: httpx.AsyncClient | None,
    musicbox_client: httpx.AsyncClient | None,
    lx_client: httpx.AsyncClient | None,
) -> bool:
    """issue #23：推荐曲目入列前的逐首可播校验（短预算，只验存在性）。

    - netease：musicbox 解析直链，data.url 非空即通过（quality 用最便宜的
      standard 档，实际播放仍走完整音质阶梯与降档）
    - lx：lxmusic /api/v1/track/url 解析直链；服务端解析成功即已含 Range
      探活（900s 探活缓存复用，后续正式播放反而更快）
    - musicdl：/stream Range 首 KB 头部探活（200/206 且非文本/JSON）
    任何源校验异常一律按"不可播"处理——推荐宁缺毋滥，播放失败自动跳歌的
    体验远差于列表少几首。
    """
    guid = str(track.get("guid") or "")
    parts = guid.split(":")
    if len(parts) < 3 or parts[0] != "online":
        return True  # 非在线条目（理论不会出现）不拦截
    src = parts[1]
    timeout = verify_timeout_s()
    try:
        if src == "netease":
            if musicbox_client is None:
                return True
            r = await musicbox_client.get(
                f"/api/v1/song/{parts[2]}/url",
                params={"quality": "standard"}, timeout=timeout,
            )
            if r.status_code != 200:
                return False
            body = r.json()
            data = body.get("data") if isinstance(body, dict) else None
            return bool(isinstance(data, dict) and data.get("url"))
        if src == "lx":
            if lx_client is None:
                return True
            r = await lx_client.get(
                "/api/v1/track/url",
                params={"id": ":".join(parts[2:]), "quality": "standard"},
                timeout=timeout,
            )
            if r.status_code != 200:
                return False
            body = r.json()
            data = body.get("data") if isinstance(body, dict) else None
            return bool(isinstance(data, dict) and data.get("url"))
        # musicdl 聚合引擎（migu/kuwo/...）：id 可能含冒号，取第 3 段之后的整体
        if musicdl_client is None:
            return True
        r = await musicdl_client.send(
            musicdl_client.build_request(
                "GET", "/stream",
                params={"id": ":".join(parts[2:]), "proxy": "true"},
                headers={"Range": "bytes=0-1023", "Accept-Encoding": "identity"},
            ),
            stream=True,
        )
        try:
            ctype = r.headers.get("content-type", "").lower()
            return r.status_code in (200, 206) and "text/" not in ctype and "json" not in ctype
        finally:
            await r.aclose()
    except Exception as e:
        logger.debug("verify playable failed for %s: %s", guid, e)
        return False


async def resolve_recommendations(
    recs: list[dict],
    musicdl_client: httpx.AsyncClient | None,
    musicbox_client: httpx.AsyncClient | None,
    netease_enabled: bool,
    build_track,
    limit: int = PLAYLIST_SIZE,
    exclude_guids: set[str] | None = None,
    exclude_ta: set[tuple[str, str]] | None = None,
    lx_client: httpx.AsyncClient | None = None,
    lx_enabled: bool = False,
    lx_sources: "list[str] | None" = None,
    on_track=None,
    should_stop=None,
) -> list[dict]:
    """把候选歌名检索成可播放的在线 Track，跳过已收藏，凑满 limit 首。

    issue #23：每首入列前做可播校验（verify_track_playable），失败换下一候选。
    issue #29：on_track(track) 每入列一首即回调（供上层渐进 checkpoint 落盘，
    超时中断不再整块丢弃已完成部分）；should_stop() 为真时停止消费后续候选。
    """
    skip_ids = set(exclude_guids or ())
    skip_ta = set(exclude_ta or ())
    verify = verify_playable_enabled()
    out: list[dict] = []
    seen_ids: set[str] = set()
    seen_ta: set[tuple[str, str]] = set()
    sem = asyncio.Semaphore(search_concurrency())

    def _excluded(guid: str, title: str, artist: str) -> bool:
        if guid and guid in skip_ids:
            return True
        key = identity_key(title, artist)
        return key != ("", "") and key in skip_ta

    async def one(rec: dict) -> list[dict]:
        title = rec.get("title") or ""
        artist = rec.get("artist") or ""
        keyword = " ".join(x for x in (artist, title) if x).strip() or title
        interval = search_interval_s()
        async with sem:
            if interval > 0:
                await asyncio.sleep(interval)
            items = await _search_keyword(
                keyword, musicdl_client, musicbox_client, netease_enabled,
                lx_client=lx_client, lx_enabled=lx_enabled, lx_sources=lx_sources,
            )
        if not items and artist:
            async with sem:
                if interval > 0:
                    await asyncio.sleep(interval)
                items = await _search_keyword(
                    artist, musicdl_client, musicbox_client, netease_enabled,
                    lx_client=lx_client, lx_enabled=lx_enabled, lx_sources=lx_sources,
                )
        if not items:
            return []
        ranked = sorted(items, key=lambda it: _match_score(it, title, artist), reverse=True)
        want = 1 if title else 3
        found: list[dict] = []
        for pick in ranked:
            if not isinstance(pick, dict):
                continue
            pid = str(pick.get("id") or "")
            guid = f"online:{pid}" if pid and not str(pid).startswith("online:") else pid
            ptitle = str(pick.get("title") or pick.get("name") or "")
            partist = str(pick.get("artist") or "")
            if _excluded(guid, ptitle, partist) or _excluded(pid, ptitle, partist):
                continue
            track = build_track(pick)
            tg = str(track.get("guid") or guid)
            tt, ta = str(track.get("title") or ptitle), str(track.get("artist") or partist)
            if _excluded(tg, tt, ta):
                continue
            if verify and not await verify_track_playable(
                track, musicdl_client, musicbox_client, lx_client,
            ):
                continue
            if rec.get("genre") and isinstance(track.get("genres"), list) and not track["genres"]:
                track["genres"] = [rec["genre"]]
            track["recommendDimension"] = rec.get("dimension") or ""
            track["recommendReason"] = rec.get("reason") or ""
            found.append(track)
            if len(found) >= want:
                break
        return found

    tasks = [asyncio.create_task(one(rec)) for rec in recs]
    try:
        for fut in asyncio.as_completed(tasks):
            if should_stop and should_stop():
                break
            try:
                tracks = await fut
            except Exception as e:
                logger.debug("resolve rec failed: %s", e)
                continue
            for track in tracks or []:
                guid = str(track.get("guid") or "")
                ta = identity_key(str(track.get("title") or ""), str(track.get("artist") or ""))
                if guid in seen_ids or guid in skip_ids:
                    continue
                if ta != ("", "") and (ta in seen_ta or ta in skip_ta):
                    continue
                seen_ids.add(guid)
                if ta != ("", ""):
                    seen_ta.add(ta)
                out.append(track)
                if on_track is not None:
                    try:
                        on_track(track)
                    except Exception as e:
                        logger.debug("on_track callback failed: %s", e)
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
    return out


def stamp_playlist_tracks(tracks: list[dict], now: int | None = None) -> list[dict]:
    ts = int(now or time.time())
    out: list[dict] = []
    for t in tracks:
        item = dict(t)
        item.setdefault("createdAt", ts)
        item.setdefault("updatedAt", ts)
        item.setdefault("isFavorite", False)
        item.setdefault("isCue", False)
        item.setdefault("accessStatus", 0)
        item.setdefault("year", None)
        item.setdefault("discNo", None)
        item.setdefault("trackNo", None)
        item.setdefault("isrc", "")
        artists = item.get("artists")
        if isinstance(artists, list):
            shaped = []
            for a in artists:
                if not isinstance(a, dict):
                    continue
                aa = dict(a)
                aa.setdefault("guid", aa.get("guid") or f"{item.get('guid')}:artist")
                aa.setdefault("coverId", aa.get("guid"))
                aa.setdefault("createdAt", ts)
                aa.setdefault("updatedAt", ts)
                shaped.append(aa)
            item["artists"] = shaped
        album = item.get("album")
        if isinstance(album, dict):
            alb = dict(album)
            alb.setdefault("createdAt", ts)
            alb.setdefault("updatedAt", ts)
            alb.setdefault("releaseDate", "")
            alb.setdefault("barcode", "")
            item["album"] = alb
        out.append(item)
    return out


def _dedupe_extend(base: list[dict], extra: list[dict], limit: int) -> list[dict]:
    seen_ids = {str(t.get("guid") or "") for t in base}
    seen_ta = {identity_key(str(t.get("title") or ""), str(t.get("artist") or "")) for t in base}
    out = list(base)
    for track in extra:
        guid = str(track.get("guid") or "")
        ta = identity_key(str(track.get("title") or ""), str(track.get("artist") or ""))
        if guid and guid in seen_ids:
            continue
        if ta != ("", "") and ta in seen_ta:
            continue
        seen_ids.add(guid)
        if ta != ("", ""):
            seen_ta.add(ta)
        out.append(track)
        if len(out) >= limit:
            break
    return out[:limit]


def cache_path(user_guid: str, day: str, kind: str = "daily") -> str:
    kind = "hot" if kind == "hot" else "daily"
    return os.path.join(recommend_cache_dir(), _safe_user_name(user_guid), f"{kind}-{day}.json")


def load_daily_cache(user_guid: str, day: str, kind: str = "daily") -> dict | None:
    path = cache_path(user_guid, day, kind)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        if str(data.get("day") or "") != day:
            return None
        if data.get("status") not in ("ready", "partial"):
            return None
        tracks = data.get("tracks")
        if not isinstance(tracks, list) or not tracks:
            return None
        return data
    except Exception as e:
        logger.warning("failed to load recommend cache: %s", e)
    return None


def save_daily_cache(user_guid: str, day: str, payload: dict, kind: str = "daily") -> None:
    _atomic_write_json(cache_path(user_guid, day, kind), payload)


def purge_stale_daily_cache(user_guid: str, keep_day: str) -> None:
    folder = os.path.join(recommend_cache_dir(), _safe_user_name(user_guid))
    if not os.path.isdir(folder):
        return
    keep = {f"daily-{keep_day}.json", f"hot-{keep_day}.json"}
    for name in os.listdir(folder):
        if not name.endswith(".json") or name in keep:
            continue
        path = os.path.join(folder, name)
        try:
            os.remove(path)
            logger.info("purged stale daily recommend %s", path)
        except Exception as e:
            logger.warning("failed to purge %s: %s", path, e)


def invalidate_today_cache_all_users() -> int:
    """issue #22：音源集合变化后，清掉所有用户当日 daily/hot 推荐缓存。

    切换音源（网易↔洛雪等）后旧缓存里的曲目可能已不可播（音源禁用/白名单
    变化），当日缓存必须失效，下次打开歌单按新音源重建。返回删除的文件数。
    """
    root = recommend_cache_dir()
    try:
        users = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    except OSError:
        return 0
    day = today_key()
    removed = 0
    for user in users:
        folder = os.path.join(root, user)
        try:
            names = os.listdir(folder)
        except OSError:
            continue
        for name in names:
            if name not in (f"daily-{day}.json", f"hot-{day}.json"):
                continue
            path = os.path.join(folder, name)
            try:
                os.remove(path)
                removed += 1
                logger.info("invalidated recommend cache (source set changed): %s", path)
            except Exception as e:
                logger.warning("failed to invalidate %s: %s", path, e)
    # 音源名额一并释放：音源集合变了，"当天第一个用户"需要重新竞争
    slot = source_slot_path(day)
    try:
        if os.path.exists(slot):
            os.remove(slot)
            removed += 1
            logger.info("released daily source slot (source set changed): %s", slot)
    except Exception as e:
        logger.warning("failed to release source slot: %s", e)
    return removed


# ---- 每日推荐"音源名额"：网易日推每天只有一份内容，只允许当天第一个用户使用 ----
# 名额 marker 落盘（跨重启有效）：recommend_cache/_source-slot-<day>.json；
# 进程内 asyncio.Lock 收敛并发构建下"查名额→抓取→占名额"的竞态窗口。
_SOURCE_SLOT_LOCK = asyncio.Lock()


def source_slot_path(day: str | None = None) -> str:
    return os.path.join(recommend_cache_dir(), f"_source-slot-{day or today_key()}.json")


def read_source_slot(day: str | None = None) -> dict | None:
    path = source_slot_path(day)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and str(data.get("day") or "") == (day or today_key()):
            return data
    except Exception as e:
        logger.warning("failed to read daily source slot: %s", e)
    return None


def source_slot_claimed(day: str | None = None) -> bool:
    return read_source_slot(day) is not None


def claim_source_slot(day: str | None, user_guid: str) -> bool:
    """占用当日音源名额。已被占用返回 False（幂等，不覆盖先占者）。"""
    day = day or today_key()
    if source_slot_claimed(day):
        return False
    ok = _atomic_write_json(source_slot_path(day), {
        "day": day,
        "kind": "daily",
        "claimedBy": _safe_user_name(user_guid),
        "at": int(time.time()),
    })
    return ok


def purge_stale_source_slots(keep_day: str) -> None:
    """清掉非当日的音源名额 marker（与 purge_stale_daily_cache 同生命周期）。"""
    root = recommend_cache_dir()
    try:
        names = os.listdir(root)
    except OSError:
        return
    for name in names:
        if not (name.startswith("_source-slot-") and name.endswith(".json")):
            continue
        if name == f"_source-slot-{keep_day}.json":
            continue
        path = os.path.join(root, name)
        try:
            os.remove(path)
            logger.info("purged stale daily source slot %s", path)
        except Exception as e:
            logger.warning("failed to purge source slot %s: %s", path, e)


# 每用户最近一次推荐歌单构建结果（仅供 /_ext/healthz 观测，非持久化）
_LAST_DAILY_INFO: dict[str, dict] = {}


def _remember_daily_result(user_guid: str, payload: dict) -> None:
    kind = str(payload.get("kind") or "daily")
    _LAST_DAILY_INFO[f"{user_guid}:{kind}"] = {
        "user": user_guid,
        "kind": kind,
        "day": payload.get("day"),
        "status": payload.get("status"),
        "tiers": list(payload.get("tiers") or []),
        "tierFailures": list(payload.get("tierFailures") or []),
        "trackCount": len(payload.get("tracks") or []),
    }


def last_recommend_summary() -> dict:
    return {user: dict(info) for user, info in _LAST_DAILY_INFO.items()}


def build_playlist_record(
    guid: str,
    name: str,
    cover_id: str | None,
    track_count: int,
    created_at: int | None = None,
) -> dict:
    ts = created_at or int(time.time())
    return {
        "guid": guid,
        "name": name,
        "coverId": cover_id or guid,
        "createdAt": ts,
        "updatedAt": ts,
        "trackCount": track_count,
        "isDaily": True,
    }


def playlist_display_name(kind: str, day: str | None = None) -> str:
    if kind == "hot":
        return "热门推荐"
    day = day or today_key()
    return f"每日推荐 {day[4:6]}-{day[6:8]}"


def empty_daily_bundle(user_guid: str, kind: str = "daily") -> dict:
    kind = "hot" if kind == "hot" else "daily"
    day = today_key()
    guid = recommend_playlist_guid(kind, day, user_guid)
    playlist = build_playlist_record(
        guid=guid,
        name=playlist_display_name(kind, day),
        cover_id=guid,
        track_count=0,
    )
    return {
        "day": day,
        "kind": kind,
        "guid": guid,
        "playlist": playlist,
        "tracks": [],
        "seedCount": 0,
        "builtAt": int(time.time()),
    }


async def get_or_build_daily(
    user_guid: str,
    musicdl_client: httpx.AsyncClient | None,
    musicbox_client: httpx.AsyncClient | None,
    llm_http: httpx.AsyncClient | None,
    build_track,
    netease_enabled: bool,
    extra_seeds: list[dict] | None = None,
    favorite_items: list[dict] | None = None,
    lx_client: httpx.AsyncClient | None = None,
    lx_enabled: bool = False,
    lx_sources: "list[str] | None" = None,
    recommend_hot: bool = True,
    recommend_daily: bool = True,
    kind: str = "daily",
) -> dict:
    kind = "hot" if kind == "hot" else "daily"
    day = today_key()
    guid = recommend_playlist_guid(kind, day, user_guid)
    purge_stale_daily_cache(user_guid, day)
    if kind == "daily":
        purge_stale_source_slots(day)
    cached = load_daily_cache(user_guid, day, kind)
    existing = list(cached.get("tracks") or []) if cached else []
    if len(existing) >= PLAYLIST_SIZE:
        _remember_daily_result(user_guid, cached)
        return cached

    if kind == "hot":
        # 热门歌单：榜单原味，不排除已收藏/最近播放，也不用种子（不走 llm/fallback）
        play_seeds: list[dict] = []
        fav_seeds: list[dict] = []
        exclude_guids, exclude_ta = set(), set()
    else:
        local_seeds = read_local_recent_tracks(music_db_path(), user_guid, SEED_LIMIT)
        online_seeds = seeds_from_online_history(user_guid)
        local_favs = read_local_favorite_tracks(music_db_path(), user_guid)
        online_favs = [x for x in (favorite_items or []) if isinstance(x, dict)]
        fav_seeds = []
        for it in online_favs + local_favs:
            title, artist = _item_title_artist(it)
            album = ""
            track = it.get("track") if isinstance(it.get("track"), dict) else it
            if isinstance(track.get("album"), dict):
                album = str(track["album"].get("name") or "")
            elif track.get("albumName"):
                album = str(track.get("albumName") or "")
            elif track.get("album"):
                album = str(track.get("album") or "")
            fav_seeds.append({
                "guid": str(it.get("guid") or track.get("guid") or ""),
                "title": title,
                "artist": artist,
                "album": album,
                "genre": "",
                "language": infer_language(title, artist, album),
                "playedAt": int(it.get("createdAt") or it.get("playedAt") or 0),
                "source": "favorite",
            })
        play_seeds = merge_recent_seeds(local_seeds, online_seeds, extra_seeds, SEED_LIMIT)
        exclude_guids, exclude_ta = collect_exclude_sets(play_seeds, fav_seeds, extra_seeds, online_favs, local_favs)
    if existing:
        exclude_guids = exclude_guids | {str(t.get("guid") or "") for t in existing}
        exclude_ta = exclude_ta | {
            identity_key(str(t.get("title") or ""), str(t.get("artist") or "")) for t in existing
        }

    async def from_netease_daily(on_track=None, should_stop=None) -> list[dict]:
        # 网易每日推荐每天只有一份内容：仅当天第一个构建的用户可用（占用"音源
        # 名额"），其余用户由 llm / local-random 生成各自不同的每日推荐。
        # 锁内二次检查名额，抓取成功即占名额（列表已生成过一次，即便被当前
        # 用户的排除集过滤空也不再给后来用户重复使用）。
        if not (recommend_daily and netease_enabled and musicbox_client):
            return []
        if source_slot_claimed(day):
            return []
        async with _SOURCE_SLOT_LOCK:
            if source_slot_claimed(day):
                return []
            items = await fetch_musicbox_recommend(
                musicbox_client, "/api/v1/recommend/songs", {"limit": NETEASE_DAILY_LIMIT}
            )
            if items and claim_source_slot(day, user_guid):
                logger.info(
                    "daily source slot claimed by %s (%d candidates)", user_guid, len(items)
                )
        if not items:
            return []
        return resolve_source_candidates(items, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)

    async def from_netease_charts(on_track=None, should_stop=None) -> list[dict]:
        if not (recommend_hot and netease_enabled and musicbox_client):
            return []
        items = await fetch_musicbox_recommend(
            musicbox_client, "/api/v1/toplist",
            {"index": NETEASE_TOPLIST_INDEX, "limit": CHART_FETCH_COUNT},
        )
        if not items:
            return []
        return resolve_source_candidates(items, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)

    async def from_lx_charts(on_track=None, should_stop=None) -> list[dict]:
        if not (recommend_hot and lx_enabled and lx_client):
            return []
        items = await fetch_lx_charts(lx_client, CHART_FETCH_COUNT, sources=lx_sources)
        if not items:
            return []
        return resolve_source_candidates(items, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)

    async def from_charts_fallback(on_track=None, should_stop=None) -> list[dict]:
        """榜单兜底：严格按照当前激活的音源获取热门歌曲。
        - 若当前是网易云音源 (netease_enabled)：
          抓取网易云官方免登录榜单（热歌榜/飙升榜），确保 source 为 netease（走网易云解析播放）
        - 若当前是洛雪音源 (lx_enabled)：
          抓取酷狗 TOP500 / 飙升榜 / 网易新歌榜，统一封装为 lx 格式（走洛雪自定义源解析播放）
        - 若当前是 musicdl (musicdl_client)：
          抓取热门歌曲后通过 musicdl 检索匹配（走 musicdl 解析播放）
        """
        if not recommend_hot:
            return []
        try:
            from . import charts as chart_mod
        except ImportError:
            import charts as chart_mod

        day = today_key()

        # A. 网易云音源：抓取网易云官方免登录榜单 (热歌榜 3778678, 飙升榜 19723756)
        if netease_enabled:
            for cid in ("wy_3778678", "wy_19723756"):
                try:
                    fetched = await chart_mod.get_or_load_chart_tracks(
                        cid, netease_enabled=True, limit=PLAYLIST_SIZE
                    )
                    if fetched:
                        adapted = []
                        for it in fetched:
                            it_copy = dict(it)
                            sid = str(it_copy.get("song_id") or it_copy.get("id") or "").replace("netease:", "")
                            it_copy["id"] = f"netease:{sid}"
                            it_copy["source"] = "netease"
                            adapted.append(it_copy)
                        res = resolve_source_candidates(adapted, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)
                        if res:
                            return res
                except Exception as e:
                    logger.debug("from_charts_fallback netease %s failed: %s", cid, e)
            return []

        # B. 洛雪音源：抓取酷狗 TOP500 / 飙升榜 / 网易榜单，统一封装为 lx 格式
        if lx_enabled:
            for cid in ("kg_8888", "kg_6666", "kg_23784", "wy_3778678"):
                cached_tracks = chart_mod.load_chart_cache(cid, day)
                if cached_tracks:
                    adapted = []
                    for it in cached_tracks:
                        it_copy = dict(it)
                        raw_id = str(it_copy.get("id") or "")
                        if not raw_id.startswith("lx:"):
                            if "song_id" in it_copy:
                                raw_id = f"lx:wy:{it_copy['song_id']}"
                            elif "hash" in it_copy:
                                raw_id = f"lx:kg:{it_copy['hash']}"
                            else:
                                raw_id = f"lx:{raw_id}"
                        it_copy["id"] = raw_id
                        it_copy["source"] = "lx"
                        adapted.append(it_copy)
                    res = resolve_source_candidates(adapted, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)
                    if res:
                        return res

            for cid in ("kg_8888", "kg_6666", "wy_3778678"):
                try:
                    fetched = await chart_mod.get_or_load_chart_tracks(cid, limit=PLAYLIST_SIZE)
                    if fetched:
                        adapted = []
                        for it in fetched:
                            it_copy = dict(it)
                            raw_id = str(it_copy.get("id") or "")
                            if not raw_id.startswith("lx:"):
                                if "song_id" in it_copy:
                                    raw_id = f"lx:wy:{it_copy['song_id']}"
                                elif "hash" in it_copy:
                                    raw_id = f"lx:kg:{it_copy['hash']}"
                                else:
                                    raw_id = f"lx:{raw_id}"
                            it_copy["id"] = raw_id
                            it_copy["source"] = "lx"
                            adapted.append(it_copy)
                        res = resolve_source_candidates(adapted, build_track, PLAYLIST_SIZE, exclude_guids, exclude_ta)
                        if res:
                            return res
                except Exception as e:
                    logger.debug("from_charts_fallback lx %s failed: %s", cid, e)
            return []

        # C. musicdl 音源：抓取热门歌曲后，通过 musicdl 检索匹配
        if musicdl_client:
            chart_tracks = []
            for cid in ("kg_8888", "wy_3778678"):
                try:
                    chart_tracks = await chart_mod.get_or_load_chart_tracks(cid, limit=PLAYLIST_SIZE)
                    if chart_tracks:
                        break
                except Exception:
                    pass
            if chart_tracks:
                recs = [
                    {"title": str(t.get("title") or ""), "artist": str(t.get("artist") or "")}
                    for t in chart_tracks if t.get("title")
                ]
                return await resolve_recommendations(
                    recs, musicdl_client=musicdl_client, musicbox_client=None,
                    netease_enabled=False, build_track=build_track, limit=PLAYLIST_SIZE,
                    exclude_guids=exclude_guids, exclude_ta=exclude_ta,
                    on_track=on_track, should_stop=should_stop,
                )

        return []

    async def from_llm(on_track=None, should_stop=None) -> list[dict]:
        # 配置了 FNMUSIC_LLM_* 即作为每日推荐来源：音源名额已被占用（或音源
        # 不支持每日推荐）的用户走这一层，种子按用户（本地+在线历史+收藏）
        if llm_http is None or not llm_enabled() or not recommend_daily:
            return []
        recs = await call_llm(
            llm_http, build_llm_prompt(play_seeds, fav_seeds[:40], llm_candidate_count())
        )
        if not recs:
            return []
        return await resolve_recommendations(
            recs, musicdl_client, musicbox_client, netease_enabled, build_track,
            PLAYLIST_SIZE, exclude_guids, exclude_ta,
            lx_client=lx_client, lx_enabled=lx_enabled, lx_sources=lx_sources,
            on_track=on_track, should_stop=should_stop,
        )

    async def from_local_random(on_track=None, should_stop=None) -> list[dict]:
        # 本地曲库兜底：无需网络，只读官方库按账户播放历史随机选"最近没听过"
        # 的本地现存曲目；音源与大模型都不可用（或不足 20 首）时保证仍有内容
        if not recommend_daily:
            return []
        rows = read_local_random_tracks(
            music_db_path(), user_guid, PLAYLIST_SIZE, exclude_guids, exclude_ta
        )
        return [build_local_track(r) for r in rows]

    tracks = list(existing)
    t0 = time.monotonic()
    contributing: list[str] = []
    # issue #29：tier 中断原因（timeout/异常）随 payload 落盘并进 healthz，
    # 不再静默空列表；空结果时用户能在 /_ext/healthz 里看到是谁断了
    tier_failures: list[str] = []

    def _save_checkpoint() -> None:
        if not tracks:
            return
        stamped = stamp_playlist_tracks(tracks[:PLAYLIST_SIZE])
        picked = pick_playlist_cover_track(stamped)
        cover_id = str((picked or {}).get("coverId") or (picked or {}).get("guid") or guid)
        pl = build_playlist_record(
            guid=guid,
            name=playlist_display_name(kind, day),
            cover_id=cover_id,
            track_count=len(stamped),
        )
        cp = {
            "day": day,
            "kind": kind,
            "guid": guid,
            "status": "ready" if len(stamped) >= PLAYLIST_SIZE else "partial",
            "playlist": pl,
            "tracks": stamped,
            "tiers": list(contributing),
            "tierFailures": list(tier_failures),
            "seedCount": len(play_seeds),
            "favoriteCount": len(fav_seeds),
            "builtAt": int(time.time()),
        }
        save_daily_cache(user_guid, day, cp, kind)

    last_progress_cp = 0.0

    def _save_checkpoint_throttled() -> None:
        # 渐进落盘节流：至少间隔 2s，避免每首歌刷一次磁盘
        nonlocal last_progress_cp
        now = time.monotonic()
        if now - last_progress_cp >= 2.0:
            last_progress_cp = now
            _save_checkpoint()

    def _cur_keys() -> tuple[set[str], set[tuple[str, str]]]:
        guids = {str(t.get("guid") or "") for t in tracks}
        tas = {identity_key(str(t.get("title") or ""), str(t.get("artist") or "")) for t in tracks}
        return guids, tas

    async def run_tier(name: str, tier_factory) -> None:
        """执行单层构建。issue #29 重构：

        - 渐进：llm 层逐首经 _emit 入列并节流 checkpoint，wait_for 超时
          只中断"后续候选"，已完成部分不丢；
        - 可见：层失败 debug→warning，原因记入 tier_failures 落盘 + healthz。
        """
        nonlocal tracks
        if len(tracks) >= PLAYLIST_SIZE:
            return
        budget = build_budget_s()
        remaining = budget - (time.monotonic() - t0)
        if remaining <= 0:
            tier_failures.append(f"{name}:no-budget")
            logger.warning("daily recommend tier %s skipped: no budget left (budget=%.1fs)", name, budget)
            return
        # 本层截止时间：保护预算扩展的是这一层的可用时间，should_stop 也必须对齐它，
        # 否则 fallback 层一启动就因"全局预算已耗尽"立即自停（保护预算形同虚设）
        tier_deadline = time.monotonic() + remaining
        added = 0

        def _emit(track: dict) -> None:
            nonlocal added
            if len(tracks) >= PLAYLIST_SIZE:
                return
            cur_guids, cur_ta = _cur_keys()
            tg = str(track.get("guid") or "")
            tta = identity_key(str(track.get("title") or ""), str(track.get("artist") or ""))
            if tg and tg in cur_guids:
                return
            if tta != ("", "") and tta in cur_ta:
                return
            tracks.append(track)
            added += 1
            if name not in contributing:
                contributing.append(name)
            _save_checkpoint_throttled()

        def _should_stop() -> bool:
            return len(tracks) >= PLAYLIST_SIZE or time.monotonic() >= tier_deadline

        try:
            chunk = await asyncio.wait_for(
                tier_factory(on_track=_emit, should_stop=_should_stop),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            tier_failures.append(f"{name}:timeout")
            logger.warning(
                "daily recommend tier %s timeout (remaining=%.1fs, 已渐进入列 %d 首)", name, remaining, added,
            )
            if added:
                _save_checkpoint()
            return
        except Exception as e:
            tier_failures.append(f"{name}:{type(e).__name__}")
            logger.warning("daily recommend tier %s failed (已渐进入列 %d 首): %s", name, added, e)
            if added:
                _save_checkpoint()
            return
        if chunk:
            before = len(tracks)
            tracks = _dedupe_extend(tracks, chunk, PLAYLIST_SIZE)
            if len(tracks) > before:
                if name not in contributing:
                    contributing.append(name)
                _save_checkpoint()

    # 每日推荐链（按账户隔离）：网易真每日推荐（每天仅第一个用户，占用音源名额）
    # -> LLM（配置即用，种子按用户）-> 本地曲库随机兜底（最近没听过优先）；
    # 热门推荐链：网易热歌榜 -> lx 免登录榜单。每级不足 20 首由下一级补齐。
    # FNMUSIC_RECOMMEND_DAILY / FNMUSIC_RECOMMEND_HOT 分别门控两条链的构建。
    if kind == "hot":
        if lx_enabled:
            await run_tier("lx-charts", from_lx_charts)
            if len(tracks) < PLAYLIST_SIZE:
                await run_tier("charts-fallback", from_charts_fallback)
        elif netease_enabled:
            await run_tier("netease-charts", from_netease_charts)
            if len(tracks) < PLAYLIST_SIZE:
                await run_tier("charts-fallback", from_charts_fallback)
        else:
            await run_tier("charts-fallback", from_charts_fallback)
    elif recommend_daily:
        if not source_slot_claimed(day):
            await run_tier("netease-daily", from_netease_daily)
        await run_tier("llm", from_llm)
        if len(tracks) < PLAYLIST_SIZE:
            # local-random 兜底：纯本地只读查询（毫秒级、无网络开销），不占构建
            # 总预算——上层超时/全挂也保证歌单有内容（延续"每日推荐永不为空"）
            try:
                chunk = await from_local_random()
                if chunk:
                    before = len(tracks)
                    tracks = _dedupe_extend(tracks, chunk, PLAYLIST_SIZE)
                    if len(tracks) > before:
                        contributing.append("local-random")
                        _save_checkpoint()
            except Exception as e:
                tier_failures.append(f"local-random:{type(e).__name__}")
                logger.warning("daily recommend tier local-random failed: %s", e)

    tracks = stamp_playlist_tracks(tracks[:PLAYLIST_SIZE])
    picked = pick_playlist_cover_track(tracks)
    cover_id = str((picked or {}).get("coverId") or (picked or {}).get("guid") or guid)
    playlist = build_playlist_record(
        guid=guid,
        name=playlist_display_name(kind, day),
        cover_id=cover_id,
        track_count=len(tracks),
    )
    payload = {
        "day": day,
        "kind": kind,
        "guid": guid,
        "status": "ready" if len(tracks) >= PLAYLIST_SIZE else "partial",
        "playlist": playlist,
        "tracks": tracks,
        "tiers": contributing,
        "tierFailures": list(tier_failures),
        "seedCount": len(play_seeds),
        "favoriteCount": len(fav_seeds),
        "builtAt": int(time.time()),
    }
    if tracks:
        save_daily_cache(user_guid, day, payload, kind)
        logger.info(
            "%s recommend %s tracks=%s status=%s tiers=%s",
            kind, guid, len(tracks), payload["status"], ",".join(contributing) or "-",
        )
    _remember_daily_result(user_guid, payload)
    return payload
