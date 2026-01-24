import argparse
import json
import os
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from urllib.parse import parse_qs, urlparse, urljoin

import requests


def _load_env_file() -> None:
    """Простейший loader .env без сторонних зависимостей."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())
    except Exception:
        return


_load_env_file()

USER_AGENT = "TelegramBot (like TwitterBot)"
VK_API_HOST = os.environ.get("VK_API_HOST", "api.vk.com")
def _load_timeout(default: int = 10) -> int:
    try:
        val = int(os.environ.get("REQUEST_TIMEOUT", str(default)))
        return max(1, val)
    except Exception:
        return default


TIMEOUT = _load_timeout()
SERVICES = ("yandex", "vk", "mts", "zvuk", "spotify", "ytmusic", "shazam")
VK_ACCESS_TOKEN_ENV = os.environ.get("VK_ACCESS_TOKEN")
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
# если трек недоступен в регионе, лучше оставить MARKET пустым и не передавать параметр
SPOTIFY_MARKET = os.environ.get("SPOTIFY_MARKET", "").strip()
YTMUSIC_HEADERS_PATH = os.environ.get("YTMUSIC_HEADERS_PATH") or str(Path(__file__).resolve().parent / "ytmusic_headers.json")
MTS_API_BASE = "https://api.music.mts.ru/web/v1"
MTS_API_HEADERS = {
    "X-Music-Client": "web",
    "X-Yandex-Music-Client": "MTSMusicWebPremium/2.4.1",
    "Content-Type": "application/json",
}


def _load_local_env_token() -> Optional[str]:
    """Подхватываем YANDEX_TOKEN из локального .env, если не задан."""
    if os.environ.get("YANDEX_TOKEN"):
        return os.environ.get("YANDEX_TOKEN")
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("YANDEX_TOKEN="):
                    token = line.split("=", 1)[1].strip()
                    if token:
                        os.environ.setdefault("YANDEX_TOKEN", token)
                        return token
        except Exception:
            return None
    return os.environ.get("YANDEX_TOKEN")


YANDEX_TOKEN = _load_local_env_token()
YTMUSIC_HEADERS_JSON = os.environ.get("YTMUSIC_HEADERS_JSON")

# Подхватываем Spotify креды из .env, если не заданы
def _load_spotify_creds() -> Tuple[Optional[str], Optional[str]]:
    cid = os.environ.get("SPOTIFY_CLIENT_ID")
    sec = os.environ.get("SPOTIFY_CLIENT_SECRET")
    if cid and sec:
        return cid, sec
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("SPOTIFY_CLIENT_ID=") and not cid:
                    cid = line.split("=", 1)[1].strip()
                    os.environ.setdefault("SPOTIFY_CLIENT_ID", cid)
                if line.startswith("SPOTIFY_CLIENT_SECRET=") and not sec:
                    sec = line.split("=", 1)[1].strip()
                    os.environ.setdefault("SPOTIFY_CLIENT_SECRET", sec)
        except Exception:
            pass
    return cid, sec


SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET = _load_spotify_creds()


def _load_vk_access_token(token_path: Path) -> Optional[str]:
    """VK токен: сперва VK_ACCESS_TOKEN из env/.env, иначе читаем json-файл."""
    if VK_ACCESS_TOKEN_ENV:
        return VK_ACCESS_TOKEN_ENV
    if token_path.exists():
        try:
            token_data = json.loads(token_path.read_text(encoding="utf-8"))
            return token_data.get("access_token")
        except Exception:
            return None
    return None

# Ленивая инициализация (YT Music будет настроен ниже)
_YTMUSIC_CLIENT = None


def _proxy_url_for_service(service: Optional[str]) -> Optional[str]:
    if not service:
        return None
    if service == "yandex":
        return os.environ.get("YANDEX_PROXY_URL") or os.environ.get("MUSIC_PROXY_URL")
    if service == "vk":
        return os.environ.get("VK_PROXY_URL") or os.environ.get("MUSIC_PROXY_URL")
    if service == "mts":
        return os.environ.get("MTS_PROXY_URL") or os.environ.get("MUSIC_PROXY_URL")
    return None


def _get_proxies_for_url(url: str) -> Optional[Dict[str, str]]:
    """Прокси только для Яндекс/VK/BOOM/МТС, остальные сервисы идут напрямую."""
    proxy = _proxy_url_for_service(detect_service(url))
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def _request(method: str, url: str, **kwargs):
    proxies = _get_proxies_for_url(url)
    if proxies:
        kwargs.setdefault("proxies", proxies)
    return requests.request(method, url, **kwargs)


def _get(url: str, **kwargs):
    return _request("GET", url, **kwargs)


def _post(url: str, **kwargs):
    return _request("POST", url, **kwargs)


@dataclass
class SongMeta:
    title: Optional[str]
    album: Optional[str]
    artist: Optional[str]
    year: Optional[str]
    image: Optional[str]
    source_url: str
    resolved_url: str
    service: Optional[str]
    kind: Optional[str]  # track | album | unknown
    track_id: Optional[str]
    album_id: Optional[str]
    access_key: Optional[str]
    raw: Dict[str, str] = field(default_factory=dict)
    vk_url: Optional[str] = None
    yandex_url: Optional[str] = None
    mts_url: Optional[str] = None
    spotify_url: Optional[str] = None
    ytmusic_url: Optional[str] = None


def fetch_og_tags(url: str, dump_html: Optional[str] = None) -> Tuple[Dict[str, str], str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "ru,en;q=0.9",
    }

    # Ручное следование редиректам: если Location указывает на non-http (itms-appss и т.п.), останавливаемся на предыдущем ответе.
    current_url = url
    resp = None
    for _ in range(5):
        try:
            resp = _get(current_url, headers=headers, timeout=TIMEOUT, allow_redirects=False, stream=True)
        except requests.exceptions.InvalidSchema:
            break
        if resp.is_redirect or resp.is_permanent_redirect:
            location = resp.headers.get("Location")
            if not location:
                resp.close()
                break
            loc_scheme = urlparse(location).scheme
            if loc_scheme and loc_scheme not in {"http", "https"}:
                resp.close()
                break
            # абсолютный или относительный
            if location.startswith("http://") or location.startswith("https://"):
                resp.close()
                current_url = location
                continue
            # относительный Location
            resp.close()
            current_url = urljoin(current_url, location)
            continue
        else:
            break

    if resp is None:
        raise RuntimeError("Не удалось получить ответ")

    resp.raise_for_status()
    # Попали на капчу Яндекса / других сервисов — лучше сообщить явно.
    if "showcaptcha" in resp.url:
        raise RuntimeError("Страница вернула капчу (showcaptcha). Откройте ссылку в браузере, решите капчу и повторите.")

    final_url = resp.url

    def read_head_bytes(response, max_bytes: int = 1_000_000) -> bytes:
        buf = bytearray()
        for chunk in response.iter_content(chunk_size=8192):
            if not chunk:
                continue
            buf.extend(chunk)
            if len(buf) >= max_bytes:
                break
            if b"</head" in buf.lower():
                break
        return bytes(buf)

    head_bytes = read_head_bytes(resp)
    resp.close()
    if dump_html:
        Path(dump_html).write_bytes(head_bytes)
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except ImportError as exc:  # guard на случай, если зависимости не установлены
        raise RuntimeError("BeautifulSoup (bs4) не установлен. Выполните pip install -r requirements.txt") from exc

    soup = BeautifulSoup(head_bytes, "html.parser")
    og_tags: Dict[str, str] = {}
    for tag in soup.find_all("meta"):
        prop = tag.get("property") or tag.get("name") or ""
        if not (prop.startswith("og:") or prop.startswith("music:") or prop.startswith("ya:") or prop.startswith("vk:")):
            continue  # берем только полезные префиксы
        content = tag.get("content")
        if content is None:
            continue
        og_tags[prop] = content.strip()
    return og_tags, final_url


def _is_vk_host(host: str) -> bool:
    return bool(
        "vk." in host
        or host in {"vk.com", "m.vk.com", "vk.ru", "m.vk.ru"}
        or "boom.ru" in host
    )


def _is_youtube_host(host: str) -> bool:
    return bool("youtube.com" in host or host == "youtu.be" or host.endswith(".youtu.be"))


def _is_vk_music_url(url: str) -> bool:
    host = urlparse(url).hostname or ""
    if not _is_vk_host(host):
        return False
    kind, track_id, album_id, _ = parse_ids_from_url(url)
    return bool(kind or track_id or album_id)


def detect_service(url: str) -> Optional[str]:
    host = urlparse(url).hostname or ""
    if "yandex" in host:
        return "yandex"
    if "mts" in host or "onelink.me" in host:
        return "mts"
    if "shazam.com" in host:
        return "shazam"
    if "open.spotify.com" in host:
        return "spotify"
    if host == "music.youtube.com":
        return "ytmusic"
    if _is_vk_music_url(url):
        return "vk"
    return None


def parse_ids_from_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Возвращает (kind, track_id, album_id, access_key) по URL, если удаётся распарсить.
    """
    parsed = urlparse(url)
    segments = [s for s in parsed.path.split("/") if s]
    if not segments:
        return None, None, None, None
    query = parse_qs(parsed.query)
    access_key = query.get("access_key", [None])[0] or query.get("access_hash", [None])[0]

    def vk_audio_id(raw: str) -> Optional[str]:
        value = raw
        if value.startswith("audio_playlist"):
            value = value[len("audio_playlist") :]
        elif value.startswith("audio"):
            value = value[len("audio") :]
        parts = [p for p in value.split("_") if p]
        if len(parts) >= 2:
            return f"{parts[0]}_{parts[1]}"
        return None

    def vk_playlist_id(raw: str) -> Optional[str]:
        value = raw
        if "audio_playlist" in value:
            value = value.split("audio_playlist", 1)[1]
        parts = [p for p in value.split("_") if p]
        if len(parts) >= 2:
            return f"{parts[0]}_{parts[1]}"
        return None

    # Яндекс Музыка
    if "music.yandex" in parsed.netloc:
        if "album" in segments:
            idx = segments.index("album")
            album_id = segments[idx + 1] if idx + 1 < len(segments) else None
            if "track" in segments:
                tidx = segments.index("track")
                track_id = segments[tidx + 1] if tidx + 1 < len(segments) else None
                return "track", track_id, album_id, access_key
            return "album", None, album_id, access_key
        if segments[0] == "track" and len(segments) > 1:
            return "track", segments[1], None, access_key

    # VK музыка / boom
    if parsed.netloc in {"vk.com", "m.vk.com", "vk.ru", "m.vk.ru"} or "boom.ru" in parsed.netloc:
        # форматы: audio-200..._..., audio_playlist-200..._..., share.boom.ru/track/<id>?, /music/album/<id>
        act_param = query.get("act", [])
        if act_param:
            act_val = act_param[0]
            if act_val.startswith("audio_playlist"):
                album_id = vk_playlist_id(act_val)
                return "album", None, album_id, access_key
            if act_val.startswith("audio"):
                track_id = vk_audio_id(act_val)
                return "track", track_id, None, access_key
        if segments and segments[0].startswith("audio_playlist"):
            album_id = vk_playlist_id(segments[0])
            return "album", None, album_id, access_key
        if segments and segments[0].startswith("audio"):
            track_id = vk_audio_id(segments[0])
            return "track", track_id, None, access_key
        if len(segments) >= 3 and segments[0] == "music" and segments[1] == "album":
            playlist_raw = segments[2]
            album_id = vk_playlist_id(playlist_raw)
            return "album", None, album_id, access_key
        z_param = query.get("z", [])
        for seg in segments:
            if seg.startswith("audio_playlist"):
                album_id = vk_playlist_id(seg)
                return "album", None, album_id, access_key
            if seg == "music" and z_param:
                # vk.com/music?z=audio_playlist-200..._123
                val = z_param[0]
                album_id = vk_playlist_id(val)
                return "album", None, album_id, access_key
        if parsed.netloc.startswith("share.boom.ru"):
            if segments[0] == "track" and len(segments) > 1:
                return "track", segments[1], None, access_key
            if segments[0] == "album" and len(segments) > 1:
                return "album", None, segments[1], access_key

    # МТС музыка
    if "music.mts.ru" in parsed.netloc:
        if segments[0] in {"track", "album"} and len(segments) > 1:
            kind = segments[0]
            if kind == "track":
                return "track", segments[1], None, access_key
            return "album", None, segments[1], access_key
    if "onelink.me" in parsed.netloc:
        # короткая ссылка из МТС; тип выясняется только после перехода, но отмечаем неизвестно
        return None, None, None, access_key

    # СберЗвук / Zvuk.com
    if "zvuk.com" in parsed.netloc:
        if segments and segments[0] in {"track", "song"} and len(segments) > 1:
            return "track", segments[1], None, access_key
        if segments and segments[0] in {"release", "album"} and len(segments) > 1:
            return "album", None, segments[1], access_key

    # Spotify
    if "open.spotify.com" in parsed.netloc:
        if segments and segments[0] in {"track", "album"} and len(segments) > 1:
            kind = segments[0]
            if kind == "track":
                return "track", segments[1], None, access_key
            return "album", None, segments[1], access_key

    return None, None, None, access_key


def _pick_first(data: Dict[str, str], keys) -> Optional[str]:
    for key in keys:
        val = data.get(key)
        if val:
            return val
    return None


def _split_artist_title(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not value:
        return None, None
    for sep in (" — ", " – ", " - ", ": "):
        if sep in value:
            left, right = value.split(sep, 1)
            return left.strip() or None, right.strip() or None
    return None, value.strip() or None


def _normalize_year(val) -> Optional[str]:
    """Приводит поле года к строке, поддерживая таймстамп в секундах."""
    if val is None:
        return None
    try:
        import datetime

        if isinstance(val, (int, float)):
            # если это таймстамп (например 1764163270), конвертим в год
            if val > 3000:
                return str(datetime.datetime.utcfromtimestamp(val).year)
            return str(int(val))
        # строки оставляем как есть
        return str(val)
    except Exception:
        return None


def _pick_year_from_parts(parts) -> Optional[str]:
    """Пытаемся достать 4-значный год из массива строк."""
    for p in parts:
        p_clean = (p or "").strip()
        if len(p_clean) == 4 and p_clean.isdigit():
            return p_clean
    return None


def _normalize_text(val: Optional[str]) -> str:
    text = (val or "").casefold()
    try:
        import re

        text = re.sub(r"[^\w]+", " ", text, flags=re.UNICODE)
    except Exception:
        pass
    return " ".join(text.split())


def _latinize_text(val: Optional[str]) -> str:
    """Грубая латинизация кириллицы для матчей между ru/en артистами."""
    if not val:
        return ""
    mapping = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "д": "d",
        "е": "e",
        "ё": "yo",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "й": "i",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "sh",
        "ъ": "",
        "ы": "y",
        "ь": "",
        "э": "e",
        "ю": "yu",
        "я": "ya",
    }
    out = []
    for ch in (val or "").casefold():
        out.append(mapping.get(ch, ch))
    return _normalize_text("".join(out))


def _consonant_skeleton(val: str) -> str:
    vowels = set("aeiouy")
    return "".join(ch for ch in val if ch.isalnum() and ch not in vowels)


def _token_overlap_match(a_text: str, b_text: str, min_overlap: float = 0.75) -> bool:
    a_tokens = [t for t in a_text.split() if t]
    b_tokens = [t for t in b_text.split() if t]
    if len(a_tokens) < 2 or len(b_tokens) < 2:
        return False
    a_set = set(a_tokens)
    b_set = set(b_tokens)
    if not a_set or not b_set:
        return False
    overlap = len(a_set & b_set) / len(a_set)
    return overlap >= min_overlap


def _clean_title_for_match(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    text = val
    try:
        import re

        keywords = (
            "mix",
            "remix",
            "edit",
            "version",
            "feat",
            "featuring",
            "ft",
            "radio",
            "extended",
            "club",
            "instrumental",
            "live",
            "demo",
            "bonus",
            "remaster",
            "remastered",
            "edition",
        )
        kw = "|".join(keywords)
        # удаляем скобочные уточнения с ключевыми словами
        text = re.sub(rf"\s*[\(\[\{{][^\)\]\}}]*\b({kw})\b[^\)\]\}}]*[\)\]\}}]", "", text, flags=re.IGNORECASE)
        # удаляем хвосты feat/ft
        text = re.sub(r"\s+(feat\.?|featuring|ft\.?)\s+.+$", "", text, flags=re.IGNORECASE)
        text = " ".join(text.split())
    except Exception:
        return val
    if len(text) < 3:
        return val
    return text


def _title_matches(title_norm: str, alt_norm: Optional[str], cand_title: str) -> bool:
    if title_norm and _contains_relaxed(title_norm, cand_title):
        return True
    if alt_norm and _contains_relaxed(alt_norm, cand_title):
        return True
    return False


def _contains_relaxed(needle: str, haystack: str, min_len: int = 4) -> bool:
    if not needle or not haystack:
        return False
    if needle in haystack:
        return True
    if len(haystack) >= min_len and haystack in needle:
        return True
    return False


def _is_specific_match(title_norm: str, artist_norm: str, cand_title: str, cand_artists: str) -> bool:
    if title_norm and not _contains_relaxed(title_norm, cand_title):
        return False
    if artist_norm and not _contains_relaxed(artist_norm, cand_artists):
        return False
    return True


def _artist_match_relaxed(artist_norm: str, cand_artists: str) -> bool:
    if not artist_norm:
        return True
    if _contains_relaxed(artist_norm, cand_artists):
        return True
    if _token_overlap_match(artist_norm, cand_artists):
        return True
    a_lat = _latinize_text(artist_norm)
    c_lat = _latinize_text(cand_artists)
    if a_lat and _contains_relaxed(a_lat, c_lat):
        return True
    if _token_overlap_match(a_lat, c_lat):
        return True
    a_lat_loose = a_lat.replace("h", "")
    c_lat_loose = c_lat.replace("h", "")
    if a_lat_loose and _contains_relaxed(a_lat_loose, c_lat_loose):
        return True
    a_skel = _consonant_skeleton(a_lat_loose)
    c_skel = _consonant_skeleton(c_lat_loose)
    if len(a_skel) >= 3 and len(c_skel) >= 3:
        if _contains_relaxed(a_skel, c_skel, min_len=3):
            return True
    return False


def _is_missing_link(val: Optional[str]) -> bool:
    return not val or val == "Не найдено"


def _is_generic_mts_og(og_tags: Dict[str, str], resolved_url: Optional[str]) -> bool:
    """Определяем заглушку главной страницы МТС Музыки, чтобы не использовать её как метаданные трека."""
    og_title = (og_tags.get("og:title") or "").casefold()
    og_desc = (og_tags.get("og:description") or "").casefold()
    og_image = og_tags.get("og:image") or ""
    final = (resolved_url or "").rstrip("/")
    if final == "https://music.mts.ru":
        if "кион музыка" in og_title or "слушать музыку" in og_title:
            return True
        if "кион музыка" in og_desc:
            return True
    if og_image.endswith("/mts-music-banner.png") and ("кион музыка" in og_title):
        return True
    return False


def _is_mts_direct_url(url: Optional[str]) -> bool:
    if not url:
        return False
    return ("music.mts.ru/track/" in url) or ("music.mts.ru/album/" in url)


def _apply_core_meta(meta: "SongMeta", data: Optional[Dict[str, str]], overwrite: bool) -> None:
    if not data:
        return
    for key in ("title", "album", "artist"):
        val = data.get(key)
        if val and (overwrite or not getattr(meta, key)):
            setattr(meta, key, val)
    year = data.get("year")
    if year and (overwrite or not meta.year):
        meta.year = year


def _apply_image(meta: "SongMeta", *images: Optional[str]) -> None:
    if meta.image:
        return
    for img in images:
        if img:
            meta.image = img
            return


def _ytmusic_enrich_from_url(url: str) -> Optional[Dict[str, str]]:
    """Берёт метаданные для music.youtube.com/watch через ytmusicapi."""
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(url)
    if parsed.hostname != "music.youtube.com":
        return None

    params = parse_qs(parsed.query)
    if parsed.path == "/watch":
        vid = params.get("v", [None])[0]
        if not vid:
            return None
        yt = _ytmusic_client()
        if not yt:
            return None
        try:
            data = yt.get_watch_playlist(vid)
        except Exception:
            return None
        tracks = data.get("tracks") or []
        track = None
        for t in tracks:
            if t.get("videoId") == vid:
                track = t
                break
        if not track and tracks:
            track = tracks[0]
        if not track:
            return None

        title = track.get("title")
        artists = ", ".join(a.get("name") for a in track.get("artists", []) if a.get("name"))
        album = (track.get("album") or {}).get("name")
        album_id = (track.get("album") or {}).get("id")
        year = None
        image = None
        if album_id:
            try:
                alb = yt.get_album(album_id)
                year = _normalize_year(alb.get("year"))
                thumbs = alb.get("thumbnails") or []
                if thumbs:
                    image = thumbs[-1].get("url")
            except Exception:
                pass

        return {
            "title": title,
            "artist": artists or None,
            "album": album,
            "year": year,
            "image": image,
            "ytmusic_url": f"https://music.youtube.com/watch?v={vid}",
            "kind": "track",
        }

    if parsed.path == "/playlist":
        list_id = params.get("list", [None])[0]
        if not list_id:
            return None
        yt = _ytmusic_client()
        if not yt:
            return None
        # Если это альбомный browseId
        if list_id.startswith("MPRE"):
            try:
                alb = yt.get_album(list_id)
            except Exception:
                return None
            artists = ", ".join(a.get("name") for a in alb.get("artists", []) if a.get("name"))
            thumbs = alb.get("thumbnails") or []
            image = thumbs[-1].get("url") if thumbs else None
            return {
                "title": alb.get("title"),
                "artist": artists or None,
                "album": alb.get("title"),
                "year": _normalize_year(alb.get("year")),
                "image": image,
                "ytmusic_url": f"https://music.youtube.com/playlist?list={list_id}",
                "kind": "album",
            }

        # Иначе пробуем плейлист (OLAK...)
        try:
            pl = yt.get_playlist(list_id)
        except Exception:
            return None
        tracks = pl.get("tracks") or []
        if not tracks:
            return None
        t0 = tracks[0]
        artists = ", ".join(a.get("name") for a in t0.get("artists", []) if a.get("name"))
        album = (t0.get("album") or {}).get("name") or pl.get("title")
        album_id = (t0.get("album") or {}).get("id")
        year = None
        image = None
        if album_id:
            try:
                alb = yt.get_album(album_id)
                year = _normalize_year(alb.get("year"))
                thumbs = alb.get("thumbnails") or []
                if thumbs:
                    image = thumbs[-1].get("url")
            except Exception:
                pass
        if not image:
            thumbs = pl.get("thumbnails") or []
            if thumbs:
                image = thumbs[-1].get("url")

        return {
            "title": pl.get("title"),
            "artist": artists or None,
            "album": album,
            "year": year,
            "image": image,
            "ytmusic_url": f"https://music.youtube.com/playlist?list={list_id}",
            "kind": "album",
        }

    return None


def _yandex_enrich_from_search(meta: "SongMeta") -> Optional[Dict[str, str]]:
    """Получает метаданные из публичного поиска ЯМузыки (без токена)."""
    base_title = meta.album if meta.kind == "album" and meta.album else meta.title or meta.album
    if not base_title:
        return None
    query_parts = [base_title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)

    alt_title = _clean_title_for_match(base_title)
    search_res = _yandex_search(query) or _yandex_search(base_title)
    if not search_res and alt_title and alt_title != base_title:
        search_res = _yandex_search(alt_title)
    if not search_res:
        return None

    tracks = (search_res.get("tracks") or {}).get("items") or []
    albums = (search_res.get("albums") or {}).get("items") or []
    if not tracks and not albums and alt_title and alt_title != base_title:
        search_res = _yandex_search(alt_title) or search_res
        tracks = (search_res.get("tracks") or {}).get("items") or []
        albums = (search_res.get("albums") or {}).get("items") or []

    title_norm = _normalize_text(base_title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != base_title else None
    artist_norm = _normalize_text(meta.artist)
    album_norm = _normalize_text(meta.album)
    alt_album = _clean_title_for_match(meta.album)
    alt_album_norm = _normalize_text(alt_album) if alt_album and alt_album != meta.album else None

    def score_track(t: Dict[str, str]) -> int:
        s = 0
        t_title = _normalize_text(t.get("title"))
        t_artists = " ".join(_normalize_text(a.get("name")) for a in t.get("artists", []) if a.get("name"))
        if _title_matches(title_norm, alt_title_norm, t_title):
            s += 2
        artist_match = _artist_match_relaxed(artist_norm, t_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        alb = (t.get("albums") or [None])[0] or {}
        alb_title = _normalize_text(alb.get("title"))
        if album_norm and _contains_relaxed(album_norm, alb_title):
            s += 1
        elif alt_album_norm and _contains_relaxed(alt_album_norm, alb_title):
            s += 1
        return s

    def score_album(a: Dict[str, str]) -> int:
        s = 0
        a_title = _normalize_text(a.get("title"))
        a_artists = " ".join(_normalize_text(ar.get("name")) for ar in a.get("artists", []) if ar.get("name"))
        if _contains_relaxed(album_norm, a_title) or (alt_album_norm and _contains_relaxed(alt_album_norm, a_title)):
            s += 2
        if _title_matches(title_norm, alt_title_norm, a_title):
            s += 1
        artist_match = _artist_match_relaxed(artist_norm, a_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        return s

    def pick_best(items, scorer):
        best = None
        best_score = 0
        for it in items:
            sc = scorer(it)
            if sc > best_score:
                best_score = sc
                best = it
        return best, best_score

    if meta.kind == "album":
        best_album, sc = pick_best(albums, score_album)
        if not best_album and tracks:
            # fallback: берем альбом из первого трека
            best_track, _ = pick_best(tracks, score_track)
            best_album = (best_track.get("albums") or [None])[0] if best_track else None
        if not best_album:
            return None
        artists = ", ".join(a.get("name") for a in best_album.get("artists", []) if a.get("name"))
        cover = _build_ym_cover(best_album.get("coverUri"))
        album_id = best_album.get("id")
        return {
            "title": best_album.get("title"),
            "album": best_album.get("title"),
            "artist": artists or None,
            "year": _normalize_year(best_album.get("year")),
            "image": cover,
            "album_id": str(album_id) if album_id else None,
            "yandex_url": f"https://music.yandex.ru/album/{album_id}" if album_id else None,
        }

    best_track, _ = pick_best(tracks, score_track)
    if not best_track and albums:
        best_album, _ = pick_best(albums, score_album)
        if best_album:
            album_id = best_album.get("id")
            artists = ", ".join(a.get("name") for a in best_album.get("artists", []) if a.get("name"))
            cover = _build_ym_cover(best_album.get("coverUri"))
            return {
                "title": best_album.get("title"),
                "album": best_album.get("title"),
                "artist": artists or None,
                "year": _normalize_year(best_album.get("year")),
                "image": cover,
                "album_id": str(album_id) if album_id else None,
                "yandex_url": f"https://music.yandex.ru/album/{album_id}" if album_id else None,
            }
    if not best_track:
        return None
    artists = ", ".join(a.get("name") for a in best_track.get("artists", []) if a.get("name"))
    alb = (best_track.get("albums") or [None])[0] or {}
    album_id = alb.get("id")
    cover_uri = alb.get("coverUri") or best_track.get("coverUri")
    cover = _build_ym_cover(cover_uri)
    track_id = best_track.get("id")
    return {
        "title": best_track.get("title"),
        "album": alb.get("title"),
        "artist": artists or None,
        "year": _normalize_year(best_track.get("year") or alb.get("year")),
        "image": cover,
        "track_id": str(track_id) if track_id else None,
        "album_id": str(album_id) if album_id else None,
        "yandex_url": (
            f"https://music.yandex.ru/album/{album_id}/track/{track_id}"
            if album_id and track_id
            else (f"https://music.yandex.ru/track/{track_id}" if track_id else None)
        ),
    }


def _mts_enrich_from_ids(track_id: Optional[str], album_id: Optional[str]) -> Optional[Dict[str, str]]:
    """Берет метаданные из MTS API по id Яндекса (если доступно)."""
    if track_id:
        t = _mts_api_get_track(track_id)
        if t:
            artists = ", ".join(a.get("name") for a in t.get("artists", []) if a.get("name"))
            album = t.get("album") or {}
            cover = _build_ym_cover(album.get("cover") or t.get("cover"))
            return {
                "title": t.get("title"),
                "artist": artists or None,
                "album": album.get("title"),
                "year": _normalize_year(t.get("year") or album.get("year")),
                "image": cover,
            }
    if album_id:
        a = _mts_api_get_album(album_id)
        if a:
            artists = ", ".join(ar.get("name") for ar in a.get("artists", []) if ar.get("name"))
            cover = _build_ym_cover(a.get("cover"))
            return {
                "title": a.get("title"),
                "album": a.get("title"),
                "artist": artists or None,
                "year": _normalize_year(a.get("year")),
                "image": cover,
            }
    return None


def _extract_yandex_ids_from_meta(meta: "SongMeta") -> Tuple[Optional[str], Optional[str]]:
    if meta.yandex_url and meta.yandex_url != "Не найдено":
        _, t_track, t_album, _ = parse_ids_from_url(meta.yandex_url)
        return t_track, t_album
    return None, None


def _mts_api_get_track(track_id: Optional[str]) -> Optional[Dict[str, str]]:
    if not track_id or not track_id.isdigit():
        return None
    try:
        resp = _get(
            f"{MTS_API_BASE}/tracks",
            params={"ids": track_id},
            headers=MTS_API_HEADERS,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("result") or []
        return items[0] if items else None
    except Exception:
        return None


def _mts_api_get_album(album_id: Optional[str]) -> Optional[Dict[str, str]]:
    if not album_id or not album_id.isdigit():
        return None
    try:
        resp = _get(
            f"{MTS_API_BASE}/albums/{album_id}",
            headers=MTS_API_HEADERS,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("result")
    except Exception:
        return None


def _mts_link_from_yandex(meta: "SongMeta") -> Optional[str]:
    t_track, t_album = _extract_yandex_ids_from_meta(meta)
    if meta.kind == "album":
        if t_album and _mts_api_get_album(t_album):
            return f"https://music.mts.ru/album/{t_album}"
        return None
    # track или неизвестно
    if t_track and _mts_api_get_track(t_track):
        return f"https://music.mts.ru/track/{t_track}"
    if t_album and meta.kind == "album" and _mts_api_get_album(t_album):
        return f"https://music.mts.ru/album/{t_album}"
    return None


def _ym_fetch_track(track_id: Optional[str]) -> Optional[Dict[str, str]]:
    """Запрос в официальное API ЯМузыки через OAuth токен для трека."""
    if not (YANDEX_TOKEN and track_id):
        return None
    try:
        resp = _get(
            f"https://api.music.yandex.net/tracks/{track_id}",
            headers={"Authorization": f"OAuth {YANDEX_TOKEN}"},
            params={"lang": "ru"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("result") or []
        if not items:
            return None
        t = items[0]
        artists = ", ".join(a.get("name") for a in t.get("artists", []) if a.get("name")) or None
        alb = (t.get("albums") or [None])[0] or {}
        album_title = alb.get("title")
        alb_id = alb.get("id")
        cover_uri = alb.get("coverUri") or t.get("coverUri")
        cover = _build_ym_cover(cover_uri)
        return {
            "title": t.get("title"),
            "artist": artists,
            "album": album_title,
            "album_id": str(alb_id) if alb_id else None,
            "track_id": str(t.get("id")) if t.get("id") else None,
            "year": _normalize_year(t.get("year")),
            "image": cover,
        }
    except Exception:
        return None


def _ym_fetch_album(album_id: Optional[str]) -> Optional[Dict[str, str]]:
    """Запрос в официальное API ЯМузыки через OAuth токен для альбома."""
    if not (YANDEX_TOKEN and album_id):
        return None
    try:
        resp = _get(
            f"https://api.music.yandex.net/albums/{album_id}",
            headers={"Authorization": f"OAuth {YANDEX_TOKEN}"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        a = data.get("result") or {}
        if not a:
            return None
        artists = ", ".join(ar.get("name") for ar in a.get("artists", []) if ar.get("name")) or None
        cover = _build_ym_cover(a.get("coverUri") or a.get("ogImage"))
        return {
            "title": a.get("title"),
            "artist": artists,
            "album": a.get("title"),
            "album_id": str(a.get("id")) if a.get("id") else None,
            "year": _normalize_year(a.get("year")),
            "image": cover,
        }
    except Exception:
        return None


def _build_ym_cover(uri: Optional[str], size: str = "1000x1000") -> Optional[str]:
    if not uri:
        return None
    if "%%" in uri:
        uri = uri.replace("%%", size)
    if uri.startswith("http"):
        return uri
    return f"https://{uri.lstrip('/')}"


def _ym_fetch(track_id: Optional[str], album_id: Optional[str], kind: Optional[str] = None) -> Optional[Dict[str, str]]:
    # Используем только REST (OAuth) для получения метаданных
    if track_id:
        return _ym_fetch_track(track_id)
    if kind == "track":
        return None
    if album_id:
        return _ym_fetch_album(album_id)
    return None


# ---------- YouTube Music helpers ----------
def _ytmusic_client():
    global _YTMUSIC_CLIENT  # noqa: PLW0603
    if _YTMUSIC_CLIENT is not None:
        return _YTMUSIC_CLIENT
    try:
        from ytmusicapi import YTMusic  # type: ignore
        if YTMUSIC_HEADERS_JSON:
            import json as _json
            try:
                hdrs = _json.loads(YTMUSIC_HEADERS_JSON)
            except Exception:
                import base64
                try:
                    hdrs = _json.loads(base64.b64decode(YTMUSIC_HEADERS_JSON).decode("utf-8"))
                except Exception:
                    hdrs = None
            if hdrs:
                _YTMUSIC_CLIENT = YTMusic(auth=hdrs)
                return _YTMUSIC_CLIENT
        headers_path = Path(YTMUSIC_HEADERS_PATH)
        if headers_path.exists():
            _YTMUSIC_CLIENT = YTMusic(str(headers_path))
        else:
            _YTMUSIC_CLIENT = None
    except Exception:
        _YTMUSIC_CLIENT = None
    return _YTMUSIC_CLIENT


def _match_ytmusic(meta: "SongMeta") -> Optional[str]:
    """Ищет трек или альбом в YouTube Music."""
    yt = _ytmusic_client()
    if not yt:
        return None
    base_title = meta.title or meta.album
    if not base_title:
        return None
    is_album = meta.kind == "album"
    query_parts = [base_title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)

    search_filter = "albums" if is_album else "songs"
    fallback_to_songs = False
    alt_query = None
    try:
        results = yt.search(query, filter=search_filter, limit=5)
    except Exception:
        return None
    if not results:
        alt_title = _clean_title_for_match(base_title)
        if alt_title and alt_title != base_title:
            alt_parts = [alt_title]
            if meta.artist:
                alt_parts.append(meta.artist)
            alt_query = " ".join(p for p in alt_parts if p)
            try:
                results = yt.search(alt_query, filter=search_filter, limit=5)
            except Exception:
                return None
        if not results and is_album:
            # если это сингл, пробуем искать как трек
            fallback_to_songs = True
            try:
                results = yt.search(query, filter="songs", limit=5)
            except Exception:
                return None
            if not results and alt_query:
                try:
                    results = yt.search(alt_query, filter="songs", limit=5)
                except Exception:
                    return None
        if not results:
            return None

    title_norm = _normalize_text(base_title)
    alt_title = _clean_title_for_match(base_title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != base_title else None
    artist_norm = _normalize_text(meta.artist)

    def score(item):
        s = 0
        it_title = _normalize_text(item.get("title"))
        artists = " ".join(_normalize_text(a.get("name")) for a in item.get("artists", []) if a.get("name"))
        title_match = _title_matches(title_norm, alt_title_norm, it_title)
        artist_match = _artist_match_relaxed(artist_norm, artists)
        if not title_match or not artist_match:
            return 0
        if title_match:
            s += 2
        if artist_match:
            s += 2
        return s

    best = None
    best_score = 0
    for it in results:
        sc = score(it)
        if sc > best_score:
            best_score = sc
            best = it
    if not best or best_score == 0:
        return None

    if is_album and not fallback_to_songs:
        playlist_id = best.get("playlistId")
        browse_id = best.get("browseId")
        if playlist_id:
            return f"https://music.youtube.com/playlist?list={playlist_id}"
        if browse_id:
            return f"https://music.youtube.com/playlist?list={browse_id}"
    else:
        vid = best.get("videoId") or best.get("videoId")
        if vid:
            return f"https://music.youtube.com/watch?v={vid}"
    return None


def _ytmusic_enrich(meta: "SongMeta") -> Optional[Dict[str, str]]:
    yt = _ytmusic_client()
    if not yt:
        return None
    base_title = meta.title or meta.album
    if not base_title:
        return None
    query_parts = [base_title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)
    try:
        results = yt.search(query, filter="songs", limit=5)
    except Exception:
        return None
    if not results:
        alt_title = _clean_title_for_match(base_title)
        if alt_title and alt_title != base_title:
            alt_parts = [alt_title]
            if meta.artist:
                alt_parts.append(meta.artist)
            alt_query = " ".join(p for p in alt_parts if p)
            try:
                results = yt.search(alt_query, filter="songs", limit=5)
            except Exception:
                return None
        if not results:
            return None
    title_norm = _normalize_text(base_title)
    alt_title = _clean_title_for_match(base_title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != base_title else None
    artist_norm = _normalize_text(meta.artist)
    it = None
    for cand in results:
        c_title = _normalize_text(cand.get("title"))
        c_artists = " ".join(_normalize_text(a.get("name")) for a in cand.get("artists", []) if a.get("name"))
        if _title_matches(title_norm, alt_title_norm, c_title) and _artist_match_relaxed(artist_norm, c_artists):
            it = cand
            break
    if not it:
        return None
    artists = ", ".join(a.get("name") for a in it.get("artists", []) if a.get("name"))
    album = (it.get("album") or {}).get("name")
    return {
        "title": it.get("title"),
        "artist": artists or None,
        "album": album,
        "year": None,  # YT search не даёт года
        "image": (it.get("thumbnails") or [{}])[-1].get("url"),
    }


# ---------- Spotify helpers ----------
_SPOTIFY_TOKEN: Optional[str] = None
_SPOTIFY_TOKEN_EXP: float = 0.0


def _spotify_get_token() -> Optional[str]:
    global _SPOTIFY_TOKEN, _SPOTIFY_TOKEN_EXP  # pylint: disable=global-statement
    import time

    if _SPOTIFY_TOKEN and _SPOTIFY_TOKEN_EXP - time.time() > 60:
        return _SPOTIFY_TOKEN
    if not (SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET):
        return None
    try:
        resp = _post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            auth=(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET),
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _SPOTIFY_TOKEN = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        _SPOTIFY_TOKEN_EXP = time.time() + expires_in
        return _SPOTIFY_TOKEN
    except Exception:
        return None


def _match_spotify(meta: "SongMeta") -> Optional[str]:
    """Поиск трека/альбома в Spotify."""
    # Базовый заголовок: для альбома берем album, для трека — title
    base_title = meta.album if meta.kind == "album" and meta.album else meta.title or meta.album
    if not base_title:
        return None
    is_album = meta.kind == "album"
    token = _spotify_get_token()
    if not token:
        return None

    title_norm = _normalize_text(base_title)
    alt_title = _clean_title_for_match(base_title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != base_title else None
    artist_norm = _normalize_text(meta.artist)

    query_parts = [base_title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)

    def do_search(q: str, search_album: bool):
        try:
            search_type = "album" if search_album else "track"
            params = {"q": q, "type": search_type, "limit": 10}
            if SPOTIFY_MARKET:
                params["market"] = SPOTIFY_MARKET
            resp = _get(
                "https://api.spotify.com/v1/search",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            key = "albums" if search_album else "tracks"
            return (data.get(key) or {}).get("items", [])
        except Exception:
            return []

    # Основной поиск (по типу сущности)
    items = do_search(query, search_album=is_album)
    if not items:
        items = do_search(base_title, search_album=is_album)
    if not items and alt_title_norm:
        items = do_search(alt_title, search_album=is_album)
    if not items and meta.artist:
        # пробуем запрос вида track/album:"..." artist:"..."
        kind_prefix = "album" if is_album else "track"
        items = do_search(f'{kind_prefix}:"{base_title}" artist:"{meta.artist}"', search_album=is_album)
    if not items and meta.artist and alt_title_norm:
        kind_prefix = "album" if is_album else "track"
        items = do_search(f'{kind_prefix}:"{alt_title}" artist:"{meta.artist}"', search_album=is_album)
    # Если искали альбом и не нашли — попробуем по трекам и возьмём ссылку альбома
    album_from_tracks = None
    if is_album and not items:
        track_items = do_search(query, search_album=False)
        if not track_items:
            track_items = do_search(base_title, search_album=False)
        if not track_items and alt_title_norm:
            track_items = do_search(alt_title, search_album=False)
        if not track_items and meta.artist:
            track_items = do_search(f'track:"{base_title}" artist:"{meta.artist}"', search_album=False)
        if not track_items and meta.artist and alt_title_norm:
            track_items = do_search(f'track:"{alt_title}" artist:"{meta.artist}"', search_album=False)
        if track_items:
            # берем первый совпадающий трек и возвращаем ссылку на его альбом
            best_track = None
            for t in track_items:
                t_title = _normalize_text(t.get("name"))
                t_artists = " ".join(_normalize_text(a.get("name")) for a in t.get("artists", []) if a.get("name"))
                if _title_matches(title_norm, alt_title_norm, t_title) and _artist_match_relaxed(artist_norm, t_artists):
                    best_track = t
                    break
            if not best_track:
                return None
            album_url = best_track.get("album", {}).get("external_urls", {}).get("spotify")
            if album_url:
                album_from_tracks = album_url
    if not items:
        return None

    def score(item: Dict[str, str]) -> int:
        s = 0
        it_title = _normalize_text(item.get("name"))
        artists = " ".join(_normalize_text(a.get("name")) for a in item.get("artists", []) if a.get("name"))
        title_match = _title_matches(title_norm, alt_title_norm, it_title)
        artist_match = _artist_match_relaxed(artist_norm, artists)
        if not title_match or not artist_match:
            return 0
        if title_match:
            s += 2
        if artist_match:
            s += 2
        if meta.album and item.get("album") and item["album"].get("name"):
            if _normalize_text(meta.album) in _normalize_text(item["album"]["name"]):
                s += 1
        return s

    best = None
    best_score = 0
    for it in items:
        sc = score(it)
        if sc > best_score:
            best_score = sc
            best = it
    if not best or best_score == 0:
        best = None
    if best:
        if is_album:
            url = best.get("external_urls", {}).get("spotify")
            if not url:
                url = best.get("album", {}).get("external_urls", {}).get("spotify")
            if url:
                return url
        else:
            url = best.get("external_urls", {}).get("spotify")
            if url:
                return url
    if album_from_tracks:
        return album_from_tracks
    return None


def _spotify_enrich(meta: "SongMeta") -> Optional[Dict[str, str]]:
    """Возвращает метаданные из Spotify (title/artist/album/year/image) по поиску."""
    token = _spotify_get_token()
    if not token:
        return None
    base_title = meta.album if meta.kind == "album" and meta.album else meta.title or meta.album
    if not base_title:
        return None
    query_parts = [base_title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)
    try:
        params = {"q": query, "type": "track", "limit": 5}
        if SPOTIFY_MARKET:
            params["market"] = SPOTIFY_MARKET
        resp = _get(
            "https://api.spotify.com/v1/search",
            params=params,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
        items = (resp.json().get("tracks") or {}).get("items", [])
        if not items:
            alt_title = _clean_title_for_match(base_title)
            if alt_title and alt_title != base_title:
                alt_query_parts = [alt_title]
                if meta.artist:
                    alt_query_parts.append(meta.artist)
                alt_query = " ".join(p for p in alt_query_parts if p)
                resp = _get(
                    "https://api.spotify.com/v1/search",
                    params={"q": alt_query, "type": "track", "limit": 5},
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                resp.raise_for_status()
                items = (resp.json().get("tracks") or {}).get("items", [])
        if not items:
            return None
        title_norm = _normalize_text(base_title)
        alt_title = _clean_title_for_match(base_title)
        alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != base_title else None
        artist_norm = _normalize_text(meta.artist)
        it = None
        for cand in items:
            c_title = _normalize_text(cand.get("name"))
            c_artists = " ".join(_normalize_text(a.get("name")) for a in cand.get("artists", []) if a.get("name"))
            if _title_matches(title_norm, alt_title_norm, c_title) and _artist_match_relaxed(artist_norm, c_artists):
                it = cand
                break
        if not it:
            return None
        artists = ", ".join(a.get("name") for a in it.get("artists", []) if a.get("name"))
        album = it.get("album") or {}
        images = album.get("images") or []
        cover = images[0]["url"] if images else None
        return {
            "title": it.get("name"),
            "artist": artists or None,
            "album": album.get("name"),
            "year": (album.get("release_date") or "")[:4] or None,
            "image": cover,
        }
    except Exception:
        return None


def _spotify_enrich_from_url(spotify_url: str) -> Optional[Dict[str, str]]:
    """Достаёт метаданные по прямой ссылке Spotify (track или album)."""
    token = _spotify_get_token()
    if not token or not spotify_url:
        return None
    try:
        path = urlparse(spotify_url).path.strip("/")
        parts = path.split("/")
        if len(parts) < 2:
            return None
        kind, sid = parts[0], parts[1]
        if not sid:
            return None
        endpoint = None
        if kind == "track":
            endpoint = f"https://api.spotify.com/v1/tracks/{sid}"
        elif kind == "album":
            endpoint = f"https://api.spotify.com/v1/albums/{sid}"
        if not endpoint:
            return None
        resp = _get(endpoint, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if kind == "track":
            album = data.get("album") or {}
            images = album.get("images") or []
            cover = images[0]["url"] if images else None
            artists = ", ".join(a.get("name") for a in data.get("artists", []) if a.get("name"))
            return {
                "title": data.get("name"),
                "artist": artists or None,
                "album": album.get("name"),
                "year": (album.get("release_date") or "")[:4] or None,
                "image": cover,
            }
        if kind == "album":
            images = data.get("images") or []
            cover = images[0]["url"] if images else None
            artists = ", ".join(a.get("name") for a in data.get("artists", []) if a.get("name"))
            return {
                "title": data.get("name"),
                "artist": artists or None,
                "album": data.get("name"),
                "year": (data.get("release_date") or "")[:4] or None,
                "image": cover,
            }
    except Exception:
        return None
    return None


def _mts_search(query: str) -> Optional[Dict[str, List[Dict[str, str]]]]:
    """Возвращает searchResult из __NEXT_DATA__ на music.mts.ru/search?text=..."""
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = _get("https://music.mts.ru/search", params={"text": query}, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception:
        return None
    from bs4 import BeautifulSoup  # type: ignore

    soup = BeautifulSoup(resp.text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    try:
        data = json.loads(script.string)
        return data.get("props", {}).get("pageProps", {}).get("searchResult")
    except Exception:
        return None


def _match_mts(meta: "SongMeta") -> Optional[str]:
    """Ищет ссылку в МТС по названию/исполнителю."""
    if not meta.title:
        return None
    title_norm = _normalize_text(meta.title)
    alt_title = _clean_title_for_match(meta.title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != meta.title else None
    artist_norm = _normalize_text(meta.artist)
    album_norm = _normalize_text(meta.album)
    alt_album = _clean_title_for_match(meta.album)
    alt_album_norm = _normalize_text(alt_album) if alt_album and alt_album != meta.album else None

    def do_search(q: str) -> Optional[Dict[str, List[Dict[str, str]]]]:
        if not q.strip():
            return None
        return _mts_search(q)

    # сначала полный запрос, потом fallback только по названию
    search_res = do_search(" ".join(p for p in (meta.title, meta.artist) if p))
    if not search_res:
        search_res = do_search(meta.title)
    if not search_res and alt_title_norm:
        search_res = do_search(alt_title)
    if not search_res:
        return _mts_link_from_yandex(meta)

    tracks = search_res.get("tracks") or []
    albums = search_res.get("albums") or []
    if not tracks and not albums and alt_title_norm:
        search_res = do_search(alt_title) or search_res
        tracks = search_res.get("tracks") or []
        albums = search_res.get("albums") or []

    def score_track(t: Dict[str, str]) -> int:
        s = 0
        t_title = _normalize_text(t.get("title"))
        t_artists = " ".join(_normalize_text(a.get("name")) for a in t.get("artists", []) if a.get("name"))
        if not _title_matches(title_norm, alt_title_norm, t_title):
            return 0
        s += 2
        artist_match = _artist_match_relaxed(artist_norm, t_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        album_title = _normalize_text(t.get("albumTitle"))
        if album_norm and album_norm in album_title:
            s += 1
        elif alt_album_norm and alt_album_norm in album_title:
            s += 1
        return s

    def score_album(a: Dict[str, str]) -> int:
        s = 0
        a_title = _normalize_text(a.get("title"))
        a_artists = " ".join(_normalize_text(ar.get("name")) for ar in a.get("artists", []) if ar.get("name"))
        if not _title_matches(album_norm or title_norm, alt_album_norm or alt_title_norm, a_title):
            return 0
        if _contains_relaxed(album_norm, a_title) or (alt_album_norm and _contains_relaxed(alt_album_norm, a_title)):
            s += 2
        if _title_matches(title_norm, alt_title_norm, a_title):
            s += 1
        artist_match = _artist_match_relaxed(artist_norm, a_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        return s

    if meta.kind == "album":
        best_album = None
        best_score = 0
        for a in albums:
            sc = score_album(a)
            if sc > best_score:
                best_score = sc
                best_album = a
        if best_album and best_score >= 1:
            alb_id = best_album.get("id")
            if alb_id:
                return f"https://music.mts.ru/album/{alb_id}"
        # fallback: взять альбом из треков
        for t in tracks:
            if _normalize_text(t.get("albumTitle")) == album_norm:
                alb_id = t.get("albumId")
                if alb_id:
                    return f"https://music.mts.ru/album/{alb_id}"
        return _mts_link_from_yandex(meta)

    # track
    best_track = None
    best_score = 0
    for t in tracks:
        sc = score_track(t)
        if sc > best_score:
            best_score = sc
            best_track = t
    if best_track and best_score >= 1:
        tid = best_track.get("id")
        if tid:
            return f"https://music.mts.ru/track/{tid}"
    return _mts_link_from_yandex(meta)


def _yandex_search(query: str) -> Optional[Dict[str, List[Dict[str, str]]]]:
    """Поиск по ЯМузыке (официальный API с OAuth при наличии токена, иначе public handlers)."""
    if YANDEX_TOKEN:
        try:
            resp = _get(
                "https://api.music.yandex.net/search",
                params={"text": query, "type": "all", "page": 0, "nocorrect": "true"},
                headers={"Authorization": f"OAuth {YANDEX_TOKEN}"},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            res = resp.json().get("result", {})
            tracks = (res.get("tracks") or {}).get("results") or []
            albums = (res.get("albums") or {}).get("results") or []
            return {
                "tracks": {"items": tracks},
                "albums": {"items": albums},
            }
        except Exception:
            pass
    headers = {"User-Agent": USER_AGENT}
    params = {
        "text": query,
        "type": "all",
        "page": 0,
        "playlist-infinite": "true",
    }
    try:
        resp = _get(
            "https://music.yandex.ru/handlers/music-search.jsx",
            params=params,
            headers=headers,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _match_yandex(meta: "SongMeta") -> Optional[str]:
    """Ищет ссылку в ЯМузыке по названию/исполнителю."""
    if not meta.title:
        return None
    title_norm = _normalize_text(meta.title)
    alt_title = _clean_title_for_match(meta.title)
    alt_title_norm = _normalize_text(alt_title) if alt_title and alt_title != meta.title else None
    artist_norm = _normalize_text(meta.artist)
    album_norm = _normalize_text(meta.album)
    alt_album = _clean_title_for_match(meta.album)
    alt_album_norm = _normalize_text(alt_album) if alt_album and alt_album != meta.album else None

    def do_search(q: str) -> Optional[Dict[str, List[Dict[str, str]]]]:
        if not q.strip():
            return None
        return _yandex_search(q)

    # сначала полный запрос, потом fallback только по названию
    search_res = do_search(" ".join(p for p in (meta.title, meta.artist) if p))
    if not search_res:
        search_res = do_search(meta.title)
    if not search_res and alt_title_norm:
        search_res = do_search(alt_title)
    if not search_res:
        return None

    tracks = (search_res.get("tracks") or {}).get("items") or []
    albums = (search_res.get("albums") or {}).get("items") or []
    if not tracks and not albums and alt_title_norm:
        search_res = do_search(alt_title) or search_res
        tracks = (search_res.get("tracks") or {}).get("items") or []
        albums = (search_res.get("albums") or {}).get("items") or []

    def score_track(t: Dict[str, str]) -> int:
        s = 0
        t_title = _normalize_text(t.get("title"))
        t_artists = " ".join(_normalize_text(a.get("name")) for a in t.get("artists", []) if a.get("name"))
        if not _title_matches(title_norm, alt_title_norm, t_title):
            return 0
        s += 2
        artist_match = _artist_match_relaxed(artist_norm, t_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        alb = (t.get("albums") or [None])[0] or {}
        alb_title = _normalize_text(alb.get("title"))
        if album_norm and album_norm in alb_title:
            s += 1
        elif alt_album_norm and alt_album_norm in alb_title:
            s += 1
        return s

    def score_album(a: Dict[str, str]) -> int:
        s = 0
        a_title = _normalize_text(a.get("title"))
        a_artists = " ".join(_normalize_text(ar.get("name")) for ar in a.get("artists", []) if ar.get("name"))
        if not _title_matches(album_norm or title_norm, alt_album_norm or alt_title_norm, a_title):
            return 0
        if _contains_relaxed(album_norm, a_title) or (alt_album_norm and _contains_relaxed(alt_album_norm, a_title)):
            s += 2
        if _title_matches(title_norm, alt_title_norm, a_title):
            s += 1
        artist_match = _artist_match_relaxed(artist_norm, a_artists)
        if artist_norm and not artist_match:
            return 0
        if artist_match:
            s += 2
        return s

    if meta.kind == "album":
        best_album = None
        best_score = 0
        for a in albums:
            sc = score_album(a)
            if sc > best_score:
                best_score = sc
                best_album = a
        if best_album and best_score >= 1:
            aid = best_album.get("id")
            if aid:
                return f"https://music.yandex.ru/album/{aid}"
        # fallback: альбом из трека
        for t in tracks:
            alb = (t.get("albums") or [None])[0] or {}
            if album_norm and album_norm == _normalize_text(alb.get("title")):
                aid = alb.get("id")
                if aid:
                    return f"https://music.yandex.ru/album/{aid}"
        return None

    # track
    best_track = None
    best_score = 0
    for t in tracks:
        sc = score_track(t)
        if sc > best_score:
            best_score = sc
            best_track = t
    if best_track and best_score >= 1:
        tid = best_track.get("id")
        alb = (best_track.get("albums") or [None])[0] or {}
        aid = alb.get("id")
        if tid and aid:
            return f"https://music.yandex.ru/album/{aid}/track/{tid}"
        if tid:
            return f"https://music.yandex.ru/track/{tid}"
    return None


def _yandex_album_info(album_id: Optional[str]) -> Optional[Dict[str, str]]:
    """Получает информацию об альбоме ЯМузыки по id (title/year/cover)."""
    if not album_id:
        return None
    if YANDEX_TOKEN:
        data = _ym_fetch_album(album_id)
        if data:
            return {
                "title": data.get("title"),
                "year": data.get("year"),
                "cover": data.get("image"),
                "artist": data.get("artist"),
            }
    try:
        resp = _get(
            "https://music.yandex.ru/handlers/album.jsx",
            params={"album": album_id, "lang": "ru"},
            headers={"User-Agent": USER_AGENT},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        cover = _build_ym_cover(data.get("coverUri"))
        artists = [a.get("name") for a in data.get("artists", []) if a.get("name")]
        return {
            "title": data.get("title"),
            "year": _normalize_year(data.get("year")),
            "cover": cover,
            "artist": ", ".join(artists) if artists else None,
        }
    except Exception:
        return None


def _match_vk(meta: "SongMeta", token: Optional[str]) -> Optional[str]:
    """Поиск в VK через audio.search (нужен токен с правом audio)."""
    if not token or not meta.title:
        return None
    title_norm = _normalize_text(meta.title)
    artist_norm = _normalize_text(meta.artist)
    album_norm = _normalize_text(meta.album)

    query_parts = [meta.title]
    if meta.artist:
        query_parts.append(meta.artist)
    query = " ".join(p for p in query_parts if p)

    try:
        resp = _vk_call("audio.search", {"q": query, "count": 5}, token)
    except Exception:
        return None
    items = resp.get("items") if isinstance(resp, dict) else []
    if not items:
        return None

    def score(item: Dict[str, str]) -> int:
        s = 0
        it_title = _normalize_text(item.get("title"))
        it_artist = _normalize_text(item.get("artist"))
        if title_norm and title_norm in it_title:
            s += 2
        if artist_norm and artist_norm in it_artist:
            s += 2
        if album_norm and item.get("album") and isinstance(item["album"], dict):
            alb_title = _normalize_text(item["album"].get("title"))
            if album_norm in alb_title:
                s += 1
        return s

    best = None
    best_score = 0
    for it in items:
        sc = score(it)
        if sc > best_score:
            best_score = sc
            best = it
    if best and best_score >= 2:
        if meta.kind == "album" and isinstance(best.get("album"), dict):
            alb = best["album"]
            owner = alb.get("owner_id") or best.get("owner_id")
            aid = alb.get("id")
            access = alb.get("access_hash") or alb.get("access_key") or best.get("access_key")
            if owner and aid:
                suffix = f"_{access}" if access else ""
                return f"https://vk.com/music/album/{owner}_{aid}{suffix}"
        owner = best.get("owner_id")
        tid = best.get("id")
        access = best.get("access_key")
        if owner and tid:
            suffix = f"_{access}" if access else ""
            return f"https://vk.com/audio{owner}_{tid}{suffix}"
    return None


def _vk_call(method: str, params: Dict[str, str], token: str) -> Dict[str, str]:
    payload = {"access_token": token, "v": "5.199"}
    payload.update(params)
    headers = {}
    url = f"https://{VK_API_HOST}/method/{method}"
    if VK_API_HOST != "api.vk.com":
        headers["Host"] = "api.vk.com"
    verify = True
    # при использовании IP сертификат не совпадает, отключаем проверку
    if VK_API_HOST != "api.vk.com":
        verify = False
    try:
        resp = _post(url, data=payload, headers=headers, timeout=15, verify=verify)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException:
        # Если override хоста не сработал, попробуем api.vk.com напрямую
        if VK_API_HOST != "api.vk.com":
            fallback = _post(
                "https://api.vk.com/method/{0}".format(method),
                data=payload,
                timeout=15,
            )
            fallback.raise_for_status()
            data = fallback.json()
        else:
            raise

    if "error" in data:
        raise RuntimeError(f"VK API error: {data['error']}")
    return data["response"]


def _vk_fetch_track(track_id: str, token: str) -> Dict[str, str]:
    # ожидаем owner_track (например -2001899747_144899747)
    resp = _vk_call("audio.getById", {"audios": track_id}, token)
    items = resp if isinstance(resp, list) else resp.get("items")
    if not items:
        raise RuntimeError("VK: трек не найден")
    item = items[0]
    artists = [a.get("name") for a in item.get("main_artists", []) if a.get("name")] or [item.get("artist")]
    album = item.get("album") or {}
    cover = None
    if isinstance(album, dict) and album.get("thumb"):
        cover = album["thumb"].get("photo_1200") or album["thumb"].get("photo_600")
    year = None
    if isinstance(album, dict):
        year = album.get("year") or album.get("release_year")
    year = year or item.get("year") or item.get("date")
    year = _normalize_year(year)
    return {
        "title": item.get("title"),
        "artist": ", ".join([a for a in artists if a]) if artists else None,
        "album": album.get("title") if isinstance(album, dict) else None,
        "cover": cover,
        "year": year,
        "duration": item.get("duration"),
    }


def _vk_fetch_playlist(playlist_id: str, access_key: Optional[str], token: str) -> Dict[str, str]:
    # playlist_id в формате owner_playlist (например -2000956728_25956728)
    if "_" not in playlist_id:
        raise RuntimeError("VK: playlist_id должен быть owner_playlist")
    owner_raw, pl_raw = playlist_id.split("_", 1)
    resp = _vk_call(
        "audio.getPlaylistById",
        {
            "owner_id": owner_raw,
            "playlist_id": pl_raw,
            "need_playlist": 1,
            "need_tracks": 1,
            **({"access_key": access_key, "access_hash": access_key} if access_key else {}),
        },
        token,
    )
    playlist = resp.get("playlist") if isinstance(resp, dict) else None
    # у некоторых токенов playlist лежит прямо в корне response
    if not playlist and isinstance(resp, dict):
        playlist = resp
    tracks = resp.get("audios", []) if isinstance(resp, dict) else []
    cover = None
    first_track = None
    if playlist:
        for t in playlist.get("thumbs", []):
            if t.get("width", 0) >= 600:
                cover = t.get("url")
                break
        # fallback по полям photo_... (photo объект)
        if not cover and isinstance(playlist.get("photo"), dict):
            photo = playlist["photo"]
            for key in ("photo_1200", "photo_600", "photo_300", "photo_270"):
                if photo.get(key):
                    cover = photo[key]
                    break
    if tracks:
        t0 = tracks[0]
        first_track = {
            "title": t0.get("title"),
            "artist": t0.get("artist"),
            "album": (t0.get("album") or {}).get("title") if isinstance(t0.get("album"), dict) else None,
        }
    # Основные исполнители
    artists = []
    if playlist:
        artists = [a.get("name") for a in playlist.get("main_artists", []) if a.get("name")]
    return {
        "title": playlist.get("title") if playlist else None,
        "album": playlist.get("title") if playlist else None,
        "artist": ", ".join(artists) if artists else None,
        "description": playlist.get("description") if playlist else None,
        "cover": cover,
        "tracks_count": len(tracks),
        "year": playlist.get("year") if playlist else None,
        "first_track": first_track,
    }


def _apply_cross_links(meta: "SongMeta") -> None:
    """Устанавливает взаимные ссылки между ЯМузыкой и МТС, если совпадают id."""
    if meta.track_id:
        if meta.service == "yandex":
            meta.yandex_url = meta.yandex_url or meta.source_url
            meta.mts_url = meta.mts_url or f"https://music.mts.ru/track/{meta.track_id}"
        elif meta.service == "mts":
            meta.mts_url = meta.mts_url or meta.source_url
            meta.yandex_url = meta.yandex_url or f"https://music.yandex.ru/track/{meta.track_id}"
    if meta.album_id and meta.kind == "album":
        if meta.service == "yandex":
            meta.yandex_url = meta.yandex_url or meta.source_url
            meta.mts_url = meta.mts_url or f"https://music.mts.ru/album/{meta.album_id}"
        elif meta.service == "mts":
            meta.mts_url = meta.mts_url or meta.source_url
            meta.yandex_url = meta.yandex_url or f"https://music.yandex.ru/album/{meta.album_id}"


def _enrich_vk_from_search(meta: "SongMeta") -> None:
    """Для VK/BOOM: заменяем title/artist/album результатами поиска по токенам."""
    # 1) Яндекс поиск — основной источник, т.к. чаще совпадает по каталогу
    y_meta = _yandex_enrich_from_search(meta)
    core_set = False
    if y_meta:
        _apply_core_meta(meta, y_meta, overwrite=True)
        core_set = True
        if y_meta.get("yandex_url"):
            meta.yandex_url = y_meta["yandex_url"]

    # 2) MTS метаданные по id Яндекса (если доступны)
    t_track, t_album = _extract_yandex_ids_from_meta(meta)
    m_meta = _mts_enrich_from_ids(t_track, t_album)
    if m_meta:
        _apply_core_meta(meta, m_meta, overwrite=not core_set)
        core_set = core_set or bool(m_meta)
    if not meta.mts_url or meta.mts_url == "Не найдено":
        mts_link = _mts_link_from_yandex(meta)
        if mts_link:
            meta.mts_url = mts_link

    # 3) Spotify/YouTube Music как запасные источники
    s_meta = _spotify_enrich(meta)
    if s_meta and not core_set:
        _apply_core_meta(meta, s_meta, overwrite=True)
        core_set = True
    ytm_meta = _ytmusic_enrich(meta)
    if ytm_meta and not core_set:
        _apply_core_meta(meta, ytm_meta, overwrite=True)
        core_set = True

    # Дополняем пустые поля из остальных источников
    _apply_core_meta(meta, s_meta, overwrite=False)
    _apply_core_meta(meta, ytm_meta, overwrite=False)

    # Обложка: берём первую доступную из любых источников
    _apply_image(
        meta,
        (y_meta or {}).get("image"),
        (m_meta or {}).get("image"),
        (s_meta or {}).get("image"),
        (ytm_meta or {}).get("image"),
    )


def _fill_missing_image(meta: "SongMeta") -> None:
    """Если нет обложки — берём из любого доступного API/сервиса."""
    if meta.image:
        return
    candidates = []

    # Яндекс: по прямой ссылке/ids
    y_track, y_album = _extract_yandex_ids_from_meta(meta)
    if y_track or y_album:
        ym_info = _ym_fetch(y_track, y_album) or _yandex_album_info(y_album)
        if ym_info and ym_info.get("image"):
            candidates.append(ym_info.get("image"))
        elif ym_info and ym_info.get("cover"):
            candidates.append(ym_info.get("cover"))

    # МТС: по ids (обычно совпадают с Яндекс)
    if y_track or y_album:
        m_meta = _mts_enrich_from_ids(y_track, y_album)
        if m_meta and m_meta.get("image"):
            candidates.append(m_meta.get("image"))

    # Spotify / YouTube Music
    if meta.spotify_url and not _is_missing_link(meta.spotify_url):
        s_meta = _spotify_enrich_from_url(meta.spotify_url) or _spotify_enrich(meta)
        if s_meta and s_meta.get("image"):
            candidates.append(s_meta.get("image"))
    if meta.ytmusic_url and not _is_missing_link(meta.ytmusic_url):
        ytm_meta = _ytmusic_enrich(meta)
        if ytm_meta and ytm_meta.get("image"):
            candidates.append(ytm_meta.get("image"))

    _apply_image(meta, *candidates)


def normalize_song_meta(url: str, og_tags: Dict[str, str], resolved_url: Optional[str] = None) -> SongMeta:
    url_for_parse = resolved_url or url
    service = detect_service(url_for_parse) or detect_service(url)
    kind, track_id, album_id, access_key = parse_ids_from_url(url_for_parse)
    # если после редиректа идентификаторы потерялись (VK уводит на /audio), fallback на исходный URL
    if not track_id and not album_id:
        fallback_kind, fallback_track, fallback_album, fallback_key = parse_ids_from_url(url)
        kind = kind or fallback_kind
        track_id = track_id or fallback_track
        album_id = album_id or fallback_album
        access_key = access_key or fallback_key

    og_tags_for_parse = og_tags
    if service == "mts" and _is_generic_mts_og(og_tags, url_for_parse):
        og_tags_for_parse = {}

    title = _pick_first(og_tags_for_parse, ("og:title", "title", "music:song", "music:album"))
    description = _pick_first(og_tags_for_parse, ("og:description", "description"))

    artist = _pick_first(og_tags_for_parse, ("music:musician", "music:artist", "vk:music:artist", "ya:music:artist"))
    album = _pick_first(og_tags_for_parse, ("music:album", "ya:music:album"))
    year = _pick_first(og_tags_for_parse, ("music:release_date", "ya:music:year"))

    # Пытаемся вытащить artist/title из заголовка или description
    # Если в artist лежит ссылка (например, music.yandex.ru/artist/...), не используем её как имя
    if artist and artist.startswith("http"):
        artist = None

    artist_from_title = False
    if not artist:
        artist_candidate, title_clean = _split_artist_title(title)
        if artist_candidate:
            artist = artist_candidate
            artist_from_title = True
        title = title_clean
    # VK часто отдаёт рекламное описание, не используем его для метаданных
    if service == "vk" and description:
        desc_lower = description.lower()
        if "слушайте в vk музыке" in desc_lower or "любимые треки" in desc_lower:
            description = None
    if description:
        structured_desc = "·" in description or "•" in description
        # Форматы ЯМузыки: "Исполнитель • Трек • 2025" или "Исполнитель · Альбом · 2025 ..."
        parts = [p.strip() for p in description.replace("·", "•").split("•") if p.strip()]
        if parts and (not artist or (service == "mts" and artist_from_title)):
            artist = parts[0]
        if not album and len(parts) >= 2 and parts[1].lower() not in {"трек", "track", "альбом", "album", "сингл", "single"}:
            album = parts[1]
        artist_desc, title_desc = _split_artist_title(description)
        artist = artist or artist_desc
        if not title:
            title = title_desc
        # если description выглядит как "Исполнитель - Альбом", а уже есть title, то title_desc можно трактовать как альбом
        if not album and title_desc and title and title_desc != title and not structured_desc:
            album = title_desc

    image = _pick_first(og_tags_for_parse, ("og:image", "og:image:url"))

    # Попытка вытащить album_id из URL обложки (часто в яндекс/мтс картинках есть .<id>-N/).
    if not album_id and image:
        import re

        m = re.search(r"\.(\d+)-\d+/", image)
        if m:
            album_id = m.group(1)

    # Специфика share.boom.ru: og:title "Партизан - Хаски" (альбом - артист) или "Я боюсь - Хаски" (трек - артист)
    if "share.boom.ru" in url.lower() and service == "vk":
        og_title = og_tags.get("og:title")
        a_left, a_right = _split_artist_title(og_title)
        if kind == "album":
            if a_left and a_right:
                album = a_left
                title = a_left
                artist = a_right
        elif kind == "track":
            if a_left and a_right:
                title = a_left
                artist = a_right

    # Определение kind/года по описанию (актуально для mts onelink)
    if description and not kind and service == "mts":
        desc_lower = description.lower()
        if "альбом" in desc_lower or "album" in desc_lower:
            kind = "album"
        elif "трек" in desc_lower or "track" in desc_lower:
            kind = "track"

    if description and not year:
        parts = [p.strip() for p in description.replace("·", "•").split("•") if p.strip()]
        year = _pick_year_from_parts(parts) or year

    meta = SongMeta(
        title=title,
        album=album,
        artist=artist,
        year=year,
        image=image,
        source_url=url,
        resolved_url=url_for_parse,
        service=service,
        kind=kind,
        track_id=track_id,
        album_id=album_id,
        access_key=access_key,
        raw=og_tags,
    )

    if meta.service == "ytmusic":
        yt_meta = _ytmusic_enrich_from_url(meta.source_url or meta.resolved_url)
        if yt_meta:
            _apply_core_meta(meta, yt_meta, overwrite=True)
            meta.year = yt_meta.get("year") or meta.year
            meta.image = yt_meta.get("image") or meta.image
            meta.kind = meta.kind or yt_meta.get("kind")
            if yt_meta.get("ytmusic_url"):
                meta.ytmusic_url = yt_meta.get("ytmusic_url")

    if meta.service == "spotify":
        meta.spotify_url = meta.spotify_url or meta.source_url
        s_meta = _spotify_enrich_from_url(meta.spotify_url)
        if s_meta:
            _apply_core_meta(meta, s_meta, overwrite=True)
            meta.year = s_meta.get("year") or meta.year
            meta.image = s_meta.get("image") or meta.image

    # Если это ссылка vk.com (не boom), сразу ставим исходный URL как vk_url без поиска
    src_host = urlparse(url).hostname or ""
    if service == "vk" and ("vk.com" in src_host or "vk.ru" in src_host):
        meta.vk_url = url

    # Для альбомов, если название есть, а поле album отсутствует или содержит заглушку "Альбом" — ставим album = title
    if meta.kind == "album" and meta.title and (not meta.album or meta.album.strip().lower() == "альбом"):
        meta.album = meta.title
    # Для синглов: если album = "Сингл", используем название трека
    if meta.kind == "track" and meta.title and meta.album and meta.album.strip().lower() in {"сингл", "single"}:
        meta.album = meta.title

    _apply_cross_links(meta)

    # Если есть токен ЯМузыки — дополним/уточним данные (избегая капчи)
    if meta.service == "yandex":
        ym_data = _ym_fetch(meta.track_id, meta.album_id, meta.kind)
        if ym_data:
            meta.title = ym_data.get("title") or meta.title
            meta.artist = ym_data.get("artist") or meta.artist
            meta.album = ym_data.get("album") or meta.album
            meta.year = ym_data.get("year") or meta.year
            meta.image = ym_data.get("image") or meta.image
            meta.track_id = ym_data.get("track_id") or meta.track_id
            meta.album_id = ym_data.get("album_id") or meta.album_id
            # подставим прямую ссылку, если нет
            if meta.track_id and meta.album_id and not meta.yandex_url:
                meta.yandex_url = f"https://music.yandex.ru/album/{meta.album_id}/track/{meta.track_id}"
            elif meta.album_id and not meta.yandex_url:
                meta.yandex_url = f"https://music.yandex.ru/album/{meta.album_id}"
    return meta


def main() -> None:
    parser = argparse.ArgumentParser(description="Извлечь OG-теги трека/альбома")
    parser.add_argument("url", help="Ссылка на трек/альбом (ЯМузыка/ВК/МТС Музыка)")
    parser.add_argument("--json", action="store_true", help="Вывести результат в JSON")
    parser.add_argument("--dump-html", metavar="PATH", help="Сохранить HTML ответа для отладки")
    parser.add_argument("--vk-token-file", default="vk_tokens.json", help="Файл с VK access_token (для ссылок VK)")
    args = parser.parse_args()

    service_hint = detect_service(args.url)
    if service_hint is None and _is_vk_host(urlparse(args.url).hostname or ""):
        print("Ссылка VK не относится к музыке. Пропускаю.")
        return
    if service_hint is None and _is_youtube_host(urlparse(args.url).hostname or ""):
        print("Ссылка YouTube не относится к поддерживаемым музыкальным сервисам. Пропускаю.")
        return
    kind_hint, track_hint, album_hint, access_hint = parse_ids_from_url(args.url)
    if service_hint == "spotify" and not (kind_hint or track_hint or album_hint):
        print("Ссылка Spotify не относится к треку/альбому. Пропускаю.")
        return
    meta: Optional[SongMeta] = None
    resolved_url: Optional[str] = None
    og_tags: Dict[str, str] = {}
    try:
        og_tags, resolved_url = fetch_og_tags(args.url, dump_html=args.dump_html)
    except requests.HTTPError as e:
        print(f"HTTP ошибка: {e.response.status_code} {e.response.reason}")
        return
    except requests.RequestException as e:
        # Если это VK и не смогли скачать OG (например, DNS/блок), продолжаем без OG, опираясь на URL и VK API.
        if service_hint == "vk":
            og_tags, resolved_url = {}, None
        elif service_hint == "yandex":
            ym_data = _ym_fetch(track_hint, album_hint, kind_hint)
            if ym_data:
                meta = SongMeta(
                    title=ym_data.get("title"),
                    album=ym_data.get("album"),
                    artist=ym_data.get("artist"),
                    year=ym_data.get("year"),
                    image=ym_data.get("image"),
                    source_url=args.url,
                    resolved_url=args.url,
                    service="yandex",
                    kind=kind_hint or ("track" if ym_data.get("track_id") else "album"),
                    track_id=ym_data.get("track_id") or track_hint,
                    album_id=ym_data.get("album_id") or album_hint,
                    access_key=access_hint,
                    raw={},
                )
                _apply_cross_links(meta)
            else:
                # Даже если не получили YM API, продолжим с пустыми OG (чтобы не падать из-за капчи)
                og_tags, resolved_url = {}, args.url
        else:
            print(f"Ошибка запроса: {e}")
            return
    except RuntimeError as e:
        if service_hint == "yandex":
            ym_data = _ym_fetch(track_hint, album_hint, kind_hint)
            if ym_data:
                meta = SongMeta(
                    title=ym_data.get("title"),
                    album=ym_data.get("album"),
                    artist=ym_data.get("artist"),
                    year=ym_data.get("year"),
                    image=ym_data.get("image"),
                    source_url=args.url,
                    resolved_url=args.url,
                    service="yandex",
                    kind=kind_hint or ("track" if ym_data.get("track_id") else "album"),
                    track_id=ym_data.get("track_id") or track_hint,
                    album_id=ym_data.get("album_id") or album_hint,
                    access_key=access_hint,
                    raw={},
                )
                _apply_cross_links(meta)
            else:
                # Если капча, продолжаем без OG, используя только ids из URL
                og_tags, resolved_url = {}, args.url
        else:
            print(f"Ошибка парсинга: {e}")
            return

    if meta is None:
        if og_tags:
            meta = normalize_song_meta(args.url, og_tags, resolved_url=resolved_url)
        else:
            # даже если OG нет (часто VK дает заглушку), попробуем распарсить по URL и дернуть VK API
            kind, track_id, album_id, access_key = parse_ids_from_url(resolved_url or args.url)
            if not track_id and not album_id:
                fallback_kind, fallback_track, fallback_album, fallback_key = parse_ids_from_url(args.url)
                kind = kind or fallback_kind
                track_id = track_id or fallback_track
                album_id = album_id or fallback_album
                access_key = access_key or fallback_key
            meta = SongMeta(
                title=None,
                album=None,
                artist=None,
                year=None,
                image=None,
                source_url=args.url,
                resolved_url=resolved_url or args.url,
                service=detect_service(resolved_url or args.url) or detect_service(args.url),
                kind=kind,
                track_id=track_id,
                album_id=album_id,
                access_key=access_key,
                raw={},
            )
            _apply_cross_links(meta)
            # Если это ссылка vk.com (не boom), сразу ставим исходный URL как vk_url без поиска
            src_host = urlparse(args.url).hostname or ""
            if meta.service == "vk" and ("vk.com" in src_host or "vk.ru" in src_host) and not meta.vk_url:
                meta.vk_url = args.url

    # Для МТС: пытаемся привести onelink к прямой ссылке и заполнить ids
    if meta.service == "mts":
        # если есть ids — предпочитаем прямую ссылку
        if not _is_mts_direct_url(meta.mts_url):
            if meta.kind == "album" and meta.album_id:
                meta.mts_url = f"https://music.mts.ru/album/{meta.album_id}"
            elif meta.kind == "track" and meta.track_id:
                meta.mts_url = f"https://music.mts.ru/track/{meta.track_id}"
            elif _is_mts_direct_url(meta.source_url):
                meta.mts_url = meta.source_url
        # пробуем найти прямую ссылку поиском по МТС, если её нет
        if not _is_mts_direct_url(meta.mts_url):
            mts_link = _match_mts(meta)
            if mts_link:
                meta.mts_url = mts_link
        # если получили прямую ссылку — проставим ids и кросс-ссылки
        if _is_mts_direct_url(meta.mts_url):
            k, t_id, a_id, _ = parse_ids_from_url(meta.mts_url)
            meta.kind = meta.kind or k
            meta.track_id = meta.track_id or t_id
            meta.album_id = meta.album_id or a_id
            _apply_cross_links(meta)
        elif meta.mts_url and "onelink.me" in meta.mts_url:
            if not (meta.title or meta.artist or meta.album):
                meta.mts_url = "Не найдено"
        if not meta.mts_url and "onelink.me" in meta.source_url and (meta.title or meta.artist or meta.album):
            meta.mts_url = meta.source_url

    # Для Яндекс -> попробуем найти ссылку в МТС через поиск, если нет прямой кросс-ссылки
    if meta.service == "yandex" and not meta.mts_url:
        mts_link = _match_mts(meta)
        meta.mts_url = mts_link or "Не найдено"
    # Для МТС -> попробуем найти ссылку в ЯМузыке через поиск, если нет прямой кросс-ссылки
    if meta.service == "mts" and not meta.yandex_url:
        y_link = _match_yandex(meta)
        meta.yandex_url = y_link or "Не найдено"
    if meta.service == "mts" and meta.yandex_url and meta.yandex_url != "Не найдено":
        k, t_id, a_id, _ = parse_ids_from_url(meta.yandex_url)
        meta.kind = meta.kind or k
        meta.track_id = meta.track_id or t_id
        meta.album_id = meta.album_id or a_id
        if not _is_mts_direct_url(meta.mts_url):
            if meta.track_id:
                meta.mts_url = f"https://music.mts.ru/track/{meta.track_id}"
            elif meta.kind == "album" and meta.album_id:
                meta.mts_url = f"https://music.mts.ru/album/{meta.album_id}"
        _apply_cross_links(meta)
    # Для МТС: если прямую ссылку так и не получили — помечаем как не найдено
    if meta.service == "mts" and not meta.mts_url:
        meta.mts_url = "Не найдено"
    # Для МТС: уточняем данные альбома/трека через API ЯМузыки (по album_id/track_id)
    if meta.service == "mts":
        ym_info = _ym_fetch(meta.track_id, meta.album_id, meta.kind)
        if ym_info:
            meta.title = ym_info.get("title") or meta.title
            meta.artist = ym_info.get("artist") or meta.artist
            meta.album = ym_info.get("album") or meta.album
            meta.year = ym_info.get("year") or meta.year
            meta.image = ym_info.get("image") or meta.image
            meta.track_id = ym_info.get("track_id") or meta.track_id
            meta.album_id = ym_info.get("album_id") or meta.album_id
    # Для МТС: если есть album_id — подтянем корректные данные по альбому, но не затираем название трека
    if meta.service == "mts" and meta.album_id:
        info = _yandex_album_info(meta.album_id)
        if info:
            if meta.kind == "album" or not meta.title:
                meta.title = info.get("title") or meta.title
            if (not meta.album) or (meta.album.strip().lower() in {"альбом", "album"}) or ("трек" in meta.album.lower()):
                meta.album = info.get("title") or meta.album
            if not meta.artist:
                meta.artist = info.get("artist") or meta.artist
            if not meta.year:
                meta.year = info.get("year") or meta.year
            if not meta.image:
                meta.image = info.get("cover") or meta.image
    # Для Яндекс: обогащение через API по id (для обхода капчи и получения ISRC)
    if meta.service == "yandex":
        ym_info = _ym_fetch(meta.track_id, meta.album_id, meta.kind)
        if ym_info:
            meta.title = ym_info.get("title") or meta.title
            meta.artist = ym_info.get("artist") or meta.artist
            meta.album = ym_info.get("album") or meta.album
            meta.year = ym_info.get("year") or meta.year
            meta.image = ym_info.get("image") or meta.image
            meta.track_id = ym_info.get("track_id") or meta.track_id
            meta.album_id = ym_info.get("album_id") or meta.album_id

    # Для YouTube Music: попробуем найти ссылки в ЯМузыке и МТС по токенам
    if meta.service == "ytmusic":
        if not meta.yandex_url or meta.yandex_url == "Не найдено":
            y_link = _match_yandex(meta)
            meta.yandex_url = y_link or "Не найдено"
        if not meta.mts_url or meta.mts_url == "Не найдено":
            mts_link = _match_mts(meta)
            meta.mts_url = mts_link or "Не найдено"

    # Для Spotify: пробуем найти ссылки в ЯМузыке и МТС по токенам
    if meta.service == "spotify":
        if not meta.yandex_url or meta.yandex_url == "Не найдено":
            y_link = _match_yandex(meta)
            meta.yandex_url = y_link or "Не найдено"
        if not meta.mts_url or meta.mts_url == "Не найдено":
            mts_link = _match_mts(meta)
            meta.mts_url = mts_link or "Не найдено"
    # Поиск в VK для любых сервисов (если не VK) при наличии токена с audio
    token_path = Path(args.vk_token_file)
    token = _load_vk_access_token(token_path)
    # Поиск в VK для любых сервисов при наличии токена с audio
    if token:
        if meta.service != "vk":
            vk_link = _match_vk(meta, token)
            meta.vk_url = vk_link or meta.vk_url
        else:
            # даже для VK ссылок (share.boom и т.п.) можно попытаться найти "чистую" ссылку
            if not meta.vk_url:
                vk_link = _match_vk(meta, token)
                meta.vk_url = vk_link or meta.vk_url
        if meta.service != "vk" and not meta.vk_url and (meta.title or meta.album):
            meta.vk_url = "Не найдено"

    # Если ссылка VK и есть access_token — попробуем дополнить данные через VK API
    vk_data = None
    if meta.service == "vk":
        token = _load_vk_access_token(token_path)
        if token:
            try:
                if meta.kind == "track" and meta.track_id and "_" in meta.track_id:
                    vk_data = _vk_fetch_track(meta.track_id, token)
                elif meta.kind == "album" and meta.album_id and "_" in meta.album_id:
                    vk_data = _vk_fetch_playlist(meta.album_id, meta.access_key, token)
                elif meta.kind == "track" and meta.vk_url:
                    # share.boom: track_id может быть без owner_id; попробуем разобрать из найденной vk_url
                    t_kind, t_track, _, t_key = parse_ids_from_url(meta.vk_url)
                    if t_track and "_" in t_track:
                        meta.track_id = t_track
                        meta.access_key = meta.access_key or t_key
                        vk_data = _vk_fetch_track(t_track, token)
                # Если это альбом share.boom и у нас появилась vk_url с owner_id/access_hash — дернем playlist, чтобы взять год/обложку
                if not vk_data and meta.kind == "album" and meta.vk_url:
                    a_kind, _, a_album, a_key = parse_ids_from_url(meta.vk_url)
                    if a_album and "_" in a_album:
                        meta.album_id = meta.album_id or a_album
                        meta.access_key = meta.access_key or a_key
                        vk_data = _vk_fetch_playlist(a_album, a_key or meta.access_key, token)
            except Exception as err:  # pylint: disable=broad-except
                print(f"Не удалось получить данные VK API: {err}")
        else:
            print("VK токен не найден (env VK_ACCESS_TOKEN или vk_tokens.json). Пропускаю VK API.")

    if vk_data:
        meta.title = vk_data.get("title") or meta.title
        meta.artist = vk_data.get("artist") or meta.artist
        meta.album = vk_data.get("album") or meta.album
        meta.image = vk_data.get("cover") or meta.image
        meta.year = _normalize_year(vk_data.get("year")) or meta.year
        first_track = vk_data.get("first_track") if isinstance(vk_data, dict) else None
    else:
        first_track = None

    # Если это VK и есть id + access_key — сформируем ссылку даже при ошибках API
    if meta.service == "vk":
        if meta.kind == "album" and meta.album_id and meta.access_key and not meta.vk_url:
            meta.vk_url = f"https://vk.com/music/album/{meta.album_id}_{meta.access_key}"
        if meta.kind == "track" and meta.track_id and meta.access_key and not meta.vk_url:
            meta.vk_url = f"https://vk.com/audio{meta.track_id}_{meta.access_key}"
        # После обогащения данными VK пытаемся найти кросс-ссылки
        if not meta.yandex_url or meta.yandex_url == "Не найдено":
            y_link = _match_yandex(meta)
            if not y_link and first_track:
                temp = SongMeta(
                    title=first_track.get("title"),
                    album=first_track.get("album") or meta.album,
                    artist=first_track.get("artist") or meta.artist,
                    year=None,
                    image=None,
                    source_url=meta.source_url,
                    resolved_url=meta.resolved_url,
                    service=meta.service,
                    kind="track",
                    track_id=None,
                    album_id=None,
                    access_key=None,
                    raw={},
                )
                y_link = _match_yandex(temp)
                # если нашли трек ЯМузыки — возьмём оттуда ids
                if y_link:
                    _, t_track, t_album, _ = parse_ids_from_url(y_link)
                    meta.track_id = meta.track_id or t_track
                    meta.album_id = meta.album_id or t_album
            meta.yandex_url = y_link or "Не найдено"
        if not meta.mts_url or meta.mts_url == "Не найдено":
            mts_link = _match_mts(meta)
            if not mts_link and first_track:
                temp = SongMeta(
                    title=first_track.get("title"),
                    album=first_track.get("album") or meta.album,
                    artist=first_track.get("artist") or meta.artist,
                    year=None,
                    image=None,
                    source_url=meta.source_url,
                    resolved_url=meta.resolved_url,
                    service=meta.service,
                    kind="track",
                    track_id=None,
                    album_id=None,
                    access_key=None,
                    raw={},
                )
                mts_link = _match_mts(temp)
                if mts_link:
                    _, t_track, t_album, _ = parse_ids_from_url(mts_link)
                    meta.track_id = meta.track_id or t_track
                    meta.album_id = meta.album_id or t_album
            meta.mts_url = mts_link or "Не найдено"

        # Перезаполняем title/album/artist по поисковым API, а не по OG
        _enrich_vk_from_search(meta)

    # Spotify ссылка через поиск по названию/исполнителю (после всех обогащений)
    if not meta.spotify_url:
        spotify_link = _match_spotify(meta)
        if spotify_link:
            meta.spotify_url = spotify_link
        elif meta.title or meta.album:
            meta.spotify_url = "Не найдено"
    # YouTube Music ссылка через поиск
    if not meta.ytmusic_url:
        yt_link = _match_ytmusic(meta)
        if yt_link:
            meta.ytmusic_url = yt_link
        elif meta.title or meta.album:
            meta.ytmusic_url = "Не найдено"

    # Если обложки нет — попробуем получить из API других сервисов
    _fill_missing_image(meta)

    # Если нет ссылок на yandex/vk/mts (все null/Не найдено) — попробуем улучшить метаданные из Spotify/YouTube Music
    links_missing = all(
        not v or v == "Не найдено" for v in (meta.yandex_url, meta.vk_url, meta.mts_url)
    )
    if links_missing:
        if meta.spotify_url and not _is_missing_link(meta.spotify_url):
            s_meta = _spotify_enrich_from_url(meta.spotify_url) or _spotify_enrich(meta)
            if s_meta:
                meta.title = s_meta.get("title") or meta.title
                meta.artist = s_meta.get("artist") or meta.artist
                meta.album = s_meta.get("album") or meta.album
                meta.year = s_meta.get("year") or meta.year
                meta.image = s_meta.get("image") or meta.image
        if meta.ytmusic_url and not _is_missing_link(meta.ytmusic_url):
            y_meta = _ytmusic_enrich(meta)
            if y_meta:
                meta.title = y_meta.get("title") or meta.title
                meta.artist = y_meta.get("artist") or meta.artist
                meta.album = y_meta.get("album") or meta.album
                meta.year = y_meta.get("year") or meta.year
                meta.image = y_meta.get("image") or meta.image
    if args.json:
        print(json.dumps(asdict(meta), ensure_ascii=False, indent=2))
        return

    print("Нормализованные данные:")
    print(f"  Источник URL: {meta.source_url}")
    print(f"  Финальный URL: {meta.resolved_url}")
    print(f"  Сервис:   {meta.service}")
    print(f"  Тип:      {meta.kind}")
    print(f"  Название: {meta.title}")
    print(f"  Альбом:   {meta.album}")
    print(f"  Исполнитель: {meta.artist}")
    print(f"  Год:      {meta.year}")
    print(f"  Обложка:  {meta.image}")
    print(f"  Track ID: {meta.track_id}")
    print(f"  Album ID: {meta.album_id}")
    if meta.yandex_url:
        print(f"  Yandex URL: {meta.yandex_url}")
    if meta.mts_url:
        print(f"  MTS URL: {meta.mts_url}")

    print("\nСырые OG-теги:")
    for k, v in sorted(meta.raw.items()):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
