## Music Resolver (локальный прототип)

CLI-утилита для извлечения метаданных треков/альбомов из OG-тегов по ссылке на Яндекс Музыку, VK Музыку или МТС Музыку.

### Запуск
1. Установите зависимости: `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. Вызовите: `python app.py <url>`
   - `--json` чтобы получить JSON
   - `--dump-html debug.html` чтобы сохранить HTML ответа (если OG-теги не нашлись)

Пример: `python app.py https://music.yandex.ru/album/1234/track/5678`

### Что делает сейчас
- Загружает страницу по ссылке, парсит OG/music/vk/ya теги и печатает найденные поля.
- Эвристики под разные форматы ссылок ЯМузыки/ВК/МТС: пытается определить тип (трек/альбом), ids, сервис.
- Следует редиректам (например, onelink МТС), выводит исходный и финальный URL.
- Для VK: если есть `vk_tokens.json` с `access_token`, пытается дополнить данные через VK API (треки/плейлисты). Если после редиректа идентификаторы потерялись, берет их из исходной ссылки.

### Что планируется
- Допилить маппинг реальных OG-тегов с живых страниц (нужны примеры ответов).
- Поиск соответствий в других сервисах через их публичные/непубличные эндпоинты или HTML-поиск.
- Подготовка ответа для Telegram-бота (кнопки, текст) и интеграция в n8n workflow.

### Как быстро протестировать ссылки из `links.json`
1. Активируйте venv: `source .venv/bin/activate`
2. Разбор URL без сети (убедиться, что парсится тип/id/сервис):
```
python - <<'PY'
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(".").resolve()))
import app
links = [l.strip() for l in Path("../links.json").read_text().splitlines()
         if l.strip() and not l.strip().startswith("//")]
for url in links:
    kind, tid, aid = app.parse_ids_from_url(url)
    svc = app.detect_service(url)
    print(url)
    print({"service": svc, "kind": kind, "track_id": tid, "album_id": aid})
    print("-"*40)
PY
```
3. С OG-тегами (нужен интернет, может требовать RU IP/авторизацию):
```
python - <<'PY'
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(".").resolve()))
import app
links = [l.strip() for l in Path("../links.json").read_text().splitlines()
         if l.strip() and not l.strip().startswith("//")]
for url in links:
    print("=== ", url)
    try:
        tags, final_url = app.fetch_og_tags(url)
        meta = app.normalize_song_meta(url, tags, resolved_url=final_url)
        print(json.dumps(meta.__dict__, ensure_ascii=False, indent=2))
    except Exception as e:
        print("ERROR:", e)
PY
```
