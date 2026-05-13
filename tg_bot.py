"""
Telegram-бот для ручного запуску SERP-парсингу.
Запускається паралельно зі Streamlit або як окремий воркер.

Змінні середовища:
  TG_BOT_TOKEN   — токен бота
  TG_CHAT_ID     — твій особистий chat id (для авторизації та відправки)
"""

import asyncio
import datetime
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict

# ==========================
# ENV
# ==========================
os.environ.setdefault("TG_BOT_TOKEN", "8683656869:AAEZb8YZmgjUFCHXVFQ1f_C6qq-Nx64dBKU")
os.environ.setdefault("TG_CHAT_ID",   "909587225")

from parser.api_keys import load_projects
from parser.parser_core import run_project
from parser.excel_export import export_to_excel

TOKEN   = os.environ.get("TG_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TG_CHAT_ID", "")   # твій особистий user/chat id

HISTORY_FILE = "data/history.json"

# ==========================
# LOW-LEVEL API
# ==========================

def tg_post(method: str, payload: dict) -> dict:
    """POST-запит до Bot API"""
    url  = f"https://api.telegram.org/bot{TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        print(f"[TG] {method} HTTP {e.code}: {body}")
        return {}
    except Exception as e:
        print(f"[TG] {method} error: {e}")
        return {}


def tg_get(method: str, params: dict = None) -> dict:
    """GET-запит до Bot API (getUpdates, getMe)"""
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=35) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        print(f"[TG] {method} HTTP {e.code}: {body}")
        return {}
    except Exception as e:
        print(f"[TG] {method} error: {e}")
        return {}


def get_updates(offset=None) -> list:
    params = {"timeout": 25, "allowed_updates": "message,callback_query"}
    if offset is not None:
        params["offset"] = offset
    result = tg_get("getUpdates", params)
    return result.get("result", [])


def send_message(chat_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_post("sendMessage", payload)


def edit_message(chat_id, message_id, text, reply_markup=None, parse_mode="HTML"):
    payload = {
        "chat_id":    chat_id,
        "message_id": message_id,
        "text":       text,
        "parse_mode": parse_mode,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_post("editMessageText", payload)


def answer_callback(callback_query_id, text=""):
    tg_post("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


def send_document(chat_id, filepath, caption=""):
    url      = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    boundary = "----BoundaryBOT777"
    filename = os.path.basename(filepath)

    with open(filepath, "rb") as f:
        file_data = f.read()

    def field(name, value):
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode("utf-8")

    body = (
        field("chat_id", str(chat_id))
        + field("caption", caption)
        + field("parse_mode", "HTML")
        + (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'
            f"Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n"
        ).encode("utf-8")
        + file_data
        + f"\r\n--{boundary}--\r\n".encode("utf-8")
    )

    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"[TG doc] error: {e}")
        return False


# ==========================
# HISTORY helpers
# ==========================

def load_history() -> list:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history_entry(project: dict, results: list):
    history   = load_history()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {
        "timestamp":      timestamp,
        "project":        project["name"],
        "location":       project["location"],
        "pages":          project["pages"],
        "target_domains": project.get("target_domains", []) or [],
        "results":        results,
    }
    history.append(entry)
    if len(history) > 50:
        history = history[-50:]
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def get_target_root(domain: str, target_domains: list):
    d = normalize_domain(domain)
    for t in target_domains:
        t_norm = normalize_domain(t)
        if t_norm and (d == t_norm or d.endswith("." + t_norm)):
            return t_norm
    return None


def enrich_results(results: list, target_domains: list) -> list:
    enriched = []
    for r in results:
        domain_for_check = r.get("domain_clean") or r.get("domain", "")
        root = get_target_root(domain_for_check, target_domains)
        enriched.append({**r, "is_target": bool(root), "target_root": root})
    return enriched


# ==========================
# REPORT builder
# ==========================

def build_tg_report(proj: dict, res: list, duration: float,
                    end_time: datetime.datetime, pages_used: int) -> str:
    dom_stats = defaultdict(lambda: {"positions": [], "keywords": set()})
    for r in res:
        if not r.get("is_target"):
            continue
        pos = r.get("position")
        if not isinstance(pos, int):
            continue
        root = r.get("target_root") or r.get("domain", "")
        dom_stats[root]["positions"].append(pos)
        dom_stats[root]["keywords"].add(r["keyword"])

    domain_lines = []
    for dom in sorted(dom_stats.keys()):
        positions = dom_stats[dom]["positions"]
        kw_count  = len(dom_stats[dom]["keywords"])
        if not positions:
            continue
        t1_3    = sum(1 for p in positions if p <= 3)
        t4_10   = sum(1 for p in positions if 4  <= p <= 10)
        t11_20  = sum(1 for p in positions if 11 <= p <= 20)
        t21_50  = sum(1 for p in positions if 21 <= p <= 50)
        t51_100 = sum(1 for p in positions if 51 <= p <= 100)
        avg     = round(sum(positions) / len(positions), 1)

        bars = ""
        if t1_3:    bars += f"🥇 1–3: {t1_3}  "
        if t4_10:   bars += f"🟢 4–10: {t4_10}  "
        if t11_20:  bars += f"🟡 11–20: {t11_20}  "
        if t21_50:  bars += f"🟠 21–50: {t21_50}  "
        if t51_100: bars += f"🔴 51–100: {t51_100}"

        domain_lines.append(
            f"🌐 <b>{dom}</b>\n"
            f"   {bars.strip()}\n"
            f"   📌 KW у видачі: {kw_count}  |  ⌀ поз: {avg}"
        )

    domains_block = "\n\n".join(domain_lines) if domain_lines else "— немає попадань у видачі —"
    total_kw = len({r["keyword"] for r in res})

    return (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SERP ЗВІТ</b>  [ручний]\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📁 Проєкт: <b>{proj['name']}</b>\n"
        f"🗓 {end_time.strftime('%d.%m.%Y  %H:%M')}\n"
        f"🌍 {proj.get('location', '')}  |  gl={proj.get('gl', '')}  hl={proj.get('hl', '')}\n"
        f"⏱ Час парсингу: {duration:.0f} сек\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Ключів: {total_kw}  |  📄 Сторінок: {pages_used}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>ПОЗИЦІЇ ПО ДОМЕНАХ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{domains_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


# ==========================
# KEYBOARDS
# ==========================

def kb_main_menu():
    return {
        "inline_keyboard": [
            [{"text": "🚀  Запустити парсинг",  "callback_data": "menu:parse"}],
            [{"text": "📋  Мої проєкти",         "callback_data": "menu:projects"}],
            [{"text": "📈  Остання статистика",   "callback_data": "menu:stats"}],
        ]
    }


def kb_projects(projects):
    rows = []
    for i, p in enumerate(projects):
        kw_count = len(p.get("keywords", []))
        rows.append([{"text": f"📁 {p['name']}  ({kw_count} KW)", "callback_data": f"parse:proj:{i}"}])
    rows.append([{"text": "◀️  Назад", "callback_data": "menu:main"}])
    return {"inline_keyboard": rows}


def kb_pages(proj_idx):
    options = [1, 2, 3, 5, 10]
    row     = [{"text": f"{p} стор.", "callback_data": f"parse:pages:{proj_idx}:{p}"} for p in options]
    return {
        "inline_keyboard": [
            row,
            [{"text": "◀️  Назад до проєктів", "callback_data": "menu:parse"}],
        ]
    }


def kb_confirm(proj_idx, pages):
    return {
        "inline_keyboard": [[
            {"text": "✅  Запустити",  "callback_data": f"parse:run:{proj_idx}:{pages}"},
            {"text": "❌  Скасувати", "callback_data": "menu:main"},
        ]]
    }


def kb_back():
    return {"inline_keyboard": [[{"text": "◀️  Назад", "callback_data": "menu:main"}]]}


# ==========================
# SCREEN texts
# ==========================

def text_main_menu():
    return (
        "╔══════════════════════╗\n"
        "║  🤖  <b>SERP Parser Bot</b>  ║\n"
        "╚══════════════════════╝\n\n"
        "Що хочеш зробити?"
    )


def text_choose_project(projects):
    lines = ["🗂 <b>Оберіть проєкт для парсингу:</b>\n"]
    for p in projects:
        kw_count = len(p.get("keywords", []))
        lines.append(f"📁 <b>{p['name']}</b>  —  🌍 {p.get('location','—')}  |  🔑 {kw_count} KW")
    return "\n\n".join(lines)


def text_choose_pages(proj):
    kw_count = len(proj.get("keywords", []))
    return (
        f"📁 Проєкт: <b>{proj['name']}</b>\n"
        f"🌍 {proj.get('location', '—')}  |  🔑 {kw_count} ключів\n\n"
        f"📄 <b>Скільки сторінок парсити?</b>\n"
        f"<i>1 стор. = 10 результатів SERP</i>"
    )


def text_confirm(proj, pages):
    kw_count = len(proj.get("keywords", []))
    return (
        f"🔎 <b>Підтвердження</b>\n\n"
        f"📁 {proj['name']}\n"
        f"🌍 {proj.get('location','—')}\n"
        f"🔑 {kw_count} ключів  ×  📄 {pages} стор. = ~{kw_count * pages} запитів\n\n"
        f"Запустити?"
    )


def text_stats():
    history = load_history()
    if not history:
        return "📈 <b>Статистика</b>\n\nІсторія ще порожня."

    lines         = ["📈 <b>Остання статистика</b>\n"]
    done_projects = set()

    for entry in reversed(history):
        pname = entry.get("project", "—")
        if pname in done_projects:
            continue
        done_projects.add(pname)
        ts       = entry.get("timestamp", "")[:16]
        results  = entry.get("results", [])
        total_kw = len({r["keyword"] for r in results})
        targets  = [r for r in results if r.get("is_target")]
        top3     = sum(1 for r in targets if isinstance(r.get("position"), int) and r["position"] <= 3)
        top10    = sum(1 for r in targets if isinstance(r.get("position"), int) and r["position"] <= 10)
        lines.append(
            f"📁 <b>{pname}</b>  <i>({ts})</i>\n"
            f"   🔑 {total_kw} KW  |  🎯 {len(targets)} попадань\n"
            f"   🥇 Топ-3: {top3}  |  🟢 Топ-10: {top10}"
        )

    return "\n\n".join(lines)


# ==========================
# PARSING runner
# ==========================

def run_parsing(chat_id, message_id, proj: dict, pages: int):
    proj_run          = dict(proj)
    proj_run["pages"] = pages
    kw_count          = len(proj_run.get("keywords", []))

    edit_message(
        chat_id, message_id,
        f"⏳ <b>Парсинг запущено...</b>\n\n"
        f"📁 {proj_run['name']}\n"
        f"🔑 {kw_count} ключів  ×  📄 {pages} стор.\n\n"
        f"<i>Зачекайте...</i>"
    )

    start_time = datetime.datetime.now()
    results    = asyncio.run(run_project(proj_run, max_concurrent_requests=10))
    end_time   = datetime.datetime.now()
    duration   = (end_time - start_time).total_seconds()

    target_domains = proj_run.get("target_domains", []) or []
    results        = enrich_results(results, target_domains)
    save_history_entry(proj_run, results)

    history_all         = load_history()
    history_for_project = [h for h in history_all if h.get("project") == proj_run["name"]]

    filename = f"SERP_{proj_run['name']}_{end_time.strftime('%Y%m%d_%H%M')}.xlsx"
    export_to_excel(results, filename, target_domains, history_for_project)

    report = build_tg_report(proj_run, results, duration, end_time, pages)
    edit_message(chat_id, message_id, f"✅ <b>Готово!</b>\n\n{report}\n\n📎 Відправляю файл...")

    if not send_document(chat_id, filename,
                         caption=f"📊 {proj_run['name']} | {end_time.strftime('%d.%m.%Y %H:%M')}"):
        send_message(chat_id, "⚠️ Не вдалося відправити файл.")

    try:
        os.remove(filename)
    except Exception:
        pass

    send_message(chat_id, text_main_menu(), reply_markup=kb_main_menu())


# ==========================
# CALLBACK router
# ==========================

def handle_callback(callback_query):
    cq_id   = callback_query["id"]
    chat_id = callback_query["message"]["chat"]["id"]
    msg_id  = callback_query["message"]["message_id"]
    from_id = str(callback_query["from"]["id"])
    data    = callback_query.get("data", "")

    answer_callback(cq_id)

    # Авторизація
    if CHAT_ID and from_id != CHAT_ID:
        print(f"[auth] відхилено from_id={from_id}, дозволено={CHAT_ID}")
        send_message(chat_id, "⛔ Немає доступу.")
        return

    projects = load_projects().get("projects", [])

    if data == "menu:main":
        edit_message(chat_id, msg_id, text_main_menu(), reply_markup=kb_main_menu())

    elif data == "menu:parse":
        if not projects:
            edit_message(chat_id, msg_id,
                         "📭 Проєктів ще немає. Створи їх у Streamlit.", reply_markup=kb_back())
        else:
            edit_message(chat_id, msg_id,
                         text_choose_project(projects), reply_markup=kb_projects(projects))

    elif data == "menu:projects":
        if not projects:
            edit_message(chat_id, msg_id, "📭 Проєктів ще немає.", reply_markup=kb_back())
        else:
            lines = ["📋 <b>Мої проєкти:</b>\n"]
            for i, p in enumerate(projects, 1):
                kw_count = len(p.get("keywords", []))
                domains  = ", ".join(p.get("target_domains", []) or ["—"])
                lines.append(
                    f"<b>{i}. {p['name']}</b>\n"
                    f"   🌍 {p.get('location','—')}  |  🔑 {kw_count} KW\n"
                    f"   🎯 {domains}"
                )
            edit_message(chat_id, msg_id, "\n\n".join(lines), reply_markup=kb_back())

    elif data == "menu:stats":
        edit_message(chat_id, msg_id, text_stats(), reply_markup=kb_back())

    elif data.startswith("parse:proj:"):
        proj_idx = int(data.split(":")[2])
        if proj_idx >= len(projects):
            edit_message(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        edit_message(chat_id, msg_id,
                     text_choose_pages(projects[proj_idx]),
                     reply_markup=kb_pages(proj_idx))

    elif data.startswith("parse:pages:"):
        parts    = data.split(":")
        proj_idx, pages = int(parts[2]), int(parts[3])
        if proj_idx >= len(projects):
            edit_message(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        edit_message(chat_id, msg_id,
                     text_confirm(projects[proj_idx], pages),
                     reply_markup=kb_confirm(proj_idx, pages))

    elif data.startswith("parse:run:"):
        parts    = data.split(":")
        proj_idx, pages = int(parts[2]), int(parts[3])
        if proj_idx >= len(projects):
            edit_message(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        run_parsing(chat_id, msg_id, projects[proj_idx], pages)


# ==========================
# MESSAGE handler
# ==========================

def handle_message(message):
    chat_id = message["chat"]["id"]
    from_id = str(message["from"]["id"])
    text    = message.get("text", "").strip()

    if CHAT_ID and from_id != CHAT_ID:
        print(f"[auth] відхилено from_id={from_id}")
        return

    send_message(chat_id, text_main_menu(), reply_markup=kb_main_menu())


# ==========================
# MAIN LOOP
# ==========================

def main():
    # Перевірка токену
    me = tg_get("getMe")
    if me.get("ok"):
        print(f"[tg_bot] ✅ Бот: @{me['result'].get('username','?')}")
        print(f"[tg_bot] Авторизований CHAT_ID: '{CHAT_ID}'")
    else:
        print(f"[tg_bot] ❌ getMe failed. Перевір TG_BOT_TOKEN: {me}")
        return

    # Скидаємо накопичені старі updates
    old = tg_get("getUpdates", {"offset": -1})
    if old.get("result"):
        skip_id = old["result"][-1]["update_id"] + 1
        tg_get("getUpdates", {"offset": skip_id})
        print(f"[tg_bot] Скинуто старі updates (offset={skip_id})")

    # Стартове повідомлення
    if CHAT_ID:
        send_message(CHAT_ID, text_main_menu(), reply_markup=kb_main_menu())

    print("[tg_bot] Слухаю... Ctrl+C для зупинки.")

    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                print(f"[tg_bot] update {update['update_id']}: {list(update.keys())}")

                if "callback_query" in update:
                    handle_callback(update["callback_query"])
                elif "message" in update:
                    handle_message(update["message"])

        except KeyboardInterrupt:
            print("[tg_bot] Зупинено.")
            break
        except Exception as e:
            print(f"[tg_bot] Помилка циклу: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
