"""
Автопарсинг кожні N годин.
Запускається як окремий процес поряд зі Streamlit (через render.yaml або вручну).

Змінні середовища (задати в Render → Environment):
  TG_BOT_TOKEN   — токен бота
  TG_CHAT_ID     — chat id
  AUTO_INTERVAL_HOURS — інтервал у годинах (за замовчуванням 3)
"""

import asyncio
import datetime
import json
import os
import time
import urllib.error
import urllib.request
from collections import defaultdict

# ==========================
# НАЛАШТУВАННЯ — ЗМІНИТИ ТУТ
# ==========================
os.environ.setdefault("TG_BOT_TOKEN", "8683656869:AAEZb8YZmgjUFCHXVFQ1f_C6qq-Nx64dBKU")
os.environ.setdefault("TG_CHAT_ID",   "909587225")
os.environ.setdefault("AUTO_INTERVAL_HOURS", "3")
# ==========================

from parser.api_keys import load_projects
from parser.parser_core import run_project
from parser.excel_export import export_to_excel

DATA_FILE    = "data/projects.json"
HISTORY_FILE = "data/history.json"
INTERVAL_SEC = int(os.environ.get("AUTO_INTERVAL_HOURS", "3")) * 3600

BUCKET_KEYS = ["1-3", "4-10", "11-20", "21-30", "31-40", "41-50", "51-100"]


# ==========================
# ХЕЛПЕРИ
# ==========================

def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def get_target_root(domain: str, target_domains: list):
    d = normalize_domain(domain)
    for t in target_domains:
        t_norm = normalize_domain(t)
        if not t_norm:
            continue
        if d == t_norm or d.endswith("." + t_norm):
            return t_norm
    return None


def enrich_results(results: list, target_domains: list) -> list:
    enriched = []
    for r in results:
        domain_for_check = r.get("domain_clean") or r.get("domain", "")
        root = get_target_root(domain_for_check, target_domains)
        enriched.append({**r, "is_target": bool(root), "target_root": root})
    return enriched


def load_history() -> list:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history_entry(project: dict, results: list):
    history = load_history()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_domains = project.get("target_domains", []) or []
    entry = {
        "timestamp":      timestamp,
        "project":        project["name"],
        "location":       project["location"],
        "pages":          project["pages"],
        "target_domains": target_domains,
        "results":        results,
    }
    history.append(entry)
    if len(history) > 50:
        history = history[-50:]
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ==========================
# TELEGRAM
# ==========================

def send_telegram_message(token: str, chat_id: str, text: str) -> bool:
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        print(f"[TG] HTTP {e.code}: {e.read().decode('utf-8', errors='ignore')}")
        return False
    except Exception as e:
        print(f"[TG] Помилка: {e}")
        return False


def send_telegram_document(token: str, chat_id: str, filepath: str, caption: str = "") -> bool:
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    boundary = "----BoundaryXYZ123"
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
        field("chat_id", chat_id)
        + field("caption", caption)
        + (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="document"; filename="{filename}"\r\n'
            f"Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n"
        ).encode("utf-8")
        + file_data
        + f"\r\n--{boundary}--\r\n".encode("utf-8")
    )

    try:
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        print(f"[TG doc] HTTP {e.code}: {e.read().decode('utf-8', errors='ignore')}")
        return False
    except Exception as e:
        print(f"[TG doc] Помилка: {e}")
        return False


# ==========================
# ЗВІТ ДЛЯ TELEGRAM
# ==========================

def build_tg_report(proj: dict, res: list, duration: float, end_time: datetime.datetime) -> str:
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
        if t1_3:    bars += f"🥇 1-3: {t1_3}  "
        if t4_10:   bars += f"🟢 4-10: {t4_10}  "
        if t11_20:  bars += f"🟡 11-20: {t11_20}  "
        if t21_50:  bars += f"🟠 21-50: {t21_50}  "
        if t51_100: bars += f"🔴 51-100: {t51_100}"

        domain_lines.append(
            f"🌐 {dom}\n"
            f"   {bars.strip()}\n"
            f"   📌 KW у видачі: {kw_count}  |  ⌀ поз: {avg}"
        )

    domains_block = "\n\n".join(domain_lines) if domain_lines else "— немає попадань у видачі —"
    total_kw = len({r["keyword"] for r in res})

    return (
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 SERP ЗВІТ  [авто]\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📁 Проект: {proj['name']}\n"
        f"🗓 {end_time.strftime('%d.%m.%Y  %H:%M')}\n"
        f"🌍 {proj.get('location', '')}  |  gl={proj.get('gl', '')}  hl={proj.get('hl', '')}\n"
        f"⏱ Час парсингу: {duration:.0f} сек\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Ключів: {total_kw}  |  📄 Сторінок: {proj.get('pages', 1)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 ПОЗИЦІЇ ПО ДОМЕНАХ\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{domains_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


# ==========================
# ПАРСИНГ ОДНОГО ПРОЄКТУ
# ==========================

def run_one_project(proj: dict):
    print(f"[{datetime.datetime.now():%H:%M:%S}] Старт: {proj['name']}")
    token   = os.environ.get("TG_BOT_TOKEN", "")
    chat_id = os.environ.get("TG_CHAT_ID", "")

    start_time = datetime.datetime.now()
    res = asyncio.run(run_project(proj, max_concurrent_requests=10))
    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()

    target_domains = proj.get("target_domains", []) or []
    res = enrich_results(res, target_domains)

    save_history_entry(proj, res)

    history_all = load_history()
    history_for_project = [h for h in history_all if h.get("project") == proj["name"]]

    filename = f"SERP_{proj['name']}_{end_time.strftime('%Y%m%d_%H%M')}.xlsx"
    export_to_excel(res, filename, target_domains, history_for_project)

    print(f"[{datetime.datetime.now():%H:%M:%S}] Готово: {proj['name']} ({len(res)} рядків, {duration:.0f}с)")

    if token and chat_id:
        report = build_tg_report(proj, res, duration, end_time)
        sent = send_telegram_document(token, chat_id, filename, caption=report)
        if not sent:
            send_telegram_message(token, chat_id, report)

    # Прибираємо файл після відправки
    try:
        os.remove(filename)
    except Exception:
        pass


# ==========================
# ГОЛОВНИЙ ЦИКЛ
# ==========================

def main():
    token   = os.environ.get("TG_BOT_TOKEN", "")
    chat_id = os.environ.get("TG_CHAT_ID", "")
    hours   = int(os.environ.get("AUTO_INTERVAL_HOURS", "3"))

    print(f"[scheduler] Запущено. Інтервал: {hours} год. Ctrl+C для зупинки.")

    if token and chat_id:
        send_telegram_message(token, chat_id,
            f"🤖 Автопарсер запущено\nІнтервал: кожні {hours} год")

    while True:
        projects_data = load_projects()
        projects = projects_data.get("projects", [])

        if not projects:
            print(f"[{datetime.datetime.now():%H:%M:%S}] Немає проєктів, чекаю...")
        else:
            for proj in projects:
                try:
                    run_one_project(proj)
                except Exception as e:
                    print(f"[!] Помилка проєкту '{proj.get('name')}': {e}")
                    if token and chat_id:
                        send_telegram_message(token, chat_id,
                            f"❌ Помилка парсингу '{proj.get('name')}': {e}")

        next_run = datetime.datetime.now() + datetime.timedelta(seconds=INTERVAL_SEC)
        print(f"[scheduler] Наступний запуск: {next_run:%H:%M:%S}")
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
