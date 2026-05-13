import streamlit as st
import asyncio
import datetime
import json
import os
import urllib.request
import urllib.parse
from collections import defaultdict

import pandas as pd

from parser.api_keys import load_projects, save_projects
# Імпортуємо з parser_core замість parser_core_ultra
from parser.parser_core import run_project
from parser.excel_export import export_to_excel

# ==========================
# ЗМІННІ СЕРЕДОВИЩА — ЗАПОВНИ ВРУЧНУ
# ==========================
os.environ.setdefault("TG_BOT_TOKEN", "8683656869:AAEZb8YZmgjUFCHXVFQ1f_C6qq-Nx64dBKU")
os.environ.setdefault("TG_CHAT_ID", "909587225")
# ==========================

st.set_page_config(page_title="SERP Parser", layout="wide")
st.title("🔍 Google SERP Parser (Serper.dev) ⚡ ULTRA FAST")

PROJECTS_DATA = load_projects()
HISTORY_FILE = "data/history.json"


# ==========================
# ХЕЛПЕРИ ДЛЯ ДОМЕНІВ / СУБДОМЕНІВ
# ==========================

def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    if d.startswith("www."):
        d = d[4:]
    return d


def enrich_results(results: list, target_domains: list) -> list:
    """
    Проставляє is_target і target_root для кожного результату.
    Використовує domain_clean (без www.) якщо є, інакше domain.
    """
    enriched = []
    for r in results:
        domain_for_check = r.get("domain_clean") or r.get("domain", "")
        root = get_target_root(domain_for_check, target_domains)
        enriched.append({
            **r,
            "is_target": bool(root),
            "target_root": root,
        })
    return enriched


def is_target_domain(domain: str, target_domains) -> bool:
    """
    Перевіряє, чи domain є таргетним (www. ігнорується):
    - exact match: example.com
    - або субдомен: casino.example.com для example.com
    """
    d = normalize_domain(domain)
    for t in target_domains:
        t_norm = normalize_domain(t)
        if not t_norm:
            continue
        if d == t_norm or d.endswith("." + t_norm):
            return True
    return False


def get_target_root(domain: str, target_domains):
    """
    Повертає таргетний root-домен (www. ігнорується), або None.
    """
    d = normalize_domain(domain)
    for t in target_domains:
        t_norm = normalize_domain(t)
        if not t_norm:
            continue
        if d == t_norm or d.endswith("." + t_norm):
            return t_norm
    return None


# ==========================
# ПОЗИЦІЙНІ БАКЕТИ ДО ТОП-100
# ==========================

def bucket_for_position(pos: int) -> str:
    """
    Розкладаємо позиції по діапазонах до ТОП-100.
    Все, що вище 100 — у бакет >100.
    """
    if pos is None:
        return ">100"
    if 1 <= pos <= 3:
        return "1-3"
    if 4 <= pos <= 10:
        return "4-10"
    if 11 <= pos <= 20:
        return "11-20"
    if 21 <= pos <= 30:
        return "21-30"
    if 31 <= pos <= 40:
        return "31-40"
    if 41 <= pos <= 50:
        return "41-50"
    if 51 <= pos <= 100:
        return "51-100"
    return ">100"


BUCKET_KEYS = ["1-3", "4-10", "11-20", "21-30", "31-40", "41-50", "51-100"]

BUCKET_SCORES = {
    "1-3": 100,
    "4-10": 60,
    "11-20": 40,
    "21-30": 25,
    "31-40": 15,
    "41-50": 8,
    "51-100": 3,
}


def calculate_score(buckets: dict) -> int:
    """
    Інтегральний скоринг з урахуванням усіх бакетів до ТОП-100.
    """
    return sum(buckets.get(k, 0) * BUCKET_SCORES[k] for k in BUCKET_KEYS)


# ==========================
# ІСТОРІЯ ПАРСИНГІВ
# ==========================

def load_history():
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history_entry(project: dict, results: list):
    """
    Зберігаємо запуск із is_target + target_root.
    results вже мають бути збагачені через enrich_results().
    """
    history = load_history()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    target_domains = project.get("target_domains", []) or []

    entry = {
        "timestamp": timestamp,
        "project": project["name"],
        "location": project["location"],
        "pages": project["pages"],
        "target_domains": target_domains,
        "results": results,
    }

    history.append(entry)
    if len(history) > 50:
        history = history[-50:]

    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ==========================
# НАЛАШТУВАННЯ ШВИДКОСТІ
# ==========================

# ==========================
# TELEGRAM НАЛАШТУВАННЯ
# ==========================

st.sidebar.header("📬 Telegram сповіщення")
tg_token = st.sidebar.text_input(
    "Bot Token",
    value=os.environ.get("TG_BOT_TOKEN", ""),
    type="password",
    key="tg_token",
    help="Отримай від @BotFather",
)
tg_chat_id = st.sidebar.text_input(
    "Chat ID",
    value=os.environ.get("TG_CHAT_ID", ""),
    key="tg_chat_id",
    help="Отримай від @userinfobot або з посилання на групу",
)
tg_enabled = st.sidebar.toggle("Надсилати результати в Telegram", value=bool(tg_token and tg_chat_id))

st.sidebar.divider()


def send_telegram_message(token: str, chat_id: str, text: str):
    """Надсилає повідомлення в Telegram через Bot API."""
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = json.dumps({
            "chat_id": chat_id,
            "text": text,
        }).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")
        st.warning(f"⚠️ Telegram HTTP {e.code}: {error_body}")
        return False
    except Exception as e:
        st.warning(f"⚠️ Telegram: не вдалося надіслати — {e}")
        return False


def send_telegram_document(token: str, chat_id: str, filepath: str, caption: str = ""):
    """Надсилає файл у Telegram через Bot API (multipart/form-data)."""
    import urllib.error
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
            url,
            data=body,
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")
        st.warning(f"⚠️ Telegram (файл) HTTP {e.code}: {error_body}")
        return False
    except Exception as e:
        st.warning(f"⚠️ Telegram (файл): не вдалося надіслати — {e}")
        return False
speed_mode = st.sidebar.selectbox(
    "Режим парсингу",
    [
        "🚀 Ultra Fast (найшвидший, 20 паралельних запитів)",
        "⚡ Fast (швидкий, 15 паралельних запитів)",
        "🔄 Balanced (збалансований, 10 паралельних запитів)",
        "🐢 Safe (безпечний, 5 паралельних запитів)",
    ],
    index=1,
)

# Визначаємо параметри на основі режиму
if "Ultra Fast" in speed_mode:
    max_concurrent = 20
elif "Fast" in speed_mode:
    max_concurrent = 15
elif "Balanced" in speed_mode:
    max_concurrent = 10
else:  # Safe
    max_concurrent = 5

st.sidebar.info(
    f"**Поточні налаштування:**\n"
    f"- Паралельних запитів: {max_concurrent}\n"
    f"- Метод: Batched (пакетами по 50)"
)

# ==========================
# ДОДАВАННЯ НОВОГО ПРОЄКТУ
# ==========================

st.header("➕ Додати новий проєкт")

with st.form("add_project"):
    name = st.text_input("Назва проєкту")
    api_key = st.text_input("API ключ Serper.dev", type="password")
    raw_kw = st.text_area("Ключові слова (кожне з нового рядка)")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        loc = st.text_input("LOCATION", "Ukraine")
    with col2:
        gl = st.text_input("GL", "ua")
    with col3:
        hl = st.text_input("HL", "uk")
    with col4:
        pages = st.slider(
            "Кількість сторінок (1 = ТОП-10, 3 = ТОП-30 ... до ТОП-100)",
            1,
            10,
            1,
        )

    target_raw = st.text_area(
        "Цільові домени (root, кожен з нового рядка, опціонально)",
        value="",
    )

    submitted = st.form_submit_button("Додати")
    if submitted:
        if not name or not api_key or not raw_kw.strip():
            st.error("Заповни хоча б назву, API-ключ і ключові слова")
        else:
            project = {
                "name": name,
                "api_key": api_key,
                "keywords": [k.strip() for k in raw_kw.split("\n") if k.strip()],
                "location": loc,
                "gl": gl,
                "hl": hl,
                "pages": pages,
                "target_domains": [d.strip() for d in target_raw.split("\n") if d.strip()],
            }
            PROJECTS_DATA["projects"].append(project)
            save_projects(PROJECTS_DATA)
            st.success("Проєкт додано")
            st.rerun()

st.divider()

# ==========================
# СПИСОК ПРОЄКТІВ + РЕДАГУВАННЯ
# ==========================

st.header("📁 Список проєктів")

if not PROJECTS_DATA["projects"]:
    st.info("Немає проєктів")
else:
    for idx, proj in enumerate(PROJECTS_DATA["projects"]):
        with st.expander(f"📌 {proj['name']}"):
            new_name = st.text_input(
                "Назва проєкту",
                value=proj["name"],
                key=f"name_{idx}",
            )
            new_api_key = st.text_input(
                "API ключ Serper.dev",
                value=proj["api_key"],
                type="password",
                key=f"key_{idx}",
            )

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                new_loc = st.text_input(
                    "LOCATION", value=proj["location"], key=f"loc_{idx}"
                )
            with col2:
                new_gl = st.text_input("GL", value=proj["gl"], key=f"gl_{idx}")
            with col3:
                new_hl = st.text_input("HL", value=proj["hl"], key=f"hl_{idx}")
            with col4:
                new_pages = st.slider(
                    "Кількість сторінок (1 = ТОП-10, 3 = ТОП-30 ... до ТОП-100)",
                    1,
                    10,
                    proj.get("pages", 1),
                    key=f"pages_{idx}",
                )

            kw_text = st.text_area(
                "Ключові слова (редаговані)",
                value="\n".join(proj["keywords"]),
                key=f"kw_{idx}",
                height=150,
            )

            target_text = st.text_area(
                "Цільові домени (root)",
                value="\n".join(proj.get("target_domains", [])),
                key=f"targets_{idx}",
                height=80,
            )

            col_save, col_run, col_del = st.columns(3)

            with col_save:
                if st.button("💾 Зберегти зміни", key=f"save_{idx}"):
                    proj["name"] = new_name
                    proj["api_key"] = new_api_key
                    proj["location"] = new_loc
                    proj["gl"] = new_gl
                    proj["hl"] = new_hl
                    proj["pages"] = new_pages
                    proj["keywords"] = [
                        k.strip() for k in kw_text.split("\n") if k.strip()
                    ]
                    proj["target_domains"] = [
                        d.strip() for d in target_text.split("\n") if d.strip()
                    ]
                    save_projects(PROJECTS_DATA)
                    st.success("Зміни збережено")
                    st.rerun()

            with col_run:
                if st.button("🚀 Запустити парсинг", key=f"run_{idx}"):
                    st.session_state["run_project_idx"] = idx
                    st.rerun()

            with col_del:
                if st.button("🗑 Видалити проєкт", key=f"del_{idx}"):
                    PROJECTS_DATA["projects"].pop(idx)
                    save_projects(PROJECTS_DATA)
                    st.rerun()

# ==========================
# ЗАПУСК ПАРСИНГУ (ПРОГРЕС + БІЛЬШЕ ІНФИ)
# ==========================

if "run_project_idx" in st.session_state:
    i = st.session_state["run_project_idx"]
    proj = PROJECTS_DATA["projects"][i]

    st.header(f"🚀 Запуск парсингу: {proj['name']}")
    st.info(f"**Режим:** {speed_mode} | **Паралельних запитів:** {max_concurrent}")

    progress_bar = st.progress(0)
    current_kw_text = st.empty()
    log_box = st.empty()

    log_messages = []

    def append_log(msg: str):
        log_messages.append(msg)
        log_box.text("\n".join(log_messages[-50:]))

    total_kw = len(proj.get("keywords", []))
    append_log(
        f"⚡ Старт проєкту '{proj['name']}' в режимі {speed_mode}"
    )
    append_log(
        f"📊 Ключових слів: {total_kw}, сторінок на ключ: {proj.get('pages', 1)}, "
        f"паралельних запитів: {max_concurrent}"
    )
    append_log(
        f"🌍 LOCATION={proj.get('location')}, GL={proj.get('gl')}, HL={proj.get('hl')}"
    )
    if proj.get("target_domains"):
        append_log("🎯 Цільові домени (root): " + ", ".join(proj["target_domains"]))
    else:
        append_log("⚠️ Цільові домени не задано.")

    def on_progress(current_idx, total_keywords, kw):
        if total_keywords > 0:
            frac = current_idx / total_keywords
        else:
            frac = 0.0
        progress_bar.progress(frac)
        current_kw_text.write(
            f"⚡ Обробка ключа: **{current_idx}/{total_keywords}** — `{kw}` "
            f"(сторінок: {proj.get('pages', 1)})"
        )
        append_log(f"🔍 Ключ {current_idx}/{total_keywords}: {kw}")

    start_time = datetime.datetime.now()
    append_log(f"⏱️ Початок: {start_time.strftime('%H:%M:%S')}")

    with st.spinner("⚡ Парсинг в процесі..."):
        res = asyncio.run(
            run_project(proj, progress_callback=on_progress, max_concurrent_requests=max_concurrent)
        )

    end_time = datetime.datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Збагачуємо результати одразу після парсингу
    target_domains = proj.get("target_domains", []) or []
    res = enrich_results(res, target_domains)

    target_hits = sum(1 for r in res if r.get("is_target"))
    top3  = sum(1 for r in res if r.get("is_target") and isinstance(r.get("position"), int) and r["position"] <= 3)
    top10 = sum(1 for r in res if r.get("is_target") and isinstance(r.get("position"), int) and r["position"] <= 10)

    st.success(f"✅ Готово! Отримано {len(res)} результатів за {duration:.1f} секунд")
    append_log(f"⏱️ Кінець: {end_time.strftime('%H:%M:%S')}")
    append_log(f"⚡ Тривалість: {duration:.1f} секунд")
    append_log(f"📊 Швидкість: {len(res)/duration:.1f} результатів/сек")
    append_log(f"✅ Завершено. Всього записів: {len(res)}, цільових: {target_hits}")

    # зберігаємо історію (res вже збагачений)
    save_history_entry(proj, res)
    history_all = load_history()
    history_for_project = [h for h in history_all if h.get("project") == proj["name"]]

    # експорт у Excel
    filename = f"SERP_{proj['name']}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    export_to_excel(res, filename, target_domains, history_for_project)

    with open(filename, "rb") as f:
        st.download_button("⬇ Завантажити Excel", data=f, file_name=filename)

    # Telegram — детальний SEO-звіт + Excel файл
    if tg_enabled and tg_token and tg_chat_id:

        # Збираємо статистику по кожному цільовому домену
        from collections import defaultdict as _dd
        dom_stats = _dd(lambda: {"positions": [], "keywords": set()})
        for r in res:
            if not r.get("is_target"):
                continue
            pos = r.get("position")
            if not isinstance(pos, int):
                continue
            root = r.get("target_root") or r.get("domain", "")
            dom_stats[root]["positions"].append(pos)
            dom_stats[root]["keywords"].add(r["keyword"])

        # Формуємо рядки по доменах
        domain_lines = []
        for dom in sorted(dom_stats.keys()):
            positions = dom_stats[dom]["positions"]
            kw_count  = len(dom_stats[dom]["keywords"])
            if not positions:
                continue
            t1_3   = sum(1 for p in positions if p <= 3)
            t4_10  = sum(1 for p in positions if 4 <= p <= 10)
            t11_20 = sum(1 for p in positions if 11 <= p <= 20)
            t21_50 = sum(1 for p in positions if 21 <= p <= 50)
            t51_100= sum(1 for p in positions if 51 <= p <= 100)
            avg    = round(sum(positions) / len(positions), 1)
            best   = min(positions)

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

        total_kw_parsed = len({r["keyword"] for r in res})
        tg_text = (
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 SERP ЗВІТ\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📁 Проект: {proj['name']}\n"
            f"🗓 {end_time.strftime('%d.%m.%Y  %H:%M')}\n"
            f"🌍 {proj.get('location', '')}  |  gl={proj.get('gl', '')}  hl={proj.get('hl', '')}\n"
            f"⏱ Час парсингу: {duration:.0f} сек\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔑 Ключів: {total_kw_parsed}  |  📄 Сторінок: {proj.get('pages', 1)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📌 ПОЗИЦІЇ ПО ДОМЕНАХ\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{domains_block}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )

        sent_doc = send_telegram_document(tg_token, tg_chat_id, filename, caption=tg_text)
        if sent_doc:
            st.info("📬 Excel зі звітом надіслано в Telegram")
        else:
            if send_telegram_message(tg_token, tg_chat_id, tg_text):
                st.info("📬 Звіт надіслано в Telegram (файл не вдалося)")

    del st.session_state["run_project_idx"]

# ==========================
# АНАЛІТИКА / ТАБИ (по окремих проєктах, з урахуванням субдоменів)
# ==========================

history = load_history()

if history:
    project_names_in_history = sorted(
        {h.get("project") for h in history if h.get("project")}
    )

    if not project_names_in_history:
        st.info("В історії немає жодного проєкту.")
    else:
        selected_project = st.selectbox(
            "Оберіть проєкт для аналітики",
            project_names_in_history,
        )

        history_for_project = [
            h for h in history if h.get("project") == selected_project
        ]

        if not history_for_project:
            st.info("Для цього проєкту ще немає запусків.")
        else:
            last_entry = history_for_project[-1]
            last_results = last_entry["results"]
            last_targets = last_entry.get("target_domains", []) or []

            # Target Domains Stats (останній прогін, домени/субдомени)
            domain_buckets = defaultdict(lambda: {k: 0 for k in BUCKET_KEYS})
            domain_keywords = defaultdict(set)
            domain_roots = {}

            for r in last_results:
                if not r.get("is_target"):
                    continue
                dom = r["domain"]
                pos = r.get("position")
                if not isinstance(pos, int):
                    continue
                bucket = bucket_for_position(pos)
                if bucket not in BUCKET_KEYS:
                    continue
                domain_buckets[dom][bucket] += 1
                domain_keywords[dom].add(r["keyword"])
                domain_roots[dom] = r.get("target_root") or get_target_root(dom, last_targets)

            target_domains_stats = []
            for dom, buckets in domain_buckets.items():
                total = sum(buckets.values())
                if total == 0:
                    continue
                score = calculate_score(buckets)
                row = {
                    "Domain": dom,
                    "Target Root": domain_roots.get(dom, ""),
                    "Total": total,
                }
                for k in BUCKET_KEYS:
                    row[f"Pos {k}"] = buckets[k]
                row["Score"] = score
                row["Keywords"] = "; ".join(sorted(domain_keywords[dom]))
                target_domains_stats.append(row)

            target_domains_stats.sort(key=lambda x: x["Score"], reverse=True)

            # Position Buckets (усі домени/субдомени, останній прогін)
            all_domain_buckets = defaultdict(lambda: {k: 0 for k in BUCKET_KEYS})

            for r in last_results:
                pos = r.get("position")
                if not isinstance(pos, int):
                    continue
                bucket = bucket_for_position(pos)
                if bucket not in BUCKET_KEYS:
                    continue
                dom = r["domain"]
                all_domain_buckets[dom][bucket] += 1

            position_buckets = []
            for dom, buckets in all_domain_buckets.items():
                total = sum(buckets.values())
                if total == 0:
                    continue
                score = calculate_score(buckets)
                row = {
                    "Domain": dom,
                    "Total": total,
                }
                for k in BUCKET_KEYS:
                    row[f"Pos {k}"] = buckets[k]
                row["Score"] = score
                row["Is_Target"] = "✅" if is_target_domain(dom, last_targets) else ""
                position_buckets.append(row)

            position_buckets.sort(key=lambda x: x["Score"], reverse=True)

            # History Summary (по доменах/субдоменах)
            history_summary = []
            domain_set_for_summary = set()
            for entry in history_for_project:
                ts = entry["timestamp"]
                proj_name = entry["project"]
                entry_results = entry["results"]

                domain_positions = defaultdict(list)
                for r in entry_results:
                    if not r.get("is_target"):
                        continue
                    pos = r.get("position")
                    if not isinstance(pos, int):
                        continue
                    dom = r["domain"]
                    domain_positions[dom].append(pos)

                for dom, positions in domain_positions.items():
                    if not positions:
                        continue
                    avg_pos = round(sum(positions) / len(positions), 1)
                    history_summary.append(
                        {
                            "Date": ts,
                            "Project": proj_name,
                            "Domain": dom,
                            "Total Found": len(positions),
                            "Avg Pos": avg_pos,
                            "Top 3": sum(1 for p in positions if p <= 3),
                            "Top 10": sum(1 for p in positions if p <= 10),
                            "Top 20": sum(1 for p in positions if p <= 20),
                            "Top 30": sum(1 for p in positions if p <= 30),
                            "Top 50": sum(1 for p in positions if p <= 50),
                            "Top 100": sum(1 for p in positions if p <= 100),
                        }
                    )
                    domain_set_for_summary.add(dom)

            # Dynamics: між двома останніми запусками
            all_keywords_for_targets = set()
            for entry in history_for_project:
                for r in entry["results"]:
                    if r.get("is_target"):
                        all_keywords_for_targets.add(r["keyword"])

            tab_results, tab_targets, tab_buckets, tab_dynamics, tab_summary = st.tabs(
                [
                    "Results",
                    "Target Domains Stats",
                    "Position Buckets",
                    "Dynamics",
                    "History Summary",
                ]
            )

            # Results
            with tab_results:
                st.subheader(
                    f"Results (останній прогін для проєкту: {selected_project})"
                )
                rows = []
                for r in last_results:
                    rows.append(
                        {
                            "Keyword": r["keyword"],
                            "Position": r["position"],
                            "Domain": r["domain"],
                            "Title": r["title"],
                            "Snippet": r["snippet"],
                            "URL": r["url"],
                            "Is_Target": "✅" if r.get("is_target") else "",
                            "Target Root": r.get("target_root") or "",
                        }
                    )
                st.dataframe(rows, width="stretch")

            # Target Domains Stats
            with tab_targets:
                st.subheader("Target Domains Stats (останній прогін, домени/субдомени)")
                if target_domains_stats:
                    st.dataframe(target_domains_stats, width="stretch")
                else:
                    st.info(
                        "Немає попадань по цільових доменах у останньому прогоні для цього проєкту."
                    )

            # Position Buckets
            with tab_buckets:
                st.subheader("Position Buckets (усі домени/субдомени, цей проєкт)")
                if position_buckets:
                    st.dataframe(position_buckets, width="stretch")
                else:
                    st.info("Немає даних для buckets.")

            # Dynamics
            with tab_dynamics:
                st.subheader(
                    "Dynamics (по ключах, між останнім та попереднім запуском)"
                )
                runs_count = len(history_for_project)
                if runs_count < 2:
                    st.info("Для динаміки потрібно мінімум два запуски цього проєкту.")
                else:
                    prev_entry = history_for_project[-2]
                    curr_entry = history_for_project[-1]

                    prev_results = prev_entry["results"]
                    curr_results = curr_entry["results"]

                    target_roots = [
                        normalize_domain(d)
                        for d in curr_entry.get("target_domains", []) or []
                    ]

                    if target_roots:
                        selected_root = st.selectbox(
                            "Оберіть домен (root, без субдоменів)",
                            target_roots,
                        )
                    else:
                        selected_root = None
                        st.info("У проєкті немає цільових доменів — динаміка не рахується.")

                    if selected_root:
                        prev_pos = defaultdict(lambda: None)
                        curr_pos = defaultdict(lambda: None)

                        # попередній запуск
                        for r in prev_results:
                            if not r.get("is_target"):
                                continue
                            root = r.get("target_root") or get_target_root(
                                r["domain"], [selected_root]
                            )
                            if root != selected_root:
                                continue
                            kw = r["keyword"]
                            pos = r.get("position")
                            if isinstance(pos, int):
                                if prev_pos[kw] is None or pos < prev_pos[kw]:
                                    prev_pos[kw] = pos

                        # поточний запуск
                        for r in curr_results:
                            if not r.get("is_target"):
                                continue
                            root = r.get("target_root") or get_target_root(
                                r["domain"], [selected_root]
                            )
                            if root != selected_root:
                                continue
                            kw = r["keyword"]
                            pos = r.get("position")
                            if isinstance(pos, int):
                                if curr_pos[kw] is None or pos < curr_pos[kw]:
                                    curr_pos[kw] = pos

                        all_keywords = sorted(set(prev_pos.keys()) | set(curr_pos.keys()))
                        rows_dyn = []
                        for kw in all_keywords:
                            p_prev = prev_pos.get(kw)
                            p_curr = curr_pos.get(kw)
                            if p_prev is None and p_curr is not None:
                                status = "NEW"
                            elif p_prev is not None and p_curr is None:
                                status = "LOST"
                            else:
                                status = "SAME"
                            rows_dyn.append(
                                {
                                    "Keyword": kw,
                                    f"{prev_entry['timestamp']}": (
                                        p_prev if p_prev is not None else "—"
                                    ),
                                    f"{curr_entry['timestamp']}": (
                                        p_curr if p_curr is not None else "—"
                                    ),
                                    "Status": status,
                                }
                            )

                        if rows_dyn:
                            st.dataframe(rows_dyn, width="stretch")
                        else:
                            st.info("Немає даних для динаміки по цьому домену.")

            # History Summary
            with tab_summary:
                st.subheader(
                    "History Summary (по прогонах, по доменах/субдоменах, цей проєкт)"
                )
                if not history_summary:
                    st.info(
                        "Поки що немає попадань по цільових доменах для цього проєкту."
                    )
                else:
                    domain_options = ["Усі домени"] + sorted(domain_set_for_summary)
                    selected_domain = st.selectbox(
                        "Оберіть домен/субдомен для перегляду", domain_options
                    )
                    if selected_domain == "Усі домени":
                        rows_to_show = history_summary
                    else:
                        rows_to_show = [
                            r
                            for r in history_summary
                            if r["Domain"] == selected_domain
                        ]
                    st.dataframe(rows_to_show, width="stretch")
else:
    st.info(
        "Історія ще порожня — запусти хоча б один парсинг, щоб побачити звіт."
    )
