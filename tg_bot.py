"""
Telegram-бот для ручного запуску SERP-парсингу.
Повністю автономний — не залежить від структури папок parser/.
Запускається: python tg_bot.py
"""

import asyncio
import aiohttp
import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# ==========================
# НАЛАШТУВАННЯ
# ==========================
TOKEN   = os.environ.get("TG_BOT_TOKEN", "8683656869:AAEZb8YZmgjUFCHXVFQ1f_C6qq-Nx64dBKU")
CHAT_ID = os.environ.get("TG_CHAT_ID",   "909587225")

DATA_FILE    = "data/projects.json"
HISTORY_FILE = "data/history.json"

# ==========================
# TELEGRAM API (GET + POST)
# ==========================

def tg_get(method: str, params: dict = None) -> dict:
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=35) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[TG GET] {method} {e.code}: {e.read().decode('utf-8','ignore')}")
        return {}
    except Exception as e:
        print(f"[TG GET] {method} error: {e}")
        return {}


def tg_post(method: str, payload: dict) -> dict:
    url  = f"https://api.telegram.org/bot{TOKEN}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[TG POST] {method} {e.code}: {e.read().decode('utf-8','ignore')}")
        return {}
    except Exception as e:
        print(f"[TG POST] {method} error: {e}")
        return {}


def get_updates(offset=None) -> list:
    params = {"timeout": 25, "allowed_updates": "message,callback_query"}
    if offset is not None:
        params["offset"] = offset
    return tg_get("getUpdates", params).get("result", [])


def send_msg(chat_id, text, markup=None, parse_mode="HTML"):
    p = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    if markup:
        p["reply_markup"] = markup
    return tg_post("sendMessage", p)


def edit_msg(chat_id, msg_id, text, markup=None, parse_mode="HTML"):
    p = {"chat_id": chat_id, "message_id": msg_id, "text": text, "parse_mode": parse_mode}
    if markup:
        p["reply_markup"] = markup
    return tg_post("editMessageText", p)


def answer_cb(cb_id, text=""):
    tg_post("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})


def send_doc(chat_id, filepath, caption=""):
    url      = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
    boundary = "----BotBoundary42"
    filename = os.path.basename(filepath)
    with open(filepath, "rb") as f:
        file_data = f.read()

    def field(name, val):
        return (f"--{boundary}\r\nContent-Disposition: form-data; "
                f'name="{name}"\r\n\r\n{val}\r\n').encode()

    body = (field("chat_id", str(chat_id))
            + field("caption", caption)
            + field("parse_mode", "HTML")
            + (f"--{boundary}\r\nContent-Disposition: form-data; "
               f'name="document"; filename="{filename}"\r\n'
               f"Content-Type: application/vnd.openxmlformats-officedocument"
               f".spreadsheetml.sheet\r\n\r\n").encode()
            + file_data
            + f"\r\n--{boundary}--\r\n".encode())
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status == 200
    except Exception as e:
        print(f"[send_doc] {e}")
        return False


# ==========================
# DATA: projects & history
# ==========================

def load_projects() -> list:
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("projects", [])
    except Exception:
        return []


def load_history() -> list:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history(project: dict, results: list):
    history   = load_history()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history.append({
        "timestamp":      timestamp,
        "project":        project["name"],
        "location":       project.get("location", ""),
        "pages":          project.get("pages", 1),
        "target_domains": project.get("target_domains", []) or [],
        "results":        results,
    })
    if len(history) > 50:
        history = history[-50:]
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ==========================
# DOMAIN helpers
# ==========================

def norm(domain: str) -> str:
    d = (domain or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def get_root(domain: str, targets: list):
    d = norm(domain)
    for t in targets:
        tn = norm(t)
        if tn and (d == tn or d.endswith("." + tn)):
            return tn
    return None


def enrich(results: list, targets: list) -> list:
    out = []
    for r in results:
        dc   = r.get("domain_clean") or r.get("domain", "")
        root = get_root(dc, targets)
        out.append({**r, "is_target": bool(root), "target_root": root})
    return out


# ==========================
# SERP parser (inline)
# ==========================

def _extract_domain(url: str):
    try:
        host = url.split("://", 1)[-1].split("/")[0].split(":")[0].lower()
        return host, host[4:] if host.startswith("www.") else host
    except Exception:
        return url, url


async def fetch_serp(session, api_key, keyword, location, gl, hl, page, sem):
    async with sem:
        headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        payload = {"q": keyword, "location": location, "gl": gl, "hl": hl,
                   "num": 10, "page": page, "autocorrect": False}
        try:
            async with session.post("https://google.serper.dev/search",
                                    json=payload, headers=headers) as resp:
                if resp.status != 200:
                    print(f"[serp] {resp.status} for '{keyword}' p{page}")
                    return []
                data = await resp.json()
                results = []
                for idx, item in enumerate(data.get("organic", []), 1):
                    link = item.get("link", "")
                    domain, domain_clean = _extract_domain(link)
                    results.append({
                        "keyword":      keyword,
                        "position":     (page - 1) * 10 + idx,
                        "domain":       domain,
                        "domain_clean": domain_clean,
                        "title":        item.get("title", ""),
                        "snippet":      item.get("snippet", ""),
                        "url":          link,
                    })
                return results
        except Exception as e:
            print(f"[serp] error '{keyword}' p{page}: {e}")
            return []


async def parse_project(proj: dict) -> list:
    api_key  = proj.get("api_key", "")
    keywords = proj.get("keywords", [])
    location = proj.get("location", "Germany")
    gl       = proj.get("gl", "de")
    hl       = proj.get("hl", "de")
    pages    = proj.get("pages", 1)

    if not api_key or not keywords:
        return []

    sem     = asyncio.Semaphore(10)
    timeout = aiohttp.ClientTimeout(total=30)
    tasks   = [(kw, pg) for kw in keywords for pg in range(1, pages + 1)]

    async with aiohttp.ClientSession(timeout=timeout) as session:
        coros   = [fetch_serp(session, api_key, kw, location, gl, hl, pg, sem)
                   for kw, pg in tasks]
        batches = await asyncio.gather(*coros)

    results = []
    for batch in batches:
        results.extend(batch)
    return results


# ==========================
# EXCEL export (inline)
# ==========================

BUCKET_KEYS = ["1-3","4-10","11-20","21-30","31-40","41-50","51-100"]
BUCKET_SCORES = {"1-3":100,"4-10":60,"11-20":40,"21-30":25,"31-40":15,"41-50":8,"51-100":3}
HEADER_BG, HEADER_FG, GREEN_BG = "1F4E78", "FFFFFF", "C6EFCE"
_thin = Side(style="thin", color="CCCCCC")
_brd  = Border(left=_thin, right=_thin, top=_thin, bottom=_thin)

def _fill(c): return PatternFill(start_color=c, end_color=c, fill_type="solid")

def _hdr(ws):
    ws.row_dimensions[1].height = 22
    for cell in ws[1]:
        cell.font      = Font(bold=True, color=HEADER_FG, size=10)
        cell.fill      = _fill(HEADER_BG)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _brd

def _row(ws, row_num, is_target=False):
    for cell in ws[row_num]:
        if is_target: cell.fill = _fill(GREEN_BG)
        cell.border = _brd
        cell.font   = Font(size=10)
    ws.row_dimensions[row_num].height = 16

def bkt(pos):
    if not isinstance(pos, int): return ">100"
    for lo, hi, k in [(1,3,"1-3"),(4,10,"4-10"),(11,20,"11-20"),(21,30,"21-30"),
                      (31,40,"31-40"),(41,50,"41-50"),(51,100,"51-100")]:
        if lo <= pos <= hi: return k
    return ">100"

def score(buckets):
    return sum(buckets.get(k,0)*BUCKET_SCORES[k] for k in BUCKET_KEYS)

def export_excel(results, filename, targets, history):
    wb = Workbook()

    # Sheet 1 — Results
    ws = wb.active
    ws.title = "Results"
    ws.append(["#","Keyword","Position","Domain","Title","Snippet","URL","Target","Root"])
    _hdr(ws)
    ws.freeze_panes = "A2"
    for i, r in enumerate(results, 1):
        it = r.get("is_target", False)
        ws.append([i, r.get("keyword",""), r.get("position"), r.get("domain",""),
                   r.get("title",""), r.get("snippet",""), r.get("url",""),
                   "✅" if it else "", r.get("target_root") or ""])
        _row(ws, ws.max_row, it)
    for col, w in zip("ABCDEFGHI", [5,32,8,28,38,42,52,6,22]):
        ws.column_dimensions[col].width = w

    if not history:
        wb.save(filename)
        return

    last = history[-1]["results"]

    # Sheet 2 — Target Stats
    ws2 = wb.create_sheet("Target Stats")
    ws2.append(["Domain","Root"]+[f"Pos {k}" for k in BUCKET_KEYS]+["Score","KW","Keywords"])
    _hdr(ws2)
    dom_bkt = defaultdict(lambda: {k:0 for k in BUCKET_KEYS})
    dom_kw  = defaultdict(set)
    dom_rt  = {}
    for r in last:
        if not r.get("is_target"): continue
        pos = r.get("position")
        if not isinstance(pos, int): continue
        b = bkt(pos)
        if b not in BUCKET_KEYS: continue
        d = r["domain"]
        dom_bkt[d][b] += 1
        dom_kw[d].add(r["keyword"])
        dom_rt[d] = r.get("target_root","")
    rows2 = sorted(dom_bkt.items(), key=lambda x: score(x[1]), reverse=True)
    for d, bkts in rows2:
        ws2.append([d, dom_rt.get(d,"")]+[bkts[k] for k in BUCKET_KEYS]
                   +[score(bkts), len(dom_kw[d]), "; ".join(sorted(dom_kw[d]))])
        _row(ws2, ws2.max_row, True)

    # Sheet 3 — Dynamics
    ws3 = wb.create_sheet("Dynamics")
    labels = [e.get("timestamp","")[:10] for e in history]
    ws3.append(["Keyword","Domain","Current","Trend"]+labels+["Avg","Best","Worst"])
    _hdr(ws3)
    combos = {(r["keyword"], r["domain"]) for e in history for r in e["results"] if r.get("is_target")}
    for kw, dom in sorted(combos):
        positions = []
        for e in history:
            found = [r["position"] for r in e["results"]
                     if r.get("is_target") and r["keyword"]==kw
                     and r["domain"]==dom and isinstance(r.get("position"),int)]
            positions.append(min(found) if found else None)
        valid = [p for p in positions if p is not None]
        if not valid: continue
        cur  = valid[-1]
        diff = (valid[-2] - cur) if len(valid) >= 2 else 0
        trend = "New" if len(valid)==1 else (f"↑{diff}" if diff>0 else (f"↓{abs(diff)}" if diff<0 else "="))
        ws3.append([kw, dom, cur, trend]+[p if p is not None else "—" for p in positions]
                   +[round(sum(valid)/len(valid),1), min(valid), max(valid)])
        _row(ws3, ws3.max_row, True)

    wb.save(filename)


# ==========================
# REPORT for Telegram
# ==========================

def build_report(proj, results, duration, end_time, pages):
    dom = defaultdict(lambda: {"pos":[], "kw":set()})
    for r in results:
        if not r.get("is_target"): continue
        pos = r.get("position")
        if not isinstance(pos, int): continue
        root = r.get("target_root") or r.get("domain","")
        dom[root]["pos"].append(pos)
        dom[root]["kw"].add(r["keyword"])

    lines = []
    for d in sorted(dom):
        pos = dom[d]["pos"]
        if not pos: continue
        kw  = len(dom[d]["kw"])
        avg = round(sum(pos)/len(pos), 1)
        t   = [sum(1 for p in pos if p<=3),
               sum(1 for p in pos if 4<=p<=10),
               sum(1 for p in pos if 11<=p<=20),
               sum(1 for p in pos if 21<=p<=50),
               sum(1 for p in pos if 51<=p<=100)]
        icons = ["🥇 1–3","🟢 4–10","🟡 11–20","🟠 21–50","🔴 51–100"]
        bars  = "  ".join(f"{ic}: {n}" for ic, n in zip(icons, t) if n)
        lines.append(f"🌐 <b>{d}</b>\n   {bars}\n   📌 KW: {kw}  ⌀ поз: {avg}")

    total_kw = len({r["keyword"] for r in results})
    block    = "\n\n".join(lines) if lines else "— немає попадань —"

    return (
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>SERP ЗВІТ</b> [ручний]\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📁 <b>{proj['name']}</b>\n"
        f"🗓 {end_time.strftime('%d.%m.%Y %H:%M')}\n"
        f"🌍 {proj.get('location','')}  gl={proj.get('gl','')}  hl={proj.get('hl','')}\n"
        f"⏱ {duration:.0f} сек\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Ключів: {total_kw}  📄 Сторінок: {pages}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>ПОЗИЦІЇ ПО ДОМЕНАХ</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{block}\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


# ==========================
# KEYBOARDS
# ==========================

def kb_main():
    return {"inline_keyboard": [
        [{"text": "🚀  Запустити парсинг",  "callback_data": "go:parse"}],
        [{"text": "📋  Мої проєкти",         "callback_data": "go:list"}],
        [{"text": "📈  Остання статистика",   "callback_data": "go:stats"}],
    ]}

def kb_projects(projects):
    rows = [[{"text": f"📁 {p['name']}  ({len(p.get('keywords',[]))} KW)",
              "callback_data": f"proj:{i}"}]
            for i, p in enumerate(projects)]
    rows.append([{"text": "◀️ Назад", "callback_data": "go:main"}])
    return {"inline_keyboard": rows}

def kb_pages(pi):
    row = [{"text": f"{n} стор.", "callback_data": f"pages:{pi}:{n}"} for n in [1,2,3,5,10]]
    return {"inline_keyboard": [row, [{"text": "◀️ Назад", "callback_data": "go:parse"}]]}

def kb_confirm(pi, pg):
    return {"inline_keyboard": [[
        {"text": "✅ Запустити", "callback_data": f"run:{pi}:{pg}"},
        {"text": "❌ Скасувати", "callback_data": "go:main"},
    ]]}

def kb_back():
    return {"inline_keyboard": [[{"text": "◀️ Назад", "callback_data": "go:main"}]]}


# ==========================
# SCREEN texts
# ==========================

def t_main():
    return ("╔════════════════════════╗\n"
            "║  🤖  <b>SERP Parser Bot</b>   ║\n"
            "╚════════════════════════╝\n\n"
            "Що хочеш зробити?")

def t_list(projects):
    if not projects:
        return "📭 Проєктів ще немає. Створи їх у Streamlit."
    lines = ["🗂 <b>Оберіть проєкт:</b>\n"]
    for p in projects:
        kw = len(p.get("keywords", []))
        lines.append(f"📁 <b>{p['name']}</b>  —  🌍 {p.get('location','—')}  |  🔑 {kw} KW")
    return "\n\n".join(lines)

def t_pages(proj):
    kw = len(proj.get("keywords", []))
    return (f"📁 <b>{proj['name']}</b>\n"
            f"🌍 {proj.get('location','—')}  |  🔑 {kw} ключів\n\n"
            f"📄 <b>Скільки сторінок парсити?</b>\n"
            f"<i>1 стор. = 10 результатів SERP</i>")

def t_confirm(proj, pages):
    kw = len(proj.get("keywords", []))
    return (f"🔎 <b>Підтвердження</b>\n\n"
            f"📁 {proj['name']}\n"
            f"🌍 {proj.get('location','—')}\n"
            f"🔑 {kw} ключів  ×  📄 {pages} стор. = ~{kw*pages} запитів\n\n"
            f"Запустити?")

def t_stats():
    history = load_history()
    if not history:
        return "📈 <b>Статистика</b>\n\nІсторія порожня."
    lines = ["📈 <b>Остання статистика</b>\n"]
    seen  = set()
    for e in reversed(history):
        n = e.get("project","—")
        if n in seen: continue
        seen.add(n)
        ts      = e.get("timestamp","")[:16]
        res     = e.get("results",[])
        total   = len({r["keyword"] for r in res})
        targets = [r for r in res if r.get("is_target")]
        top3    = sum(1 for r in targets if isinstance(r.get("position"),int) and r["position"]<=3)
        top10   = sum(1 for r in targets if isinstance(r.get("position"),int) and r["position"]<=10)
        lines.append(f"📁 <b>{n}</b>  <i>({ts})</i>\n"
                     f"   🔑 {total} KW  |  🎯 {len(targets)} попадань\n"
                     f"   🥇 Топ-3: {top3}  |  🟢 Топ-10: {top10}")
    return "\n\n".join(lines)


# ==========================
# RUN PARSING
# ==========================

def run_parsing(chat_id, msg_id, proj: dict, pages: int):
    proj_run          = dict(proj)
    proj_run["pages"] = pages
    kw                = len(proj_run.get("keywords", []))

    edit_msg(chat_id, msg_id,
             f"⏳ <b>Парсинг запущено...</b>\n\n"
             f"📁 {proj_run['name']}\n"
             f"🔑 {kw} ключів  ×  📄 {pages} стор. = ~{kw*pages} запитів\n\n"
             f"<i>Зачекайте, це займе кілька хвилин...</i>")

    t0      = datetime.datetime.now()
    results = asyncio.run(parse_project(proj_run))
    t1      = datetime.datetime.now()
    dur     = (t1 - t0).total_seconds()

    targets = proj_run.get("target_domains", []) or []
    results = enrich(results, targets)
    save_history(proj_run, results)

    hist    = [h for h in load_history() if h.get("project") == proj_run["name"]]
    fname   = f"SERP_{proj_run['name']}_{t1.strftime('%Y%m%d_%H%M')}.xlsx"
    export_excel(results, fname, targets, hist)

    report = build_report(proj_run, results, dur, t1, pages)
    edit_msg(chat_id, msg_id, f"✅ <b>Готово!</b>\n\n{report}\n\n📎 Відправляю файл...")

    if not send_doc(chat_id, fname,
                    caption=f"📊 {proj_run['name']} | {t1.strftime('%d.%m.%Y %H:%M')}"):
        send_msg(chat_id, "⚠️ Не вдалося відправити файл.")

    try: os.remove(fname)
    except Exception: pass

    send_msg(chat_id, t_main(), markup=kb_main())


# ==========================
# HANDLERS
# ==========================

def on_callback(cq):
    cq_id   = cq["id"]
    chat_id = cq["message"]["chat"]["id"]
    msg_id  = cq["message"]["message_id"]
    from_id = str(cq["from"]["id"])
    data    = cq.get("data", "")

    answer_cb(cq_id)

    # Авторизація
    if CHAT_ID and from_id != CHAT_ID:
        print(f"[auth] denied from_id={from_id} (allowed={CHAT_ID})")
        send_msg(chat_id, "⛔ Немає доступу.")
        return

    projects = load_projects()

    if data == "go:main":
        edit_msg(chat_id, msg_id, t_main(), markup=kb_main())

    elif data == "go:parse":
        if not projects:
            edit_msg(chat_id, msg_id, "📭 Проєктів немає. Створи їх у Streamlit.", markup=kb_back())
        else:
            edit_msg(chat_id, msg_id, t_list(projects), markup=kb_projects(projects))

    elif data == "go:list":
        if not projects:
            edit_msg(chat_id, msg_id, "📭 Проєктів немає.", markup=kb_back())
        else:
            lines = ["📋 <b>Мої проєкти:</b>\n"]
            for i, p in enumerate(projects, 1):
                kw  = len(p.get("keywords", []))
                dom = ", ".join(p.get("target_domains", []) or ["—"])
                lines.append(f"<b>{i}. {p['name']}</b>\n"
                              f"   🌍 {p.get('location','—')}  |  🔑 {kw} KW\n"
                              f"   🎯 {dom}")
            edit_msg(chat_id, msg_id, "\n\n".join(lines), markup=kb_back())

    elif data == "go:stats":
        edit_msg(chat_id, msg_id, t_stats(), markup=kb_back())

    elif data.startswith("proj:"):
        pi = int(data.split(":")[1])
        if pi >= len(projects):
            edit_msg(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        edit_msg(chat_id, msg_id, t_pages(projects[pi]), markup=kb_pages(pi))

    elif data.startswith("pages:"):
        _, pi, pg = data.split(":")
        pi, pg = int(pi), int(pg)
        if pi >= len(projects):
            edit_msg(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        edit_msg(chat_id, msg_id, t_confirm(projects[pi], pg), markup=kb_confirm(pi, pg))

    elif data.startswith("run:"):
        _, pi, pg = data.split(":")
        pi, pg = int(pi), int(pg)
        if pi >= len(projects):
            edit_msg(chat_id, msg_id, "❌ Проєкт не знайдено.")
            return
        run_parsing(chat_id, msg_id, projects[pi], pg)


def on_message(msg):
    chat_id = msg["chat"]["id"]
    from_id = str(msg["from"]["id"])

    if CHAT_ID and from_id != CHAT_ID:
        print(f"[auth] denied from_id={from_id}")
        return

    send_msg(chat_id, t_main(), markup=kb_main())


# ==========================
# MAIN
# ==========================

def main():
    print(f"[bot] TOKEN = {TOKEN[:20]}...")
    print(f"[bot] CHAT_ID = {CHAT_ID}")

    me = tg_get("getMe")
    if not me.get("ok"):
        print(f"[bot] ❌ getMe failed: {me}")
        sys.exit(1)

    username = me["result"].get("username", "?")
    print(f"[bot] ✅ Запущено як @{username}")

    # Скидаємо накопичені старі updates
    old = tg_get("getUpdates", {"offset": -1})
    if old.get("result"):
        skip = old["result"][-1]["update_id"] + 1
        tg_get("getUpdates", {"offset": skip})
        print(f"[bot] Скинуто старі updates, offset={skip}")

    # Надсилаємо меню при старті
    result = send_msg(CHAT_ID, t_main(), markup=kb_main())
    if result.get("ok"):
        print(f"[bot] Стартове меню надіслано в chat_id={CHAT_ID}")
    else:
        print(f"[bot] ⚠️ Не вдалося надіслати стартове меню: {result}")

    print("[bot] Слухаю... (Ctrl+C для зупинки)")

    offset = None
    while True:
        try:
            updates = get_updates(offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                print(f"[bot] upd={upd['update_id']} keys={list(upd.keys())}")
                if "callback_query" in upd:
                    on_callback(upd["callback_query"])
                elif "message" in upd:
                    on_message(upd["message"])
        except KeyboardInterrupt:
            print("[bot] Зупинено.")
            break
        except Exception as e:
            print(f"[bot] ❌ Помилка: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
