import asyncio
import aiohttp
from typing import List, Dict, Callable, Optional


def _extract_domain(url: str) -> tuple[str, str]:
    """
    Витягує домен з URL.
    Повертає (domain_original, domain_clean) — де clean без www.
    """
    try:
        # Видаляємо схему (http://, https://)
        without_scheme = url.split("://", 1)[-1] if "://" in url else url
        # Беремо тільки хост (до першого /)
        host = without_scheme.split("/")[0].lower()
        # Видаляємо port якщо є
        host = host.split(":")[0]
        domain_clean = host.removeprefix("www.")
        return host, domain_clean
    except Exception:
        return url, url


async def fetch_serp(
    session: aiohttp.ClientSession,
    api_key: str,
    keyword: str,
    location: str,
    gl: str,
    hl: str,
    page: int,
    semaphore: asyncio.Semaphore,
) -> List[Dict]:
    """
    Асинхронний запит до Serper API для однієї сторінки одного ключового слова.
    Повертає список результатів або порожній список у разі помилки.
    """
    async with semaphore:
        url = "https://google.serper.dev/search"
        headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "q": keyword,
            "location": location,
            "gl": gl,
            "hl": hl,
            "num": 10,
            "page": page,
            "autocorrect": False,   # не виправляти запит автоматично
        }

        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    results = []
                    organic = data.get("organic", [])

                    for idx, item in enumerate(organic, start=1):
                        position = (page - 1) * 10 + idx
                        link = item.get("link", "")
                        domain, domain_clean = _extract_domain(link)

                        results.append({
                            "keyword": keyword,
                            "position": position,
                            "domain": domain,           # оригінальний субдомен (для відображення)
                            "domain_clean": domain_clean,  # без www. (для порівняння)
                            "title": item.get("title", ""),
                            "snippet": item.get("snippet", ""),
                            "url": link,
                        })

                    return results

                else:
                    print(f"⚠️ Помилка {response.status} для '{keyword}' (сторінка {page})")
                    return []

        except asyncio.TimeoutError:
            print(f"⏰ Таймаут для '{keyword}' (сторінка {page})")
            return []
        except aiohttp.ClientError as e:
            print(f"🌐 Мережева помилка '{keyword}' (сторінка {page}): {e}")
            return []
        except Exception as e:
            print(f"⚠️ Несподівана помилка '{keyword}' (сторінка {page}): {e}")
            return []


async def run_project_ultra(
    project: Dict,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    max_concurrent_requests: int = 20,
) -> List[Dict]:
    """
    УЛЬТРА-ШВИДКИЙ парсинг: всі ключі та всі сторінки обробляються паралельно.

    Args:
        project: Словник з налаштуваннями проєкту
        progress_callback: Функція зворотного виклику для оновлення прогресу (current, total, keyword)
        max_concurrent_requests: Максимальна кількість одночасних HTTP-запитів

    Returns:
        Список результатів парсингу
    """
    api_key = project.get("api_key", "")
    keywords = project.get("keywords", [])
    location = project.get("location", "Germany")
    gl = project.get("gl", "de")
    hl = project.get("hl", "de")
    pages = project.get("pages", 1)

    if not api_key or not keywords:
        return []

    semaphore = asyncio.Semaphore(max_concurrent_requests)
    total_keywords = len(keywords)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Будуємо всі завдання з метаданими для прогресу
        task_meta = []  # (keyword, keyword_idx, page)
        coroutines = []

        for idx, keyword in enumerate(keywords, start=1):
            for page in range(1, pages + 1):
                task_meta.append((keyword, idx, page))
                coroutines.append(
                    fetch_serp(session, api_key, keyword, location, gl, hl, page, semaphore)
                )

        # Збираємо результати з відстеженням прогресу через as_completed
        all_results: List[Dict] = []

        if progress_callback:
            # Відстежуємо прогрес після завершення кожного запиту
            futures = [asyncio.ensure_future(c) for c in coroutines]
            done_count = 0
            reported_keywords: set = set()

            for future in asyncio.as_completed(futures):
                page_results = await future
                all_results.extend(page_results)
                done_count += 1

                # Визначаємо поточне ключове слово за індексом завданя
                meta_idx = done_count - 1
                if meta_idx < len(task_meta):
                    kw, kw_idx, _ = task_meta[meta_idx]
                    if kw_idx not in reported_keywords:
                        reported_keywords.add(kw_idx)
                        progress_callback(kw_idx, total_keywords, kw)
        else:
            all_results_nested = await asyncio.gather(*coroutines)
            for page_results in all_results_nested:
                all_results.extend(page_results)

    return all_results


async def run_project_batched(
    project: Dict,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    max_concurrent_requests: int = 15,
    batch_size: int = 50,
) -> List[Dict]:
    """
    Парсинг з batch-обробкою: запити виконуються пакетами для кращого контролю навантаження.

    Args:
        project: Словник з налаштуваннями проєкту
        progress_callback: Функція зворотного виклику для оновлення прогресу
        max_concurrent_requests: Максимальна кількість одночасних HTTP-запитів
        batch_size: Розмір пакету запитів

    Returns:
        Список результатів парсингу
    """
    api_key = project.get("api_key", "")
    keywords = project.get("keywords", [])
    location = project.get("location", "Germany")
    gl = project.get("gl", "de")
    hl = project.get("hl", "de")
    pages = project.get("pages", 1)

    if not api_key or not keywords:
        return []

    semaphore = asyncio.Semaphore(max_concurrent_requests)
    all_results: List[Dict] = []
    total_keywords = len(keywords)

    # Будуємо список всіх завдань
    all_tasks = [
        (keyword, idx, page)
        for idx, keyword in enumerate(keywords, start=1)
        for page in range(1, pages + 1)
    ]

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        reported_keywords: set = set()

        for batch_start in range(0, len(all_tasks), batch_size):
            batch = all_tasks[batch_start: batch_start + batch_size]

            coroutines = [
                fetch_serp(session, api_key, kw, location, gl, hl, page, semaphore)
                for kw, _, page in batch
            ]

            batch_results = await asyncio.gather(*coroutines)

            for page_results in batch_results:
                all_results.extend(page_results)

            # Сповіщаємо про унікальні ключові слова у цьому пакеті
            if progress_callback:
                for kw, kw_idx, _ in batch:
                    if kw_idx not in reported_keywords:
                        reported_keywords.add(kw_idx)
                        progress_callback(kw_idx, total_keywords, kw)

    return all_results


async def run_project(
    project: Dict,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    max_concurrent_requests: int = 15,
) -> List[Dict]:
    """
    Оптимізований парсинг — використовує batch-метод для балансу швидкості та надійності.
    """
    return await run_project_batched(
        project, progress_callback, max_concurrent_requests, batch_size=50
    )