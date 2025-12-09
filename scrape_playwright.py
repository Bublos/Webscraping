"""
Echo24.cz web scraper – Playwright (dynamický scraping)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

from urllib.parse import urljoin

# ----------------- Debug / konfigurace -----------------

DEBUG = True  # při False poběží headless a bude méně výpisů


def dprint(msg: str) -> None:
    if DEBUG:
        print(f"[DEBUG] {msg}")


# ----------------- Konstanty -----------------

SOURCE_NAME = "echo24.cz"
HOMEPAGE = "https://echo24.cz/"

# User-Agent pro Windows 10 + Chrome
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# --- Preferovaný pořadník parserů pro BeautifulSoup (fallbacky) ---
PARSER_ORDER = ("lxml", "html5lib", "html.parser")


def make_soup(html: str | bytes) -> BeautifulSoup:
    """
    Vytvoří BeautifulSoup s fallbacky:
    1) lxml (nejrychlejší), 2) html5lib (tolerantní), 3) html.parser (vestavěný).
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="ignore")

    last_err = None
    for parser in PARSER_ORDER:
        try:
            return BeautifulSoup(html, parser)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(
        "Žádný dostupný HTML parser. Nainstaluj alespoň jeden z: lxml, html5lib "
        f"(poslední chyba: {last_err})"
    )


# --- Dodatečné regulární tagy podle obsahu článku ---

POLITICS_RX = re.compile(
    r"\b(politika|premiér|prezident|vláda|poslanecká sněmovna|senát|ministr|koalice|opozice|"
    r"evropská\s+(?:unie|komise)|nato)\b",
    re.IGNORECASE,
)

ECONOMY_RX = re.compile(
    r"\b(ekonomika|inflace|hdp|nezaměstnanost|rozpočet|deficit|mzdy|obchod|export|dovoz|"
    r"investice|kurz|koruna|čnb|úrokové\s+sazby|trh)\b",
    re.IGNORECASE,
)

EMAIL_RX = re.compile(
    r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b"
)

PATTERN_TAGS: List[Tuple[re.Pattern, Optional[str]]] = [
    (POLITICS_RX, "Politika"),
    (ECONOMY_RX, "Ekonomika"),
    (EMAIL_RX, None),  # přidá přímo nalezenou e-mailovou adresu jako tag
]


# --- Timezone (Praha) ---

try:
    if ZoneInfo is None:
        raise ZoneInfoNotFoundError
    PRAGUE_TZ = ZoneInfo("Europe/Prague")
except Exception:
    try:
        import tzdata  # noqa: F401

        PRAGUE_TZ = ZoneInfo("Europe/Prague")
    except Exception:
        from dateutil import tz as _tz

        PRAGUE_TZ = _tz.gettz("Europe/Prague") or _tz.UTC


@dataclass
class Article:
    title: str
    url: str
    date: str
    author: str
    source: str
    content_snippet: str
    full_content: str
    tags: List[str]

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "url": self.url,
            "date": self.date,
            "author": self.author,
            "source": self.source,
            "content_snippet": self.content_snippet,
            "full_content": self.full_content,
            "tags": self.tags,
        }


# ----------------- Pomocné funkce -----------------


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def url_md5_8(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[-8:]


def parse_date_iso(dt_str: Optional[str]) -> str:
    """Parse a date string to ISO 8601 with Prague TZ; fallback to now()."""
    if dt_str:
        try:
            dt = dateparser.parse(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=PRAGUE_TZ)
            return dt.astimezone(PRAGUE_TZ).isoformat()
        except Exception:
            pass
    return datetime.now(PRAGUE_TZ).isoformat()


def target_path(root: Path, date_iso: str, url: str) -> Path:
    dt = dateparser.parse(date_iso)
    y = f"{dt.year:04d}"
    m = f"{dt.month:02d}"
    h8 = url_md5_8(url)
    fname = f"{SOURCE_NAME.split('.')[0]}-{dt:%Y%m%d}-{h8}.json"
    return root / SOURCE_NAME.split(".")[0] / y / m / fname


def existing_hashes(root: Path) -> Set[str]:
    """Projde existující JSON soubory a vrátí sadu 8-char hashů (kvůli duplicitám)."""
    hashes: Set[str] = set()
    base = root / SOURCE_NAME.split(".")[0]
    if not base.exists():
        return hashes
    for p in base.rglob("*.json"):
        m = re.search(r"-([0-9]{8})-([0-9a-f]{8})\.json$", p.name)
        if m:
            hashes.add(m.group(2))
    return hashes


def save_article(root: Path, art: Article) -> Path:
    out_path = target_path(root, art.date, art.url)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(art.to_dict(), f, indent=4, ensure_ascii=False)
    return out_path


# ----------------- Cookie lišta -----------------


def click_cookie_button(target, where: str) -> bool:
    """
    Zkusí najít a kliknout na tlačítko / element s textem souhlasu cookies
    v dané stránce nebo framu. Vrací True pokud klikl.
    """
    # konkrétní text z Echo24 + obecnější varianty
    texts = [
        "Rozumím a přijímám",
        "Rozumím, přijímám",
        "Rozumím",
        "Přijmout vše",
        "Přijmout cookies",
        "Souhlasím",
        "Souhlasím se vším",
        "Accept all",
        "I agree",
    ]

    for txt in texts:
        # 1) tlačítko s tímto textem
        selectors = [
            f"button:has-text('{txt}')",
            f"text='{txt}'",  # libovolný element s tímto textem
        ]
        for sel in selectors:
            loc = target.locator(sel)
            try:
                if loc.count() == 0:
                    continue
                if not loc.first.is_visible():
                    continue
                dprint(f"Klikám na cookie tlačítko ({where}): {txt} (selector: {sel})")
                loc.first.click()
                target.wait_for_timeout(500)
                return True
            except Exception:
                # když tenhle selector failne, zkus další
                continue
    return False


def handle_cookies(page) -> None:
    """
    Pokusí se odkliknout cookie lištu (na stránce i v iframu).
    Pro debugging uvidíš, jak tlačítko problikne.
    """
    # chvilku počkej, ať má CMP šanci se vykreslit
    page.wait_for_timeout(1000)

    # 1) hlavní stránka
    if click_cookie_button(page, "main page"):
        return

    # 2) iframy (CMP často běží v iframu)
    for frame in page.frames:
        if frame is page.main_frame:
            continue
        if click_cookie_button(frame, "iframe"):
            return

    dprint("Cookie tlačítko jsem nenašel – pokračuju bez odkliknutí.")


# ----------------- Playwright část -----------------


def discover_article_urls(page) -> List[str]:
    """
    Z homepage získá seznam URL článků.
    Používá Playwright page (dynamické načítání).
    """
    dprint(f"Otevírám homepage: {HOMEPAGE}")

    try:
        page.goto(HOMEPAGE, wait_until="domcontentloaded", timeout=15000)
    except PlaywrightTimeoutError:
        dprint("Timeout při načítání homepage, beru částečný HTML obsah...")

    handle_cookies(page)

    html = page.content()
    soup = make_soup(html)

    urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if href.startswith("/"):
            href = urljoin(HOMEPAGE, href)
        if not href.startswith("http"):
            continue
        if "echo24.cz" not in href:
            continue
        if "/a/" in href:
            urls.add(href.split("#")[0])

    urls_sorted = sorted(urls)
    dprint(f"Na homepage jsem našel {len(urls_sorted)} kandidátních URL článků.")
    return urls_sorted


def extract_article(page, url: str) -> Optional[Article]:
    """
    Načte článek pomocí Playwright page.goto a vytáhne data přes BeautifulSoup.
    """
    dprint(f"Načítám článek: {url}")

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
    except PlaywrightTimeoutError:
        dprint(f"Timeout při načítání článku: {url}, zkusím pokračovat s partial HTML...")
    except Exception as e:
        print(f"[ERROR] Playwright error for {url}: {e}", file=sys.stderr)
        return None

    handle_cookies(page)

    html = page.content()
    soup = make_soup(html)

    # -------- Title --------
    title_el = soup.select_one("h1") or soup.select_one("meta[property='og:title']")
    if title_el:
        if getattr(title_el, "name", "").lower() == "meta":
            title_text = title_el.get("content", "").strip()
        else:
            title_text = title_el.get_text(strip=True)
    else:
        title_text = ""

    # -------- Date --------
    date_text = None
    meta_time = soup.select_one("meta[property='article:published_time']")
    if meta_time and meta_time.get("content"):
        date_text = meta_time.get("content").strip()

    if not date_text:
        time_el = soup.select_one("time[datetime]")
        if time_el:
            date_text = (time_el.get("datetime", "") or time_el.get_text(strip=True)).strip()

    date_iso = parse_date_iso(date_text)

    # -------- Author --------
    author_text = ""
    meta_author = soup.select_one("meta[name='author']")
    if meta_author and meta_author.get("content"):
        author_text = meta_author.get("content").strip()

    if not author_text:
        by = soup.select_one(".author, .Article-author, .byline, [itemprop='author']")
        if by:
            author_text = normalize_whitespace(by.get_text(" ", strip=True))

    if not author_text:
        author_text = "Redakce Echo24"

    # -------- Content --------
    body_candidates = soup.select(
        ".article-body, .Article-content, [itemprop='articleBody'], .content, .article__content"
    )
    paragraphs: List[str] = []
    for body in body_candidates:
        ps = [
            normalize_whitespace(p.get_text(" ", strip=True))
            for p in body.select("p")
            if p.get_text(strip=True)
        ]
        paragraphs.extend(ps)

    if not paragraphs:
        for p in soup.select("article p"):
            txt = normalize_whitespace(p.get_text(" ", strip=True))
            if txt:
                paragraphs.append(txt)

    full_content = "\n\n".join(paragraphs)
    if not full_content.strip():
        dprint(f"Článek nemá žádný obsah, skip: {url}")
        return None

    snippet = full_content[:200] + ("…" if len(full_content) > 200 else "")

    # -------- Tags --------
    tags: List[str] = []
    for el in soup.select("a[rel='tag'], .tags a, .Article-tags a"):
        t = normalize_whitespace(el.get_text(strip=True))
        if t:
            tags.append(t)

    if not tags:
        meta_kw = soup.select_one("meta[name='keywords']")
        if meta_kw and meta_kw.get("content"):
            tags = [t.strip() for t in meta_kw.get("content").split(",") if t.strip()]

    # --- Doplň chytré tagy podle regexů ---
    haystack = " \n ".join(
        [
            url or "",
            title_text or "",
            full_content[:2000] or "",  # jen úvod článku kvůli výkonu
        ]
    )

    for rx, tag_name in PATTERN_TAGS:
        for match in rx.finditer(haystack):
            if tag_name is None:
                try:
                    captured = match.group(1)
                except IndexError:
                    captured = match.group(0)
                if captured:
                    tags.append(captured.lower())
            else:
                tags.append(tag_name)

    # Odstranění duplicit (case-insensitive, zachová pořadí)
    seen_lower = set()
    deduped: List[str] = []
    for t in tags:
        k = t.lower()
        if k not in seen_lower:
            seen_lower.add(k)
            deduped.append(t)
    tags = deduped

    dprint(
        f"Hotovo: '{title_text}' "
        f"(tagů: {len(tags)}, délka obsahu: {len(full_content)} znaků)"
    )

    return Article(
        title=title_text,
        url=url,
        date=date_iso,
        author=author_text,
        source=SOURCE_NAME,
        content_snippet=snippet,
        full_content=full_content,
        tags=tags,
    )


def run_once_playwright(root: Path, limit: Optional[int] = None) -> Tuple[int, int]:
    """
    Jedno “kolo” scrapingu – otevře homepage, najde URL článků,
    projde je, uloží nové články do JSON.
    V debug režimu otevírá ne-headless prohlížeč a vypisuje detailní logy.
    """
    saved = 0
    skipped = 0

    dprint("Spouštím Playwright...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not DEBUG,
            slow_mo=500 if DEBUG else 0,  # při DEBUG zpomal, ať to vidíš
        )
        context = browser.new_context(
            user_agent=UA,
            locale="cs-CZ",
            timezone_id="Europe/Prague",
        )

        homepage_page = context.new_page()
        urls = discover_article_urls(homepage_page)

        if limit:
            urls = urls[:limit]
            dprint(f"Limit {limit} → budu zpracovávat {len(urls)} URL.")

        seen = existing_hashes(root)
        dprint(f"Už existuje {len(seen)} hashů (případně duplicit).")

        article_page = context.new_page()

        for idx, url in enumerate(urls, start=1):
            h8 = url_md5_8(url)
            if h8 in seen:
                skipped += 1
                dprint(f"[{idx}/{len(urls)}] SKIP (duplicitní hash={h8}): {url}")
                continue

            dprint(f"[{idx}/{len(urls)}] Scrapuju: {url}")

            try:
                art = extract_article(article_page, url)
                if not art or not art.full_content:
                    skipped += 1
                    dprint(f"[{idx}/{len(urls)}] Žádný obsah, skip.")
                    continue

                out_path = save_article(root, art)
                seen.add(h8)
                saved += 1

                dprint(f"[{idx}/{len(urls)}] Uloženo do: {out_path}")

                # mírné zpomalení kvůli serveru
                time.sleep(0.5)
            except Exception as e:
                print(f"[ERROR] Error for {url}: {e}", file=sys.stderr)

        browser.close()

    dprint(f"Kolo dokončeno. saved={saved}, skipped={skipped}")
    return saved, skipped


# ----------------- CLI / main -----------------


def main():
    ap = argparse.ArgumentParser(description="Echo24.cz scraper (Playwright) → JSON store")
    ap.add_argument("--root", default="./data", help="Root folder for data store")
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Max number of homepage articles to process",
    )
    ap.add_argument(
        "--loop", action="store_true",
        help="Run forever; repeat every hour",
    )
    args = ap.parse_args()

    root = Path(args.root).resolve()
    print(f"Data root: {root}")

    def cycle():
        saved, skipped = run_once_playwright(root, limit=args.limit)
        now = datetime.now(PRAGUE_TZ).isoformat(timespec="seconds")
        print(f"[{now}] saved={saved} skipped={skipped}")

    try:
        if args.loop:
            while True:
                cycle()
                now = datetime.now(PRAGUE_TZ)
                seconds_to_next_hour = (60 - now.minute - 1) * 60 + (60 - now.second)
                time.sleep(max(60, seconds_to_next_hour))
        else:
            cycle()
    except KeyboardInterrupt:
        print("\n[INFO] Ukončeno uživatelem (Ctrl+C).")


if __name__ == "__main__":
    main()
