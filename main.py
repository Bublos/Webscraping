"""
Echo24.cz web scraper
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

SOURCE_NAME = "echo24.cz"
HOMEPAGE = "https://echo24.cz/"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)

# --- Preferovaný pořadník parserů pro BeautifulSoup (fallbacky) ---
PARSER_ORDER = ("lxml", "html5lib", "html.parser")

def make_soup(html: bytes) -> BeautifulSoup:
    """
    Vytvoří BeautifulSoup s fallbacky:
    1) lxml (nejrychlejší), 2) html5lib (tolerantní), 3) html.parser (vestavěný).
    """
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
# 1) Politika = statický tag
# 2) Ekonomika = statický tag
# 3) E-mail = zachytávací skupina -> do tagů se přidá nalezená e-mailová adresa

# Kompilované regulární výrazy (rychlejší a přehlednější)
POLITICS_RX = re.compile(
    r"\b(politika|premiér|prezident|vláda|poslanecká sněmovna|senát|ministr|koalice|opozice|"
    r"evropská\s+(?:unie|komise)|nato)\b",
    re.IGNORECASE
)

ECONOMY_RX = re.compile(
    r"\b(ekonomika|inflace|hdp|nezaměstnanost|rozpočet|deficit|mzdy|obchod|export|dovoz|"
    r"investice|kurz|koruna|čnb|úrokové\s+sazby|trh)\b",
    re.IGNORECASE
)

# Zachytávací skupina (group 1) vrací nalezenou e-mailovou adresu
EMAIL_RX = re.compile(
    r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b"
)

# Seznam (regex, tag_name). Pokud je tag_name None, použije se zachycená skupina (viz extract_article).
PATTERN_TAGS: List[Tuple[re.Pattern, Optional[str]]] = [
    (POLITICS_RX, "Politika"),
    (ECONOMY_RX, "Ekonomika"),
    (EMAIL_RX, None),  # přidá přímo nalezenou e-mailovou adresu jako tag
]



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


def http_get(url: str, *, timeout: int = 15) -> requests.Response:
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    resp.raise_for_status()
    return resp


# Vyčistí nadbytečné bílé znaky
def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

# Hash z url a uloží posledních 8 znaků (kvůli duplikaci)
def url_md5_8(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[-8:]


def discover_article_urls() -> List[str]:
    r = http_get(HOMEPAGE)
    soup = make_soup(r.content)
    urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if href.startswith("/"):
            href = requests.compat.urljoin(HOMEPAGE, href)
        if not href.startswith("http"):
            continue
        if "echo24.cz" not in href:  # filtr zda je opravdu ze stránky echo24
            continue
        if "/a/" in href:  # filtr zda články obsahují "/a/" (pattern)
            urls.add(href.split("#")[0])
    return sorted(urls)  # vrací unikátní URL


# Zajištění stejné timezone pro ukládání
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


def extract_article(url: str) -> Optional[Article]:
    r = http_get(url)
    soup = make_soup(r.content)

    # -------- Title --------
    title = (
        soup.select_one("h1") or
        soup.select_one("meta[property='og:title']")
    )
    if title:
        title_text = title.get_text(strip=True) if title.name != "meta" else title.get("content", "").strip()
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
            date_text = time_el.get("datetime", "").strip() or time_el.get_text(strip=True)
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
        ps = [normalize_whitespace(p.get_text(" ", strip=True)) for p in body.select("p") if p.get_text(strip=True)]
        paragraphs.extend(ps)
    if not paragraphs:
        for p in soup.select("article p"):
            txt = normalize_whitespace(p.get_text(" ", strip=True))
            if txt:
                paragraphs.append(txt)

    full_content = "\n\n".join(paragraphs)
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
    haystack = " \n ".join([
        url or "",
        title_text or "",
        full_content[:2000] or "",   # jen úvod článku kvůli výkonu
    ])

    # finditer => přidá všechny shody (např. více e-mailů)
    for rx, tag_name in PATTERN_TAGS:
        for match in rx.finditer(haystack):
            if tag_name is None:
                # Zachytávací skupina => přidáme konkrétní text (group 1)
                try:
                    captured = match.group(1)
                except IndexError:
                    captured = match.group(0)
                if captured:
                    tags.append(captured.lower())
            else:
                tags.append(tag_name)

    # Odstraň duplicity case-insensitive, zachovej pořadí a preferuj původní zápis
    seen_lower = set()
    deduped: List[str] = []
    for t in tags:
        k = t.lower()
        if k not in seen_lower:
            seen_lower.add(k)
            deduped.append(t)
    tags = deduped

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


def target_path(root: Path, date_iso: str, url: str) -> Path:
    dt = dateparser.parse(date_iso)
    y = f"{dt.year:04d}"
    m = f"{dt.month:02d}"
    h8 = url_md5_8(url)
    fname = f"{SOURCE_NAME.split('.')[0]}-{dt:%Y%m%d}-{h8}.json"
    return root / SOURCE_NAME.split(".")[0] / y / m / fname


def existing_hashes(root: Path) -> Set[str]:
    """Scan root for existing JSON files and collect their 8-char hashes to skip duplicates."""
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

"""
    Varianta s odsazenýma odstavcema:
    def save_article(root: Path, art: Article) -> Path:
        out_path = target_path(root, art.date, art.url)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        data = art.to_dict()
        json_text = json.dumps(data, indent=4, ensure_ascii=False)
        json_text = json_text.replace("\\n", "\n")

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(json_text)

        return out_path
"""


def run_once(root: Path, limit: Optional[int] = None) -> Tuple[int, int]:
    urls = discover_article_urls()  # získá kandidátní URL
    if limit:
        urls = urls[:limit]

    seen = existing_hashes(root)  # načte známé hashe
    saved = 0
    skipped = 0

    for url in urls:
        h8 = url_md5_8(url)
        if h8 in seen:
            skipped += 1
            continue
        try:
            art = extract_article(url)
            if not art or not art.full_content:
                skipped += 1
                continue
            save_article(root, art)
            seen.add(h8)
            saved += 1
            time.sleep(0.8)
        except requests.HTTPError as e:
            print(f"HTTP error for {url}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Error for {url}: {e}", file=sys.stderr)
    return saved, skipped


def main():
    ap = argparse.ArgumentParser(description="Echo24.cz scraper → JSON store")
    ap.add_argument("--root", default="./data", help="Root folder for data store")
    ap.add_argument("--limit", type=int, default=None, help="Max number of homepage articles to process")
    ap.add_argument("--loop", action="store_true", help="Run forever; repeat every hour")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    print(f"Data root: {root}")

    def cycle():  # Zavolá run_once, získá saved/skipped
        saved, skipped = run_once(root, limit=args.limit)
        now = datetime.now(PRAGUE_TZ).isoformat(timespec="seconds")
        print(f"[{now}] saved={saved} skipped={skipped}")

    if args.loop:
        while True:
            cycle()
            now = datetime.now(PRAGUE_TZ)
            seconds_to_next_hour = (60 - now.minute - 1) * 60 + (60 - now.second)
            time.sleep(max(60, seconds_to_next_hour))
    else:
        cycle()


if __name__ == "__main__":
    main()
