
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


try:
    if ZoneInfo is None:
        raise ZoneInfoNotFoundError
    PRAGUE_TZ = ZoneInfo("Europe/Prague")
except Exception:
   
    try:
        import tzdata  # 
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

    
    if not resp.encoding or resp.encoding.lower() in ["utf-8", "utf8"]:
        import chardet
        detected = chardet.detect(resp.content)
        if detected and detected["encoding"]:
            resp.encoding = detected["encoding"]
        else:
            resp.encoding = "utf-8"

    return resp



def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def url_md5_8(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[-8:]


def discover_article_urls() -> List[str]:
    r = http_get(HOMEPAGE)
    soup = BeautifulSoup(r.text, "lxml")
    urls: Set[str] = set()
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if href.startswith("/"):
            href = requests.compat.urljoin(HOMEPAGE, href)
        if not href.startswith("http"):
            continue
        if "echo24.cz" not in href:
            continue
        if "/a/" in href:
            urls.add(href.split("#")[0])
    return sorted(urls)


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
    soup = BeautifulSoup(r.text, "lxml")

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

    full_content = normalize_whitespace("\n".join(paragraphs))
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


def run_once(root: Path, limit: Optional[int] = None) -> Tuple[int, int]:
    urls = discover_article_urls()
    if limit:
        urls = urls[:limit]

    seen = existing_hashes(root)
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

    def cycle():
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
