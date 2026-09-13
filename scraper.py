#!/usr/bin/env python3
"""
Hydra Launcher Full Custom Source Scraper (High-Performance Async).

Features:
  - Concurrent fetching using asyncio + aiohttp
  - Dynamic page limits (scrapes ALL available pages)
  - Multi-tier deduplication (InfoHash / Canonical URIs / Normalized Titles)
  - Non-destructive incremental updates to source.json
"""

import asyncio
import base64
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse

import aiohttp
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

NYAA_BASE = "https://nyaa.si"
SUKEBEI_BASE = "https://sukebei.nyaa.si"
RYUUGAMES_BASE = "https://www.ryuugames.com"

# Network settings
MAX_CONCURRENT_REQUESTS = 15  # Prevents target IP blocks
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

SOURCE_NAME = "VN & Nyaa Full Source"
OUTPUT_FILE = "source.json"

# ---------------------------------------------------------------------------
# Helpers & Utilities
# ---------------------------------------------------------------------------

def normalize_size(size_str: str) -> str:
    """Normalize binary size units to standardized single-letter notation."""
    size_str = size_str.strip()
    return re.sub(r"([KMGT])iB", r"\1B", size_str, flags=re.IGNORECASE)

def extract_dedupe_key(entry: dict) -> str:
    """
    Generates a unique deduplication key for an entry.
    Priority: Torrent InfoHash -> Primary URI -> Title
    """
    uris = entry.get("uris", [])
    if uris:
        primary_uri = uris[0]
        if primary_uri.startswith("magnet:"):
            parsed = urlparse(primary_uri)
            query = parse_qs(parsed.query)
            xt = query.get("xt", [])
            for urn in xt:
                if urn.startswith("urn:btih:"):
                    return urn.split(":")[-1].lower()
        return primary_uri.strip().lower()
    
    return entry.get("title", "").strip().lower()

def load_existing_source(filepath: str) -> Tuple[dict, Dict[str, dict]]:
    """Loads existing JSON file and indexes entries by dedupe key."""
    if not os.path.exists(filepath):
        return {"name": SOURCE_NAME, "downloads": []}, {}

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            existing_downloads = data.get("downloads", [])
            existing_map = {}
            for item in existing_downloads:
                key = extract_dedupe_key(item)
                if key:
                    existing_map[key] = item
            
            print(f"[Storage] Loaded {len(existing_map)} existing entries from {filepath}.")
            return data, existing_map
    except Exception as exc:
        print(f"[WARN] Could not parse existing {filepath}: {exc}. Starting fresh.", file=sys.stderr)
        return {"name": SOURCE_NAME, "downloads": []}, {}

# ---------------------------------------------------------------------------
# Async Fetching Engine
# ---------------------------------------------------------------------------

async def fetch_text(
    session: aiohttp.ClientSession, 
    semaphore: asyncio.Semaphore, 
    url: str, 
    post_data: Optional[dict] = None,
    headers: Optional[dict] = None
) -> Optional[str]:
    """Fetch URL contents with concurrency bounds, timeouts, and retries."""
    async with semaphore:
        req_headers = {**HEADERS, **(headers or {})}
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if post_data:
                    async with session.post(url, data=post_data, headers=req_headers, timeout=REQUEST_TIMEOUT) as resp:
                        if resp.status == 404:
                            return None
                        resp.raise_for_status()
                        return await resp.text()
                else:
                    async with session.get(url, headers=req_headers, timeout=REQUEST_TIMEOUT) as resp:
                        if resp.status == 404:
                            return None
                        resp.raise_for_status()
                        return await resp.text()
            except Exception as exc:
                if attempt == MAX_RETRIES:
                    print(f"  [WARN] Failed {url} after {MAX_RETRIES} attempts: {exc}", file=sys.stderr)
                    return None
                await asyncio.sleep(1 * attempt)
    return None

# ---------------------------------------------------------------------------
# Nyaa / Sukebei Engine
# ---------------------------------------------------------------------------

async def get_nyaa_max_pages(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, base_url: str, category: str) -> int:
    """Inspects the pagination bar to discover total available pages."""
    url = f"{base_url}/?c={category}&p=1"
    html = await fetch_text(session, semaphore, url)
    if not html:
        return 1
    
    soup = BeautifulSoup(html, "lxml")
    pagination = soup.select("ul.pagination li")
    if not pagination:
        return 1
    
    pages = []
    for li in pagination:
        text = li.get_text(strip=True)
        if text.isdigit():
            pages.append(int(text))
    
    return max(pages) if pages else 1

async def parse_nyaa_page(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, base_url: str, category: str, page: int) -> List[dict]:
    url = f"{base_url}/?c={category}&p={page}"
    html = await fetch_text(session, semaphore, url)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table.torrent-list tbody tr")
    entries = []

    for row in rows:
        try:
            title_links = row.select("td[colspan='2'] a:not(.comments)")
            if not title_links:
                continue
            title = title_links[-1].get_text(strip=True)

            magnet_tag = row.find("a", href=re.compile(r"^magnet:"))
            if not magnet_tag:
                continue
            magnet_link = magnet_tag["href"]

            tds = row.find_all("td")
            file_size = normalize_size(tds[3].get_text(strip=True)) if len(tds) > 3 else "Unknown"
            raw_date = tds[4].get_text(strip=True) if len(tds) > 4 else ""

            upload_date = ""
            if raw_date:
                try:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
                    upload_date = dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                except Exception:
                    upload_date = raw_date

            entries.append({
                "title": title,
                "fileSize": file_size,
                "uploadDate": upload_date,
                "uris": [magnet_link]
            })
        except Exception:
            continue

    return entries

async def scrape_nyaa_site(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, base_url: str, category: str, label: str) -> List[dict]:
    print(f"[{label}] Determining total page count...")
    max_pages = await get_nyaa_max_pages(session, semaphore, base_url, category)
    print(f"[{label}] Found {max_pages} total pages. Starting parallel processing...")

    tasks = [parse_nyaa_page(session, semaphore, base_url, category, page) for page in range(1, max_pages + 1)]
    results = await asyncio.gather(*tasks)
    
    flat_entries = [entry for page_result in results for entry in page_result]
    print(f"[{label}] Parsed {len(flat_entries)} total entries.")
    return flat_entries

# ---------------------------------------------------------------------------
# RyuuGames Engine
# ---------------------------------------------------------------------------

async def fetch_ryuu_download_url(
    session: aiohttp.ClientSession, 
    semaphore: asyncio.Semaphore, 
    page_url: str, 
    link_key: str, 
    post_id: str, 
    shortcode_id: str
) -> Optional[str]:
    proc_url = RYUUGAMES_BASE + "/processing/"
    form_data = {
        "ryuu_sl_action": "process",
        "post_id": post_id,
        "shortcode_id": shortcode_id,
        "link_key": link_key,
    }
    headers = {"Referer": page_url}
    
    html = await fetch_text(session, semaphore, proc_url, post_data=form_data, headers=headers)
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")
        form = soup.find("form", id="continueForm")
        if not form:
            return None
        host_input = form.find("input", attrs={"name": "host"})
        if not host_input or not host_input.get("value"):
            return None
        decoded = json.loads(base64.b64decode(host_input["value"]).decode("utf-8"))
        return decoded.get("url")
    except Exception:
        return None

async def parse_ryuu_game_page(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, url: str) -> Optional[dict]:
    html = await fetch_text(session, semaphore, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else None
    if not title:
        return None

    file_size = "Unknown"
    size_match = re.search(r"(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|TiB|GB|MB|KB|TB)", soup.get_text(), re.I)
    if size_match:
        file_size = normalize_size(f"{size_match.group(1)} {size_match.group(2)}")

    buttons = soup.find_all("button", attrs={"data-link-key": True})
    seen_groups = set()
    dl_tasks = []

    for btn in buttons:
        link_key = btn.get("data-link-key")
        post_id = btn.get("data-post-id")
        shortcode_id = btn.get("data-shortcode-id")
        if not (link_key and post_id and shortcode_id):
            continue
        group_prefix = link_key.split("_")[0]
        if group_prefix in seen_groups:
            continue
        seen_groups.add(group_prefix)

        dl_tasks.append(fetch_ryuu_download_url(session, semaphore, url, link_key, post_id, shortcode_id))

    resolved_urls = await asyncio.gather(*dl_tasks)
    uris = [u for u in resolved_urls if u]

    if not uris:
        return None

    return {
        "title": title,
        "fileSize": file_size,
        "uploadDate": "",
        "uris": uris,
    }

async def scrape_ryuugames_category(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, category_path: str) -> Set[str]:
    game_urls = set()
    page = 1

    while True:
        url = f"{RYUUGAMES_BASE}{category_path.rstrip('/')}/page/{page}/" if page > 1 else f"{RYUUGAMES_BASE}{category_path}"
        html = await fetch_text(session, semaphore, url)
        if not html:
            break

        soup = BeautifulSoup(html, "lxml")
        headings = soup.find_all("h3")
        found = 0
        for heading in headings:
            link = heading.find("a")
            if link and link.get("href") and "ryuugames.com" in link["href"]:
                game_urls.add(link["href"])
                found += 1

        if found == 0:
            break
        page += 1

    return game_urls

async def scrape_ryuugames(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> List[dict]:
    print("[RyuuGames] Discovering game pages across categories...")
    categories = ["/", "/category/visualnovel/english-translated"]
    
    cat_tasks = [scrape_ryuugames_category(session, semaphore, cat) for cat in categories]
    cat_results = await asyncio.gather(*cat_tasks)
    
    all_game_urls: Set[str] = set().union(*cat_results)
    print(f"[RyuuGames] Found {len(all_game_urls)} game pages. Processing entries...")

    page_tasks = [parse_ryuu_game_page(session, semaphore, url) for url in sorted(all_game_urls)]
    results = await asyncio.gather(*page_tasks)

    entries = [r for r in results if r is not None]
    print(f"[RyuuGames] Parsed {len(entries)} valid entries.")
    return entries

# ---------------------------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------------------------

async def main():
    existing_data, existing_map = load_existing_source(OUTPUT_FILE)
    initial_count = len(existing_map)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession() as session:
        scrapers = [
            scrape_nyaa_site(session, semaphore, NYAA_BASE, "6_2", "Nyaa PC Games"),
            scrape_nyaa_site(session, semaphore, SUKEBEI_BASE, "1_3", "Sukebei Games"),
            scrape_ryuugames(session, semaphore)
        ]
        
        results = await asyncio.gather(*scrapers)

    # Flatten scrapers output
    all_new_entries = [entry for source_result in results for entry in source_result]

    # Deduplicate and merge into persistent map
    added_count = 0
    for entry in all_new_entries:
        key = extract_dedupe_key(entry)
        if key and key not in existing_map:
            existing_map[key] = entry
            added_count += 1

    final_downloads = list(existing_map.values())
    output_payload = {
        "name": SOURCE_NAME,
        "downloads": final_downloads
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n[Completed] Added {added_count} new unique entries.")
    print(f"[Completed] Total entries in {OUTPUT_FILE}: {len(final_downloads)} (Up from {initial_count})")

if __name__ == "__main__":
    asyncio.run(main())
