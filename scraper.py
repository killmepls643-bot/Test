#!/usr/bin/env python3
"""
Hydra Launcher Full Custom Source Scraper (High-Performance Async Enterprise).

Optimizations:
  - High-throughput TCP Connector with DNS Caching & Keep-Alive tuning.
  - Corrected Nyaa / Sukebei full-depth pagination discovery.
  - Fast-concurrent page indexing for WordPress/RyuuGames pagination.
  - Streaming incremental persistence & low-overhead deduplication.
  - Multi-tier hash normalizer (InfoHash v1/v2, Canonical URIs, Clean Titles).
"""

import asyncio
import base64
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration & Tuneables
# ---------------------------------------------------------------------------

NYAA_BASE = "https://nyaa.si"
SUKEBEI_BASE = "https://sukebei.nyaa.si"
RYUUGAMES_BASE = "https://www.ryuugames.com"

# Maximum concurrency bounds
MAX_CONCURRENT_REQUESTS = 40  # Tuning boundary for asyncio connection pool
DNS_CACHE_TTL = 300           # Cache resolved IPs for 5 minutes
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

SOURCE_NAME = "VN & Nyaa Full Source"
OUTPUT_FILE = "source.json"

# Regex Pre-compilations for Fast Execution
INFOHASH_HEX_REGEX = re.compile(r"urn:btih:([a-fA-F0-9]{40}|[a-fA-F0-9]{32}|[a-fA-F0-9]{64})", re.IGNORECASE)
SIZE_NORMALIZATION_REGEX = re.compile(r"([KMGT])iB", re.IGNORECASE)
SIZE_EXTRACT_REGEX = re.compile(r"(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|TiB|GB|MB|KB|TB)", re.IGNORECASE)
PAGINATION_LAST_PAGE_REGEX = re.compile(r"[?&]p=(\d+)")

# ---------------------------------------------------------------------------
# Helpers & Utilities
# ---------------------------------------------------------------------------

def normalize_size(size_str: str) -> str:
    """Normalize binary size units to standardized single-letter notation."""
    size_str = size_str.strip()
    return SIZE_NORMALIZATION_REGEX.sub(r"\1B", size_str)

def extract_infohash_from_magnet(magnet_uri: str) -> Optional[str]:
    """Extracts clean lowercase InfoHash from magnet string."""
    match = INFOHASH_HEX_REGEX.search(magnet_uri)
    if match:
        raw_hash = match.group(1)
        # Handle Base32 decoded hashes if 32 chars length
        if len(raw_hash) == 32:
            try:
                raw_hash = base64.b32decode(raw_hash.upper()).hex()
            except Exception:
                pass
        return raw_hash.lower()
    return None

def extract_dedupe_key(entry: dict) -> str:
    """
    Generates a deterministic unique deduplication key.
    Priority: Magnet InfoHash -> Canonical URI -> Normalized Title
    """
    uris = entry.get("uris", [])
    for uri in uris:
        if uri.startswith("magnet:"):
            infohash = extract_infohash_from_magnet(uri)
            if infohash:
                return f"hash:{infohash}"
            
        # Fallback to normalized HTTPS/HTTP links
        parsed = urlparse(uri)
        if parsed.scheme in ("http", "https"):
            clean_url = f"{parsed.netloc}{parsed.path}".rstrip("/").lower()
            return f"uri:{clean_url}"

    title = entry.get("title", "").strip().lower()
    title = re.sub(r"\s+", " ", title)
    return f"title:{title}"

def load_existing_source(filepath: str) -> Tuple[dict, Dict[str, dict]]:
    """Loads existing JSON source file and constructs in-memory deduplication index."""
    if not os.path.exists(filepath):
        return {"name": SOURCE_NAME, "downloads": []}, {}

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            existing_downloads = data.get("downloads", [])
            existing_map: Dict[str, dict] = {}
            
            for item in existing_downloads:
                key = extract_dedupe_key(item)
                if key:
                    existing_map[key] = item
            
            print(f"[Storage] Loaded {len(existing_map)} unique historical entries from {filepath}.")
            return data, existing_map
    except Exception as exc:
        print(f"[WARN] Failed parsing {filepath}: {exc}. Starting fresh.", file=sys.stderr)
        return {"name": SOURCE_NAME, "downloads": []}, {}

# ---------------------------------------------------------------------------
# Optimized Async Fetching Engine
# ---------------------------------------------------------------------------

async def fetch_text(
    session: aiohttp.ClientSession, 
    semaphore: asyncio.Semaphore, 
    url: str, 
    post_data: Optional[dict] = None,
    headers: Optional[dict] = None
) -> Optional[str]:
    """Execute concurrent HTTP request with retry logic and non-blocking backoff."""
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
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == MAX_RETRIES:
                    return None
                await asyncio.sleep(0.5 * attempt)
            except Exception:
                return None
    return None

# ---------------------------------------------------------------------------
# Nyaa / Sukebei Engine
# ---------------------------------------------------------------------------

async def get_nyaa_max_pages(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, base_url: str, category: str) -> int:
    """Accurately extracts maximum page count from pagination control links."""
    url = f"{base_url}/?c={category}&p=1"
    html = await fetch_text(session, semaphore, url)
    if not html:
        return 1
    
    soup = BeautifulSoup(html, "lxml")
    
    # Locate the "Last" page button or inspect all pagination hyperlinks directly
    pagination_links = soup.select("ul.pagination li a")
    max_page = 1
    
    for a_tag in pagination_links:
        href = a_tag.get("href", "")
        match = PAGINATION_LAST_PAGE_REGEX.search(href)
        if match:
            page_num = int(match.group(1))
            if page_num > max_page:
                max_page = page_num

    return max_page

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

            magnet_tag = row.find("a", href=re.compile(r"^magnet:", re.IGNORECASE))
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
    print(f"[{label}] Scanning site structure for max page boundary...")
    max_pages = await get_nyaa_max_pages(session, semaphore, base_url, category)
    print(f"[{label}] Detected {max_pages} pages. Dispatching concurrent fetch pool...")

    # Execute page parsing in batches to prevent event loop starvation
    chunk_size = 50
    all_entries = []
    
    for i in range(1, max_pages + 1, chunk_size):
        chunk_pages = range(i, min(i + chunk_size, max_pages + 1))
        tasks = [parse_nyaa_page(session, semaphore, base_url, category, p) for p in chunk_pages]
        results = await asyncio.gather(*tasks)
        for page_result in results:
            all_entries.extend(page_result)
            
    print(f"[{label}] Completed. Extracted {len(all_entries)} total entries.")
    return all_entries

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
    proc_url = f"{RYUUGAMES_BASE}/processing/"
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
    h1_tag = soup.find("h1")
    if not h1_tag:
        return None
        
    title = h1_tag.get_text(strip=True)

    file_size = "Unknown"
    size_match = SIZE_EXTRACT_REGEX.search(soup.get_text())
    if size_match:
        file_size = normalize_size(f"{size_match.group(1)} {size_match.group(2)}")

    buttons = soup.find_all("button", attrs={"data-link-key": True})
    seen_groups: Set[str] = set()
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

async def discover_ryuu_category_pages(
    session: aiohttp.ClientSession, 
    semaphore: asyncio.Semaphore, 
    category_path: str
) -> Set[str]:
    """Explores category pages using fast parallel probe scanning instead of serial loops."""
    discovered_urls: Set[str] = set()
    
    # Step 1: Probe initial page to check if category exists and extract game links
    first_url = f"{RYUUGAMES_BASE}{category_path}"
    html = await fetch_text(session, semaphore, first_url)
    if not html:
        return discovered_urls

    soup = BeautifulSoup(html, "lxml")
    for link in soup.select("h3 a[href*='ryuugames.com']"):
        discovered_urls.add(link["href"])

    # Step 2: Determine max page range by probing pagination links if available
    max_page = 1
    page_links = soup.select("a.page-numbers, ul.pagination a")
    for a in page_links:
        href = a.get("href", "")
        match = re.search(r"/page/(\d+)/", href)
        if match:
            p_num = int(match.group(1))
            if p_num > max_page:
                max_page = p_num

    # Step 3: Fast-scan speculative pages up to max_page + safety buffer in parallel steps
    async def process_cat_page(page_num: int) -> Set[str]:
        p_url = f"{RYUUGAMES_BASE}{category_path.rstrip('/')}/page/{page_num}/"
        p_html = await fetch_text(session, semaphore, p_url)
        if not p_html:
            return set()
        p_soup = BeautifulSoup(p_html, "lxml")
        return {a["href"] for a in p_soup.select("h3 a[href*='ryuugames.com']")}

    batch_size = 20
    current_page = 2
    empty_batches_in_a_row = 0

    while empty_batches_in_a_row < 2:
        pages_to_fetch = list(range(current_page, current_page + batch_size))
        tasks = [process_cat_page(p) for p in pages_to_fetch]
        results = await asyncio.gather(*tasks)
        
        batch_found = 0
        for res in results:
            discovered_urls.update(res)
            batch_found += len(res)

        if batch_found == 0:
            empty_batches_in_a_row += 1
        else:
            empty_batches_in_a_row = 0

        current_page += batch_size

    return discovered_urls

async def scrape_ryuugames(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> List[dict]:
    print("[RyuuGames] Starting multi-category parallel page discovery...")
    categories = ["/", "/category/visualnovel/english-translated/"]
    
    cat_tasks = [discover_ryuu_category_pages(session, semaphore, cat) for cat in categories]
    cat_results = await asyncio.gather(*cat_tasks)
    
    all_game_urls: Set[str] = set().union(*cat_results)
    print(f"[RyuuGames] Indexing completed. Discovered {len(all_game_urls)} unique game pages.")

    # Process discovered game pages in async worker chunks
    game_urls_list = sorted(all_game_urls)
    chunk_size = 50
    parsed_entries: List[dict] = []

    for i in range(0, len(game_urls_list), chunk_size):
        chunk = game_urls_list[i:i + chunk_size]
        tasks = [parse_ryuu_game_page(session, semaphore, url) for url in chunk]
        results = await asyncio.gather(*tasks)
        parsed_entries.extend([r for r in results if r is not None])

    print(f"[RyuuGames] Completed. Extracted {len(parsed_entries)} valid entries.")
    return parsed_entries

# ---------------------------------------------------------------------------
# Main Orchestrator Engine
# ---------------------------------------------------------------------------

async def main():
    existing_data, existing_map = load_existing_source(OUTPUT_FILE)
    initial_count = len(existing_map)

    # TCP Connector with DNS Caching and Keep-Alive settings
    connector = aiohttp.TCPConnector(
        limit=100,               # Max total simultaneous connections
        limit_per_host=20,       # Prevents target IP rate-limiting blocks
        ttl_dns_cache=DNS_CACHE_TTL,
        enable_cleanup_closed=True
    )
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        scrapers = [
            scrape_nyaa_site(session, semaphore, NYAA_BASE, "6_2", "Nyaa PC Games"),
            scrape_nyaa_site(session, semaphore, SUKEBEI_BASE, "1_3", "Sukebei Games"),
            scrape_ryuugames(session, semaphore)
        ]
        
        results = await asyncio.gather(*scrapers, return_exceptions=True)

    # Filter out potential runtime exceptions from task execution
    all_new_entries: List[dict] = []
    for res in results:
        if isinstance(res, list):
            all_new_entries.extend(res)
        elif isinstance(res, Exception):
            print(f"[WARN] Scraper task encountered runtime exception: {res}", file=sys.stderr)

    # Deduplicate and perform non-destructive updates
    added_count = 0
    updated_count = 0

    for entry in all_new_entries:
        key = extract_dedupe_key(entry)
        if not key:
            continue

        if key not in existing_map:
            existing_map[key] = entry
            added_count += 1
        else:
            # Update existing entry URIs non-destructively if new URIs are found
            existing_entry = existing_map[key]
            existing_uris = set(existing_entry.get("uris", []))
            new_uris = [u for u in entry.get("uris", []) if u not in existing_uris]
            if new_uris:
                existing_entry.setdefault("uris", []).extend(new_uris)
                updated_count += 1

    final_downloads = list(existing_map.values())
    output_payload = {
        "name": SOURCE_NAME,
        "downloads": final_downloads
    }

    # Atomic write to temporary file to avoid corruption during crashes
    temp_output_file = f"{OUTPUT_FILE}.tmp"
    with open(temp_output_file, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    os.replace(temp_output_file, OUTPUT_FILE)

    print(f"\n[Completed] Process finished successfully.")
    print(f"[Completed] Added {added_count} new entries.")
    print(f"[Completed] Updated {updated_count} existing entries with new mirror links.")
    print(f"[Completed] Total dataset size in {OUTPUT_FILE}: {len(final_downloads)} (Up from {initial_count})")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Aborted] Process interrupted by user.", file=sys.stderr)
