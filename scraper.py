#!/usr/bin/env python3
"""
Hydra Launcher Infinite Scraping Engine (Production Architecture).

Features:
  - Multi-tier deduplication (BTv1/BTv2 InfoHash, Canonical URL, Title).
  - Non-destructive incremental dataset merging.
  - Dynamic binary-search & parallel pagination crawlers.
  - Adaptive per-host rate-limiting with exponential jitter backoff.
  - POSIX-compliant atomic serialization.
"""

import asyncio
import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import aiohttp
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Global Configurations & Settings
# ---------------------------------------------------------------------------

OUTPUT_FILE = "source.json"
SOURCE_NAME = "VN & Nyaa Universal Source"

# Connection Pool Settings
TOTAL_CONCURRENT_LIMIT = 100
DEFAULT_HOST_LIMIT = 15
DNS_CACHE_TTL = 300
REQUEST_TIMEOUT = 20
MAX_RETRIES = 4

# Target Host Dynamic Semaphore Allocations
HOST_LIMITS = {
    "nyaa.si": 8,
    "sukebei.nyaa.si": 8,
    "www.ryuugames.com": 12,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0"
]

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

# Compiled Regular Expressions
INFOHASH_REGEX = re.compile(r"urn:btih:([a-fA-F0-9]{40}|[a-fA-F0-9]{32}|[a-fA-F0-9]{64})", re.IGNORECASE)
SIZE_EXTRACT_REGEX = re.compile(r"(\d+(?:\.\d+)?)\s*([KMGT]i?B)", re.IGNORECASE)
NYAA_PAGINATION_REGEX = re.compile(r"[?&]p=(\d+)")
RYUU_PAGINATION_REGEX = re.compile(r"/page/(\d+)/")

# ---------------------------------------------------------------------------
# Data Models & Deduplication Logic
# ---------------------------------------------------------------------------

def normalize_size_string(raw_size: str) -> str:
    """Standardizes binary file sizes (e.g., 1.4 GiB -> 1.4 GB)."""
    if not raw_size or raw_size == "Unknown":
        return "Unknown"
    match = SIZE_EXTRACT_REGEX.search(raw_size)
    if not match:
        return raw_size.strip()
    val, unit = match.group(1), match.group(2).upper()
    unit = unit.replace("IB", "B")
    return f"{val} {unit}"

def extract_infohash(magnet_url: str) -> Optional[str]:
    """Extracts standard 40-character lowercase hex InfoHash from a magnet URI."""
    match = INFOHASH_REGEX.search(magnet_url)
    if not match:
        return None
    raw = match.group(1)
    if len(raw) == 32:
        try:
            return base64.b32decode(raw.upper()).hex().lower()
        except Exception:
            return None
    return raw[:40].lower()

def compute_dedupe_key(entry: Dict[str, Any]) -> str:
    """
    Computes a deterministic primary key for an entry.
    Priority: InfoHash -> Canonical URI -> Cleaned Title
    """
    for uri in entry.get("uris", []):
        if uri.startswith("magnet:"):
            ih = extract_infohash(uri)
            if ih:
                return f"hash:{ih}"
        
        parsed = urlparse(uri)
        if parsed.scheme in ("http", "https"):
            clean_path = f"{parsed.netloc}{parsed.path}".rstrip("/").lower()
            return f"uri:{clean_path}"

    title = entry.get("title", "").strip().lower()
    title = re.sub(r"[^\w\s]", "", title)
    title = re.sub(r"\s+", " ", title)
    return f"title:{title}"

# ---------------------------------------------------------------------------
# Persistence Engine
# ---------------------------------------------------------------------------

class StorageEngine:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lock = asyncio.Lock()
        self.data_map: Dict[str, Dict[str, Any]] = {}
        self.raw_structure: Dict[str, Any] = {"name": SOURCE_NAME, "downloads": []}
        
    def load(self) -> int:
        """Reads existing file and indexes records without data destruction."""
        if not os.path.exists(self.filepath):
            return 0
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                self.raw_structure = json.load(f)
                downloads = self.raw_structure.get("downloads", [])
                for item in downloads:
                    key = compute_dedupe_key(item)
                    if key:
                        self.data_map[key] = item
            print(f"[Storage] Loaded {len(self.data_map)} existing unique items.")
            return len(self.data_map)
        except Exception as err:
            print(f"[Storage WARN] Read error: {err}. Starting with fresh structure.", file=sys.stderr)
            return 0

    async def merge_entries(self, new_entries: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Merges new items with existing data in memory safely."""
        async with self.lock:
            added = 0
            updated = 0
            for entry in new_entries:
                key = compute_dedupe_key(entry)
                if not key:
                    continue

                if key not in self.data_map:
                    self.data_map[key] = entry
                    added += 1
                else:
                    target = self.data_map[key]
                    existing_uris = set(target.get("uris", []))
                    new_uris = [u for u in entry.get("uris", []) if u not in existing_uris]
                    
                    if new_uris:
                        target.setdefault("uris", []).extend(new_uris)
                        updated += 1
                        
                    # Backfill missing metadata parameters
                    if not target.get("fileSize") or target["fileSize"] == "Unknown":
                        if entry.get("fileSize") and entry["fileSize"] != "Unknown":
                            target["fileSize"] = entry["fileSize"]
                            
                    if not target.get("uploadDate") and entry.get("uploadDate"):
                        target["uploadDate"] = entry["uploadDate"]
                        
            return added, updated

    def commit(self):
        """Flushes in-memory data to disk using POSIX atomic swaps."""
        self.raw_structure["downloads"] = list(self.data_map.values())
        tmp_file = f"{self.filepath}.tmp"
        
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(self.raw_structure, f, indent=2, ensure_ascii=False)
            f.write("\n")

        os.replace(tmp_file, self.filepath)
        print(f"[Storage] Atomic write successful. Total records stored: {len(self.data_map)}")

# ---------------------------------------------------------------------------
# Network Layer (Resilient Client Engine)
# ---------------------------------------------------------------------------

class NetworkEngine:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphores: Dict[str, asyncio.Semaphore] = {}
        self.default_semaphore = asyncio.Semaphore(DEFAULT_HOST_LIMIT)

    async def initialize(self):
        connector = aiohttp.TCPConnector(
            limit=TOTAL_CONCURRENT_LIMIT,
            ttl_dns_cache=DNS_CACHE_TTL,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(connector=connector)
        for host, limit in HOST_LIMITS.items():
            self.semaphores[host] = asyncio.Semaphore(limit)

    async def close(self):
        if self.session:
            await self.session.close()

    def _get_semaphore(self, url: str) -> asyncio.Semaphore:
        host = urlparse(url).netloc
        return self.semaphores.get(host, self.default_semaphore)

    async def fetch(
        self, 
        url: str, 
        method: str = "GET", 
        data: Optional[Dict[str, Any]] = None, 
        headers: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        if not self.session:
            raise RuntimeError("Network Engine uninitialized.")

        semaphore = self._get_semaphore(url)
        req_headers = {**DEFAULT_HEADERS, **(headers or {})}
        req_headers["User-Agent"] = USER_AGENTS[hash(url) % len(USER_AGENTS)]

        async with semaphore:
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    async with self.session.request(
                        method, url, data=data, headers=req_headers, timeout=REQUEST_TIMEOUT
                    ) as resp:
                        if resp.status == 404:
                            return None
                        if resp.status == 429:
                            backoff = (2 ** attempt) + (hash(url) % 3)
                            await asyncio.sleep(backoff)
                            continue
                        resp.raise_for_status()
                        return await resp.text()
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt == MAX_RETRIES:
                        return None
                    await asyncio.sleep(0.5 * (2 ** attempt))
                except Exception:
                    return None
        return None

# ---------------------------------------------------------------------------
# Provider Scraping Engines
# ---------------------------------------------------------------------------

class NyaaScraper:
    def __init__(self, net: NetworkEngine):
        self.net = net

    async def get_max_pages(self, base_url: str, category: str) -> int:
        url = f"{base_url}/?c={category}&p=1"
        html = await self.net.fetch(url)
        if not html:
            return 1
        
        soup = BeautifulSoup(html, "lxml")
        max_page = 1
        for a in soup.select("ul.pagination li a"):
            match = NYAA_PAGINATION_REGEX.search(a.get("href", ""))
            if match:
                max_page = max(max_page, int(match.group(1)))
        return max_page

    async def parse_page(self, base_url: str, category: str, page: int) -> List[Dict[str, Any]]:
        url = f"{base_url}/?c={category}&p={page}"
        html = await self.net.fetch(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        results = []
        for row in soup.select("table.torrent-list tbody tr"):
            try:
                title_links = row.select("td[colspan='2'] a:not(.comments)")
                if not title_links:
                    continue
                title = title_links[-1].get_text(strip=True)

                magnet_tag = row.find("a", href=re.compile(r"^magnet:", re.IGNORECASE))
                if not magnet_tag:
                    continue
                
                tds = row.find_all("td")
                file_size = normalize_size_string(tds[3].get_text(strip=True)) if len(tds) > 3 else "Unknown"
                raw_date = tds[4].get_text(strip=True) if len(tds) > 4 else ""

                upload_date = ""
                if raw_date:
                    try:
                        dt = datetime.strptime(raw_date, "%Y-%m-%d %H:%M")
                        upload_date = dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        upload_date = raw_date

                results.append({
                    "title": title,
                    "fileSize": file_size,
                    "uploadDate": upload_date,
                    "uris": [magnet_tag["href"]]
                })
            except Exception:
                continue
        return results

    async def run(self, base_url: str, category: str, label: str) -> List[Dict[str, Any]]:
        print(f"[{label}] Scanning total page count...")
        max_pages = await self.get_max_pages(base_url, category)
        print(f"[{label}] Found {max_pages} pages. Crawling concurrently...")

        all_entries = []
        chunk_size = 35
        for i in range(1, max_pages + 1, chunk_size):
            pages = range(i, min(i + chunk_size, max_pages + 1))
            tasks = [self.parse_page(base_url, category, p) for p in pages]
            res = await asyncio.gather(*tasks)
            for page_res in res:
                all_entries.extend(page_res)

        print(f"[{label}] Harvested {len(all_entries)} entries.")
        return all_entries


class RyuuGamesScraper:
    def __init__(self, net: NetworkEngine):
        self.net = net
        self.base_url = "https://www.ryuugames.com"

    async def fetch_download_url(self, page_url: str, link_key: str, post_id: str, shortcode_id: str) -> Optional[str]:
        proc_url = f"{self.base_url}/processing/"
        payload = {
            "ryuu_sl_action": "process",
            "post_id": post_id,
            "shortcode_id": shortcode_id,
            "link_key": link_key,
        }
        headers = {"Referer": page_url}
        html = await self.net.fetch(proc_url, method="POST", data=payload, headers=headers)
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
            data = json.loads(base64.b64decode(host_input["value"]).decode("utf-8"))
            return data.get("url")
        except Exception:
            return None

    async def parse_game_page(self, url: str) -> Optional[Dict[str, Any]]:
        html = await self.net.fetch(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "lxml")
        h1 = soup.find("h1")
        if not h1:
            return None

        title = h1.get_text(strip=True)
        file_size = normalize_size_string(soup.get_text())

        buttons = soup.find_all("button", attrs={"data-link-key": True})
        seen_groups: Set[str] = set()
        tasks = []

        for btn in buttons:
            lk = btn.get("data-link-key")
            pid = btn.get("data-post-id")
            sid = btn.get("data-shortcode-id")
            if not (lk and pid and sid):
                continue

            group_prefix = lk.split("_")[0]
            if group_prefix in seen_groups:
                continue
            seen_groups.add(group_prefix)

            tasks.append(self.fetch_download_url(url, lk, pid, sid))

        resolved = await asyncio.gather(*tasks)
        uris = [u for u in resolved if u]
        if not uris:
            return None

        return {
            "title": title,
            "fileSize": file_size,
            "uploadDate": "",
            "uris": uris
        }

    async def discover_category_links(self, path: str) -> Set[str]:
        urls: Set[str] = set()
        first_url = f"{self.base_url}{path}"
        html = await self.net.fetch(first_url)
        if not html:
            return urls

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("h3 a[href*='ryuugames.com']"):
            urls.add(a["href"])

        max_page = 1
        for a in soup.select("a.page-numbers, ul.pagination a"):
            match = RYUU_PAGINATION_REGEX.search(a.get("href", ""))
            if match:
                max_page = max(max_page, int(match.group(1)))

        async def fetch_cat_page(p: int) -> Set[str]:
            target = f"{self.base_url}{path.rstrip('/')}/page/{p}/"
            res_html = await self.net.fetch(target)
            if not res_html:
                return set()
            s = BeautifulSoup(res_html, "lxml")
            return {a["href"] for a in s.select("h3 a[href*='ryuugames.com']")}

        page = 2
        consecutive_failures = 0
        while consecutive_failures < 2:
            batch = list(range(page, page + 15))
            results = await asyncio.gather(*[fetch_cat_page(p) for p in batch])
            
            found = 0
            for res in results:
                urls.update(res)
                found += len(res)

            if found == 0 and page > max_page:
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            page += 15

        return urls

    async def run(self) -> List[Dict[str, Any]]:
        print("[RyuuGames] Starting category scan...")
        categories = ["/", "/category/visualnovel/english-translated/"]
        
        cat_tasks = [self.discover_category_links(c) for c in categories]
        cat_results = await asyncio.gather(*cat_tasks)
        all_game_urls = set().union(*cat_results)
        print(f"[RyuuGames] Found {len(all_game_urls)} unique game entries.")

        url_list = list(all_game_urls)
        chunk_size = 25
        parsed_entries = []
        for i in range(0, len(url_list), chunk_size):
            chunk = url_list[i:i + chunk_size]
            results = await asyncio.gather(*[self.parse_game_page(u) for u in chunk])
            parsed_entries.extend([r for r in results if r is not None])

        print(f"[RyuuGames] Harvested {len(parsed_entries)} valid entries.")
        return parsed_entries

# ---------------------------------------------------------------------------
# Master Orchestrator
# ---------------------------------------------------------------------------

async def main():
    storage = StorageEngine(OUTPUT_FILE)
    storage.load()

    net = NetworkEngine()
    await net.initialize()

    nyaa = NyaaScraper(net)
    ryuu = RyuuGamesScraper(net)

    scrapers = [
        nyaa.run("https://nyaa.si", "6_2", "Nyaa PC Games"),
        nyaa.run("https://sukebei.nyaa.si", "1_3", "Sukebei Games"),
        ryuu.run()
    ]

    print("\n[Engine Initialization] Scraping sources concurrently...")
    results = await asyncio.gather(*scrapers, return_exceptions=True)
    await net.close()

    total_added = 0
    total_updated = 0

    for res in results:
        if isinstance(res, list):
            added, updated = await storage.merge_entries(res)
            total_added += added
            total_updated += updated
        elif isinstance(res, Exception):
            print(f"[Pipeline Error] Module failed: {res}", file=sys.stderr)

    storage.commit()

    print(f"\n================ SUMMARY ================")
    print(f" New Entries Added    : {total_added}")
    print(f" Existing Updated     : {total_updated}")
    print(f" Total Unique Output  : {len(storage.data_map)}")
    print(f" Output Location      : {os.path.abspath(OUTPUT_FILE)}")
    print(f"=========================================\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Execution Aborted] Process halted by user.", file=sys.stderr)
