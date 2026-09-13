#!/usr/bin/env python3
"""
Hydra Launcher full custom source scraper (Incremental Updates).

Scrapes pages for:
  1. Nyaa.si PC Games
  2. Sukebei Art - Games
  3. RyuuGames Visual Novels

Outputs/Updates a Hydra Launcher-compatible source.json file without overwriting old entries.
"""

import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NYAA_BASE = "https://nyaa.si"
SUKEBEI_BASE = "https://sukebei.nyaa.si"
RYUUGAMES_BASE = "https://www.ryuugames.com"

# Maximum pages to scrape per run
MAX_NYAA_PAGES = 500        
MAX_SUKEBEI_PAGES = 500
MAX_RYUU_PAGES = 1000      

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 15

SOURCE_NAME = "VN & Nyaa Full Source"
OUTPUT_FILE = "source.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_url(url):
    """Fetch a URL with timeout and delay to respect server limits."""
    time.sleep(0.5)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"  [WARN] Failed to fetch {url}: {exc}", file=sys.stderr)
        return None

def normalize_size(size_str):
    size_str = size_str.strip()
    return re.sub(r"([KMGT])iB", r"\1B", size_str, flags=re.IGNORECASE)

def load_existing_source(filepath):
    """Loads existing JSON file and returns a map of {primary_uri: entry} for fast deduplication."""
    if not os.path.exists(filepath):
        return {}
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            existing_downloads = data.get("downloads", [])
            
            # Map each item by its first URI for unique tracking
            existing_map = {}
            for item in existing_downloads:
                uris = item.get("uris", [])
                if uris:
                    existing_map[uris[0]] = item
                elif item.get("title"):
                    existing_map[item["title"]] = item
            
            print(f"[Storage] Loaded {len(existing_map)} existing entries from {filepath}.")
            return existing_map
    except Exception as exc:
        print(f"[WARN] Could not parse existing {filepath}: {exc}. Starting fresh.", file=sys.stderr)
        return {}

# ---------------------------------------------------------------------------
# Nyaa / Sukebei Web Scraper
# ---------------------------------------------------------------------------

def scrape_nyaa_site(base_url, category_param, source_label, max_pages):
    print(f"[{source_label}] Starting web scrape...")
    entries = []
    
    for page in range(1, max_pages + 1):
        url = f"{base_url}/?c={category_param}&p={page}"
        print(f"[{source_label}] Fetching page {page}: {url}")
        html = fetch_url(url)
        if not html:
            break
            
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table.torrent-list tbody tr")
        
        if not rows:
            print(f"[{source_label}] No more results found at page {page}.")
            break
            
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

    print(f"[{source_label}] Parsed {len(entries)} total entries in this run.")
    return entries

# ---------------------------------------------------------------------------
# RyuuGames HTML Scraper
# ---------------------------------------------------------------------------

def scrape_ryuugames(max_pages):
    print("[RyuuGames] Starting scrape...")
    game_urls = set()

    categories = ["/", "/category/visualnovel/english-translated"]
    for cat in categories:
        for page in range(1, max_pages + 1):
            url = f"{RYUUGAMES_BASE}{cat.rstrip('/')}/page/{page}/" if page > 1 else f"{RYUUGAMES_BASE}{cat}"
            html = fetch_url(url)
            if not html:
                break
            soup = BeautifulSoup(html, "html.parser")
            found = 0
            for heading in soup.find_all("h3"):
                link = heading.find("a")
                if link and link.get("href") and "ryuugames.com" in link["href"]:
                    game_urls.add(link["href"])
                    found += 1
            if found == 0:
                break

    print(f"[RyuuGames] Found {len(game_urls)} total game pages.")
    
    entries = []
    for game_url in sorted(game_urls):
        entry = scrape_ryuugames_game_page(game_url)
        if entry:
            entries.append(entry)

    print(f"[RyuuGames] Parsed {len(entries)} entries in this run.")
    return entries

def fetch_ryuugames_download_url(game_page_url, link_key, post_id, shortcode_id):
    proc_url = RYUUGAMES_BASE + "/processing/"
    form_data = {
        "ryuu_sl_action": "process",
        "post_id": post_id,
        "shortcode_id": shortcode_id,
        "link_key": link_key,
    }
    try:
        resp = requests.post(
            proc_url,
            headers={**HEADERS, "Referer": game_page_url},
            data=form_data,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
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

def scrape_ryuugames_game_page(url):
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1").get_text(strip=True) if soup.find("h1") else None
    if not title:
        return None

    file_size = "Unknown"
    size_match = re.search(r"(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|TiB|GB|MB|KB|TB)", soup.get_text(), re.I)
    if size_match:
        file_size = normalize_size(f"{size_match.group(1)} {size_match.group(2)}")

    uris = []
    buttons = soup.find_all("button", attrs={"data-link-key": True})
    seen_groups = set()
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
        dl_url = fetch_ryuugames_download_url(url, link_key, post_id, shortcode_id)
        if dl_url and dl_url not in uris:
            uris.append(dl_url)

    if not uris:
        return None

    return {
        "title": title,
        "fileSize": file_size,
        "uploadDate": "",
        "uris": uris,
    }

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

def main():
    # Load existing items to avoid duplicates
    existing_entries_map = load_existing_source(OUTPUT_FILE)
    initial_count = len(existing_entries_map)

    # Scrape fresh data
    new_scrapes = []
    new_scrapes.extend(scrape_nyaa_site(NYAA_BASE, "6_2", "Nyaa", MAX_NYAA_PAGES))
    new_scrapes.extend(scrape_nyaa_site(SUKEBEI_BASE, "1_3", "Sukebei", MAX_SUKEBEI_PAGES))
    new_scrapes.extend(scrape_ryuugames(MAX_RYUU_PAGES))

    # Merge new entries into existing entries map
    added_count = 0
    for entry in new_scrapes:
        uris = entry.get("uris", [])
        key = uris[0] if uris else entry.get("title")
        
        if key and key not in existing_entries_map:
            existing_entries_map[key] = entry
            added_count += 1

    final_downloads = list(existing_entries_map.values())

    source = {
        "name": SOURCE_NAME,
        "downloads": final_downloads,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(source, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n[Completed] Added {added_count} new unique entries.")
    print(f"[Completed] Total entries in {OUTPUT_FILE}: {len(final_downloads)} (Up from {initial_count})")

if __name__ == "__main__":
    main()
