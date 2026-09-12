#!/usr/bin/env python3
"""
Hydra Launcher full custom source scraper.

Scrapes ALL pages for:
  1. Nyaa.si PC Games (https://nyaa.si/?c=6_2)
  2. Sukebei Art - Games (https://sukebei.nyaa.si/?c=1_3)
  3. RyuuGames Visual Novels

Outputs a Hydra Launcher-compatible source.json file.
"""

import base64
import json
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

# Maximum pages to scrape per run (Increase or remove limit as needed)
# Nyaa/Sukebei usually have ~100-300+ pages of games
MAX_NYAA_PAGES = 50       
MAX_SUKEBEI_PAGES = 50
MAX_RYUU_GAMES = 150      

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
    time.sleep(0.5)  # Delay between requests to avoid rate limits
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

# ---------------------------------------------------------------------------
# Nyaa / Sukebei Web Scraper (All Pages)
# ---------------------------------------------------------------------------

def scrape_nyaa_site(base_url, category_param, source_label, max_pages):
    print(f"[{source_label}] Starting full web scrape...")
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
                # Extract Title & Link
                title_links = row.select("td[colspan='2'] a:not(.comments)")
                if not title_links:
                    continue
                title = title_links[-1].get_text(strip=True)
                
                # Extract Magnet link
                magnet_tag = row.find("a", href=re.compile(r"^magnet:"))
                if not magnet_tag:
                    continue
                magnet_link = magnet_tag["href"]
                
                # Extract Size & Date
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
            except Exception as exc:
                continue

    print(f"[{source_label}] Parsed {len(entries)} total entries.")
    return entries

# ---------------------------------------------------------------------------
# RyuuGames HTML Scraper
# ---------------------------------------------------------------------------

def scrape_ryuugames():
    print("[RyuuGames] Starting full scrape...")
    game_urls = set()

    # Step 1: Collect game page links across categories
    categories = ["/", "/category/visualnovel/english-translated"]
    for cat in categories:
        for page in range(1, 15):
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
    
    # Step 2: Scrape each game page
    entries = []
    for game_url in sorted(game_urls)[:MAX_RYUU_GAMES]:
        entry = scrape_ryuugames_game_page(game_url)
        if entry:
            entries.append(entry)

    print(f"[RyuuGames] Parsed {len(entries)} entries.")
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

    # Size extraction
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
    all_downloads = []

    # 1. Nyaa PC Games
    nyaa_entries = scrape_nyaa_site(NYAA_BASE, "6_2", "Nyaa", MAX_NYAA_PAGES)
    all_downloads.extend(nyaa_entries)

    # 2. Sukebei Games
    sukebei_entries = scrape_nyaa_site(SUKEBEI_BASE, "1_3", "Sukebei", MAX_SUKEBEI_PAGES)
    all_downloads.extend(sukebei_entries)

    # 3. RyuuGames
    ryuu_entries = scrape_ryuugames()
    all_downloads.extend(ryuu_entries)

    source = {
        "name": SOURCE_NAME,
        "downloads": all_downloads,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(source, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\nCompleted! Written {len(all_downloads)} total entries to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
