#!/usr/bin/env python3
"""
Hydra Launcher custom source scraper.

Fetches game torrents from:
  1. Nyaa.si PC Games RSS (https://nyaa.si/?page=rss&c=6_2)
  2. Sukebei Art - Games RSS (https://sukebei.nyaa.si/?page=rss&c=1_3)
  3. RyuuGames main page + individual game pages (direct download links)

Outputs a Hydra Launcher-compatible source.json file.
"""

import base64
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NYAA_RSS_URL = "https://nyaa.si/?page=rss&c=6_2"
SUKEBEI_RSS_URL = "https://sukebei.nyaa.si/?page=rss&c=1_3"
RYUUGAMES_BASE = "https://www.ryuugames.com"
RYUUGAMES_CATEGORIES = [
    "/",  # main page
    "/category/visualnovel/english-translated",
]
RYUU_MAX_GAME_PAGES = 30  # cap to keep total runtime reasonable

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT = 15

SOURCE_NAME = "VN & Nyaa Combined Source"
OUTPUT_FILE = "source.json"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_url(url):
    """Fetch a URL and return the response text, or None on failure."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"  [WARN] Failed to fetch {url}: {exc}", file=sys.stderr)
        return None

def parse_date_to_iso(date_str):
    """Convert an RFC-2822 date string to ISO-8601 (e.g. 2026-09-09T12:20:48Z)."""
    try:
        dt = parsedate_to_datetime(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return date_str.strip()

def normalize_size(size_str):
    """Convert binary-prefixed sizes (1.0 GiB) to decimal notation (1.0 GB)."""
    size_str = size_str.strip()
    size_str = re.sub(r"([KMGT])iB", r"\1B", size_str, flags=re.IGNORECASE)
    return size_str

def build_magnet(info_hash):
    """Build a magnet URI from a BitTorrent info hash."""
    info_hash = info_hash.strip()
    return f"magnet:?xt=urn:btih:{info_hash}"

def extract_info_hash(description):
    """
    Extract the info hash from a Nyaa/Sukebei RSS <description> field.

    The description looks like:
      #2158567 | Title | 1.0 GiB | Software - Games | 6BDE41A0...
    """
    parts = description.split("|")
    if len(parts) >= 5:
        raw_hash = parts[-1].strip()
        raw_hash = raw_hash.replace("]]>", "").strip()
        if re.match(r"^[0-9A-Fa-f]{40}$", raw_hash):
            return raw_hash
    return None

def extract_size_from_description(description):
    """Extract the file size from a Nyaa/Sukebei RSS <description> field."""
    parts = description.split("|")
    if len(parts) >= 4:
        return normalize_size(parts[2])
    return "Unknown"

# ---------------------------------------------------------------------------
# Nyaa / Sukebei RSS parser
# ---------------------------------------------------------------------------

def parse_rss_feed(url, source_label):
    """Parse a Nyaa or Sukebei RSS feed and return a list of download entries."""
    print(f"[{source_label}] Fetching RSS: {url}")
    xml_text = fetch_url(url)
    if not xml_text:
        print(f"[{source_label}] Failed to fetch feed. Skipping.", file=sys.stderr)
        return []

    entries = []
    try:
        root = ET.fromstring(xml_text)
        items = root.findall(".//item")
        if not items:
            print(f"[{source_label}] No <item> elements found in feed.")
            return []

        for item in items:
            try:
                title = item.findtext("title", default="Unknown")
                pub_date = item.findtext("pubDate", default="")
                description = item.findtext("description", default="")
                link = item.findtext("link", default="")

                iso_date = parse_date_to_iso(pub_date) if pub_date else ""
                file_size = extract_size_from_description(description)
                info_hash = extract_info_hash(description)

                uris = []
                if info_hash:
                    uris.append(build_magnet(info_hash))
                if link and link.startswith("http"):
                    uris.append(link)

                if not uris:
                    continue

                entries.append({
                    "title": title.strip(),
                    "fileSize": file_size,
                    "uploadDate": iso_date,
                    "uris": uris,
                })
            except Exception as exc:
                print(f"  [WARN] Failed to parse item: {exc}", file=sys.stderr)
                continue

    except ET.ParseError as exc:
        print(f"[{source_label}] XML parse error: {exc}", file=sys.stderr)
        return []
    except Exception as exc:
        print(f"[{source_label}] Unexpected error: {exc}", file=sys.stderr)
        return []

    print(f"[{source_label}] Parsed {len(entries)} entries.")
    return entries

# ---------------------------------------------------------------------------
# RyuuGames HTML scraper
# ---------------------------------------------------------------------------

def scrape_ryuugames():
    """Scrape RyuuGames for visual novel titles and direct download links."""
    print("[RyuuGames] Starting scrape...")
    game_urls = set()

    for category_path in RYUUGAMES_CATEGORIES:
        url = RYUUGAMES_BASE + category_path
        print(f"[RyuuGames] Fetching listing page: {url}")
        html = fetch_url(url)
        if not html:
            continue
        try:
            soup = BeautifulSoup(html, "html.parser")
            for heading in soup.find_all("h3"):
                link = heading.find("a")
                if link and link.get("href"):
                    href = link["href"]
                    if "ryuugames.com" in href and href not in game_urls:
                        game_urls.add(href)
        except Exception as exc:
            print(f"  [WARN] Failed to parse listing {url}: {exc}", file=sys.stderr)
            continue

    for category_path in RYUUGAMES_CATEGORIES:
        for page_num in range(2, 6):
            base = category_path.rstrip("/")
            url = f"{RYUUGAMES_BASE}{base}/page/{page_num}/"
            html = fetch_url(url)
            if not html:
                break
            try:
                soup = BeautifulSoup(html, "html.parser")
                found_any = False
                for heading in soup.find_all("h3"):
                    link = heading.find("a")
                    if link and link.get("href"):
                        href = link["href"]
                        if "ryuugames.com" in href and href not in game_urls:
                            game_urls.add(href)
                            found_any = True
                if not found_any:
                    break
            except Exception:
                break

    print(f"[RyuuGames] Found {len(game_urls)} game pages to scrape.")

    game_url_list = sorted(game_urls)[:RYUU_MAX_GAME_PAGES]
    if len(game_urls) > RYUU_MAX_GAME_PAGES:
        print(f"[RyuuGames] Capping to first {RYUU_MAX_GAME_PAGES} pages.")

    entries = []
    for game_url in game_url_list:
        try:
            entry = scrape_ryuugames_game_page(game_url)
            if entry:
                entries.append(entry)
        except Exception as exc:
            print(f"  [WARN] Failed to scrape game page {game_url}: {exc}", file=sys.stderr)
            continue

    print(f"[RyuuGames] Parsed {len(entries)} entries.")
    return entries

def fetch_ryuugames_download_url(game_page_url, link_key, post_id, shortcode_id):
    """
    POST to the RyuuGames /processing/ endpoint to resolve a download button
    into its actual file-host URL.

    The processing page contains a hidden form with a base64-encoded JSON blob
    holding the real download URL.
    """
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
    except Exception:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    form = soup.find("form", id="continueForm")
    if not form:
        return None
    host_input = form.find("input", attrs={"name": "host"})
    if not host_input or not host_input.get("value"):
        return None
    try:
        decoded = json.loads(base64.b64decode(host_input["value"]).decode("utf-8"))
        return decoded.get("url")
    except Exception:
        return None

def scrape_ryuugames_game_page(url):
    """Scrape a single RyuuGames game page for title, size, date, and links."""
    html = fetch_url(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    title = None
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            title = re.sub(r"\s*-\s*Ryuugames\s*$", "", title, flags=re.IGNORECASE)
            title = re.sub(r"\s*\|\s*Ryuugames\s*$", "", title, flags=re.IGNORECASE)
    if not title:
        return None

    upload_date = ""
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        raw_date = time_tag["datetime"]
        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            upload_date = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            upload_date = raw_date
    else:
        date_match = re.search(
            r"(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2}:\d{2})?",
            str(soup),
        )
        if date_match:
            d = date_match.group(1)
            t = date_match.group(2) or "00:00:00"
            upload_date = f"{d}T{t}Z"

    file_size = "Unknown"
    page_text = soup.get_text()
    size_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|TiB|GB|MB|KB|TB)",
        page_text,
        flags=re.IGNORECASE,
    )
    if size_match:
        file_size = normalize_size(f"{size_match.group(1)} {size_match.group(2)}")

    uris = []
    buttons = soup.find_all("button", attrs={"data-link-key": True})
    seen_server_groups = set()
    for btn in buttons:
        link_key = btn.get("data-link-key")
        post_id = btn.get("data-post-id")
        shortcode_id = btn.get("data-shortcode-id")
        if not (link_key and post_id and shortcode_id):
            continue
        group_prefix = link_key.split("_")[0]
        if group_prefix in seen_server_groups:
            continue
        seen_server_groups.add(group_prefix)
        download_url = fetch_ryuugames_download_url(
            url, link_key, post_id, shortcode_id
        )
        if download_url and download_url not in uris:
            uris.append(download_url)

    if not uris:
        return None

    return {
        "title": title,
        "fileSize": file_size,
        "uploadDate": upload_date,
        "uris": uris,
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    all_downloads = []
    errors = []

    # 1. Nyaa.si PC Games RSS
    try:
        nyaa_entries = parse_rss_feed(NYAA_RSS_URL, "Nyaa")
        all_downloads.extend(nyaa_entries)
    except Exception as exc:
        msg = f"Nyaa parser failed: {exc}"
        errors.append(msg)
        print(f"[ERROR] {msg}", file=sys.stderr)

    # 2. Sukebei Art - Games RSS
    try:
        sukebei_entries = parse_rss_feed(SUKEBEI_RSS_URL, "Sukebei")
        all_downloads.extend(sukebei_entries)
    except Exception as exc:
        msg = f"Sukebei parser failed: {exc}"
        errors.append(msg)
        print(f"[ERROR] {msg}", file=sys.stderr)

    # 3. RyuuGames HTML scraper
    try:
        ryuu_entries = scrape_ryuugames()
        all_downloads.extend(ryuu_entries)
    except Exception as exc:
        msg = f"RyuuGames parser failed: {exc}"
        errors.append(msg)
        print(f"[ERROR] {msg}", file=sys.stderr)

    source = {
        "name": SOURCE_NAME,
        "downloads": all_downloads,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(source, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\n=== Summary ===")
    print(f"Total entries: {len(all_downloads)}")
    counts = {"nyaa": 0, "sukebei": 0, "ryuu": 0}
    for name, val in [("nyaa", "nyaa_entries"), ("sukebei", "sukebei_entries"), ("ryuu", "ryuu_entries")]:
        if val in locals():
            counts[name] = len(locals()[val])
    print(f"  Nyaa:    {counts['nyaa']}")
    print(f"  Sukebei: {counts['sukebei']}")
    print(f"  Ryuu:    {counts['ryuu']}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    print(f"Output written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
