#!/usr/bin/env python3
"""
scrape_drn.py
- Find talk-page links on Wikipedia dispute pages (DRN / RfM / RfA)
- Download talk-page wikitext via MediaWiki API
- Save JSONL records to `data/` as talkpage_<safe_title>.jsonl
Run:
    python scrape_drn.py --page "Wikipedia:Dispute resolution noticeboard" --outdir data --limit 200
"""

import requests
from bs4 import BeautifulSoup
import time
import json
import argparse
import os
import re
from urllib.parse import urljoin, unquote

# ====== Config ======
USER_AGENT = "TaymProjectBot/0.1 (Taym.mehdi@stud.uni-hannover.de) Python/requests - research scraping"
SLEEP_SECONDS = 1.0   # polite delay between requests
WIKI_BASE = "https://en.wikipedia.org"

API_ENDPOINT = "https://en.wikipedia.org/w/api.php"

# ====== Helpers ======
def safe_filename(s: str) -> str:
    # produce a filename-safe string
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:240]

def get_html(url: str, session: requests.Session, retries=3):
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"[WARN] get_html error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2 * (attempt+1))
    raise RuntimeError(f"Failed to fetch {url}")

def extract_talk_links_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/wiki/Talk:"):
            full = urljoin(WIKI_BASE, href)
            links.add(full)
        # sometimes links are encoded or include fragments: keep them too
        # optionally, include '/wiki/Some_page#Talk_section' - but main pattern is Talk:
    return sorted(links)

def fetch_wikitext_via_api(page_title: str, session: requests.Session):
    """
    Use action=query&prop=revisions&rvprop=content to get page wikitext
    page_title must be like 'Talk:Example'
    """
    headers = {"User-Agent": USER_AGENT}
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "rvprop": "content|timestamp",
        "rvslots": "main",
        "titles": page_title,
        "formatversion": 2
    }
    r = session.get(API_ENDPOINT, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    pages = data.get("query", {}).get("pages", [])
    if not pages:
        return None
    page = pages[0]
    if "missing" in page:
        return None
    revs = page.get("revisions")
    if not revs:
        return None
    rev = revs[0]
    return {"wikitext": rev.get("slots", {}).get("main", {}).get("content", ""), "timestamp": rev.get("timestamp")}

# ====== Main ======
def main(page_name: str, outdir: str, limit: int):
    os.makedirs(outdir, exist_ok=True)
    session = requests.Session()
    # 1) get the dispute page html
    print(f"[INFO] Fetching dispute page: {page_name}")
    # build URL from page name:
    page_url = WIKI_BASE + "/wiki/" + page_name.replace(" ", "_")
    html = get_html(page_url, session)
    print("[INFO] Extracting talk links from page HTML...")
    talk_links = extract_talk_links_from_html(html)
    print(f"[INFO] Found {len(talk_links)} / limiting to {limit}")
    talk_links = talk_links[:limit]

    out_file = os.path.join(outdir, f"talk_links_{safe_filename(page_name)}.jsonl")
    # Save the list of links
    with open(out_file, "w", encoding="utf8") as f:
        for url in talk_links:
            f.write(json.dumps({"url": url}) + "\n")
    print(f"[INFO] Saved {len(talk_links)} talk links to {out_file}")

    # 2) fetch each talk page via API and save records
    records_file = os.path.join(outdir, f"talk_pages_{safe_filename(page_name)}.jsonl")
    count = 0
    with open(records_file, "w", encoding="utf8") as outf:
        for url in talk_links:
            # Extract the page title from URL: /wiki/Talk:Page_Title
            path = url.replace(WIKI_BASE, "")
            # remove fragment
            if "#" in path:
                path = path.split("#")[0]
            title = unquote(path.replace("/wiki/", ""))
            print(f"[INFO] Fetching wikitext for: {title}")
            try:
                result = fetch_wikitext_via_api(title, session)
                if result is None:
                    print(f"[WARN] No wikitext for {title}")
                    time.sleep(SLEEP_SECONDS)
                    continue
                record = {
                    "title": title,
                    "url": url,
                    "wikitext": result.get("wikitext"),
                    "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    "revision_timestamp": result.get("timestamp")
                }
                outf.write(json.dumps(record) + "\n")
                count += 1
            except Exception as e:
                print(f"[ERROR] Failed to fetch {title}: {e}")
            time.sleep(SLEEP_SECONDS)
    print(f"[INFO] Saved {count} talk pages to {records_file}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--page", type=str, default="Wikipedia:Dispute resolution noticeboard",
                   help="Name of the wiki page (use exact capitalization and spaces), e.g. 'Wikipedia:Dispute resolution noticeboard'")
    p.add_argument("--outdir", type=str, default="data", help="Output directory")
    p.add_argument("--limit", type=int, default=200, help="Max number of links to fetch")
    args = p.parse_args()
    main(args.page, args.outdir, args.limit)
