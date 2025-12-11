#!/usr/bin/env python3
"""
fetch_talkpages.py 
Reads drn_links.jsonl produced by scrape_drn.py.
For each Talk: link:
    - Extract page title and anchor
    - Fetch full wikitext via MediaWiki API
    - Write one JSONL record per input link:
          (title, anchor, url, wikitext)


Run:
    python fetch_talkpages.py --input data/drn_links.jsonl --outdir data
"""

import json
import time
import argparse
import os
import requests
from urllib.parse import unquote
import re

USER_AGENT = "TaymProjectBot/0.1 (Taym.mehdi@stud.uni-hannover.de)"
API_ENDPOINT = "https://en.wikipedia.org/w/api.php"
SLEEP_SECONDS = 1.0



def fetch_wikitext_via_api(title: str, session: requests.Session, retries=3):
    headers = {"User-Agent": USER_AGENT}
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "rvprop": "content|timestamp",
        "rvslots": "main",
        "titles": title,
        "formatversion": 2
    }

    for attempt in range(retries):
        try:
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

            return {
                "wikitext": rev.get("slots", {}).get("main", {}).get("content", ""),
                "timestamp": rev.get("timestamp")
            }

        except Exception as e:
            print(f"[WARN] API error for '{title}' (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))

    print(f"[ERROR] Failed after retries: {title}")
    return None


def split_title_and_anchor(url: str):
    """
    url example:
        https://en.wikipedia.org/wiki/Talk:Page_Title#Section_Name

    Returns:
        title = "Talk:Page_Title"
        anchor = "Section_Name" or None
    """
    path = url.replace("https://en.wikipedia.org/wiki/", "")

    if "#" in path:
        page, anchor = path.split("#", 1)
        return unquote(page), unquote(anchor)
    else:
        return unquote(path), None


def main(input_file: str, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    session = requests.Session()

    out_path = os.path.join(outdir, "talkpages.jsonl")
    fout = open(out_path, "w", encoding="utf8")

    # To avoid fetching same talk page multiple times
    cached_pages = {}

    print("[INFO] Reading DRN links...")
    with open(input_file, "r", encoding="utf8") as fin:
        for line in fin:
            rec = json.loads(line)
            url = rec["url"]

            title, anchor = split_title_and_anchor(url)

            # Fetch full page only once
            if title not in cached_pages:
                print(f"[INFO] Fetching wikitext for: {title}")
                data = fetch_wikitext_via_api(title, session)
                if data is None:
                    print(f"[WARN] No wikitext for {title}")
                    continue
                cached_pages[title] = data
                time.sleep(SLEEP_SECONDS)

            # Write one record per link (even if wikitext was cached)
            output = {
                "title": title,
                "anchor": anchor,
                "url": url,
                "wikitext": cached_pages[title]["wikitext"],
                "revision_timestamp": cached_pages[title]["timestamp"],
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }

            fout.write(json.dumps(output) + "\n")

    fout.close()
    print(f"[DONE] Saved talk pages → {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="data/drn_links.jsonl",
                        help="Input JSONL from scrape_drn.py")
    parser.add_argument("--outdir", type=str, default="data",
                        help="Output directory")
    args = parser.parse_args()

    main(args.input, args.outdir)
