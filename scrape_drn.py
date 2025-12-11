#!/usr/bin/env python3
"""
scrape_drn.py

This script:
- Scrapes the main DRN page
- Scrapes all DRN archive pages
- Extracts ALL talk-page links (with anchors)
- Saves them into one JSONL file: data/drn_links.jsonl

Run:
    python scrape_drn.py --outdir data
"""

import requests
from bs4 import BeautifulSoup
import time
import json
import argparse
import os
from urllib.parse import urljoin


USER_AGENT = "TaymProjectBot/0.1 (Taym.mehdi@stud.uni-hannover.de)"
WIKI_BASE = "https://en.wikipedia.org"
SLEEP_SECONDS = 1.0  


def get_html(url: str, session: requests.Session, retries=3):
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries):
        try:
            r = session.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print(f"[WARN] get_html error (attempt {attempt+1}/{retries}): {e}")
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {url}")



def extract_talk_links_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/wiki/Talk:" in href:
            full = urljoin(WIKI_BASE, href)
            links.add(full)

    return sorted(links)


def extract_archive_links(html: str):
    soup = BeautifulSoup(html, "html.parser")
    archives = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "Dispute_resolution_noticeboard" in href and "Archive" in href:
            full = urljoin(WIKI_BASE, href)
            archives.add(full)

    return sorted(archives)


def main(outdir: str):
    os.makedirs(outdir, exist_ok=True)
    session = requests.Session()

    output_file = os.path.join(outdir, "drn_links.jsonl")
    fout = open(output_file, "w", encoding="utf8")

    main_page_url = WIKI_BASE + "/wiki/Wikipedia:Dispute_resolution_noticeboard"
    print("[INFO] Fetching main DRN page...")
    html = get_html(main_page_url, session)

    talk_links = extract_talk_links_from_html(html)
    print(f"[INFO] Found {len(talk_links)} talk links on main page")

    for link in talk_links:
        fout.write(json.dumps({"source": "main", "url": link}) + "\n")

   
    archives = extract_archive_links(html)
    print(f"[INFO] Found {len(archives)} archive pages")

    for arch in archives:
        print(f"[INFO] Processing archive: {arch}")
        try:
            html_arch = get_html(arch, session)
            talk_links_arch = extract_talk_links_from_html(html_arch)
            print(f"  Found {len(talk_links_arch)} talk links")

            for link in talk_links_arch:
                fout.write(json.dumps({"source": arch, "url": link}) + "\n")

        except Exception as e:
            print(f"[ERROR] Failed to scrape archive {arch}: {e}")

        time.sleep(SLEEP_SECONDS)

    fout.close()
    print(f"[DONE] All DRN links saved to: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, default="data",
                        help="Output directory")
    args = parser.parse_args()

    main(args.outdir)
