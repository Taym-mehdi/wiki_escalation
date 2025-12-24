#!/usr/bin/env python3
"""
extract_anchored_conversations.py 

For each anchored talk-page link:
    - Fetches talk-page wikitext via MediaWiki API
    - Robustly resolves URL anchor → actual section heading
    - Extracts ONLY the referenced section
    - Cleans text and parses comments (simple heuristic)
    - Saves structured conversation data
Run:
    python extract_anchored_conversations.py
"""

import json
import argparse
import os
import time
import re
import requests
import mwparserfromhell
import urllib.parse
from urllib.parse import urlparse
from difflib import SequenceMatcher



USER_AGENT = "TaymProjectBot/0.1 (Taym.mehdi@stud.uni-hannover.de)"
API_ENDPOINT = "https://en.wikipedia.org/w/api.php"
SLEEP_SECONDS = 0.5
SIMILARITY_THRESHOLD = 0.6


def fetch_wikitext_via_api(title: str, session: requests.Session, retries=3):
    headers = {"User-Agent": USER_AGENT}
    params = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "prop": "revisions",
        "rvprop": "content|timestamp",
        "rvslots": "main",
        "titles": title,
    }

    for attempt in range(retries):
        try:
            r = session.get(API_ENDPOINT, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()

            pages = data.get("query", {}).get("pages", [])
            if not pages or "missing" in pages[0]:
                return None

            rev = pages[0].get("revisions", [{}])[0]
            return {
                "wikitext": rev.get("slots", {}).get("main", {}).get("content", ""),
                "timestamp": rev.get("timestamp", ""),
            }

        except Exception as e:
            print(f"[WARN] API error for '{title}' (attempt {attempt+1}): {e}")
            time.sleep(2 * (attempt + 1))

    print(f"[ERROR] Failed after retries: {title}")
    return None



def normalize_anchor(s: str) -> str:
    if not s:
        return ""

    # URL decode
    s = urllib.parse.unquote(s)

    # MediaWiki anchor quirks
    s = s.replace("_", " ")
    s = s.replace(".22", '"')
    s = s.replace(".28", "(")
    s = s.replace(".29", ")")

    # Normalize punctuation and whitespace
    s = re.sub(r"[–—-]", " ", s)     # dash variants
    s = re.sub(r"[^\w\s\"()]", "", s)
    s = re.sub(r"\s+", " ", s)

    return s.strip().lower()



def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()



def extract_section(wikitext: str, anchor: str):
    if not anchor:
        return wikitext

    wikicode = mwparserfromhell.parse(wikitext)
    sections = wikicode.get_sections(flat=True, include_lead=True)

    norm_anchor = normalize_anchor(anchor)

    best_section = None
    best_score = 0.0

    for section in sections:
        headers = section.filter_headings()
        if not headers:
            continue

        title = headers[0].title.strip_code().strip()
        norm_title = normalize_anchor(title)

        score = similarity(norm_anchor, norm_title)
        if score > best_score:
            best_score = score
            best_section = section

    if best_section and best_score >= SIMILARITY_THRESHOLD:
        return str(best_section)

    return None


def extract_comments(section_text: str):
    comments = []
    buffer = []

    signature_regex = re.compile(
        r"(--\s*\[\[User:[^\]]+\]\]|--~~~~)",
        re.IGNORECASE
    )

    for line in section_text.split("\n"):
        buffer.append(line)

        if signature_regex.search(line):
            comments.append({
                "user": "Unknown",
                "text": "\n".join(buffer).strip()
            })
            buffer = []

    return comments



def clean_text(s: str) -> str:
    s = re.sub(r"<.*?>", "", s)
    s = s.replace("'''", "").replace("''", "")
    return s.strip()


def main(input_file: str, outdir: str):
    os.makedirs(outdir, exist_ok=True)
    output_path = os.path.join(outdir, "conversations.jsonl")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    fout = open(output_path, "w", encoding="utf8")

    with open(input_file, "r", encoding="utf8") as fin:
        for line in fin:
            rec = json.loads(line)
            url = rec["url"]

            if "#" not in url:
                continue

            parsed = urlparse(url)
            title = parsed.path.replace("/wiki/", "")
            anchor = parsed.fragment

            api_result = fetch_wikitext_via_api(title, session)
            if not api_result:
                continue

            section_text = extract_section(api_result["wikitext"], anchor)
            if section_text is None:
                print(f"[WARN] Section not found: {title}#{anchor}")
                continue

            cleaned = clean_text(section_text)
            comments = extract_comments(cleaned)

            output = {
                "talk_page": title,
                "section": anchor,
                "conversation_text": cleaned,
                "comments": comments,
                "label": "escalated"
            }

            fout.write(json.dumps(output, ensure_ascii=False) + "\n")
            time.sleep(SLEEP_SECONDS)

    fout.close()
    print(f"[DONE] Saved conversations → {output_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="data/drn_links.jsonl")
    parser.add_argument("--outdir", type=str, default="data")
    args = parser.parse_args()

    main(args.input, args.outdir)
