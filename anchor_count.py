import json
from pathlib import Path


def count_links(jsonl_path: str) -> dict:
    
    total = 0
    anchored = 0

    path = Path(jsonl_path)
    unique_url = set()
    all_url = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue  
            record = json.loads(line)
            url = record.get("url", "")
            unique = url.split('#')[0]
            unique_url.add(unique)
            all_url.append(unique)
            total += 1
            if "#" in url:
                anchored += 1

    return {
        "total_links": total,
        "anchored_links": anchored,
        "page_only_links": total - anchored,
        "unique_url": len(unique_url),
        "test" : len (all_url)
    }


if __name__ == "__main__":
    result = count_links("data/drn_links.jsonl")
    print(result)
