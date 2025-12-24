import json
import csv

INPUT_FILE = "data/conversations.jsonl"
OUTPUT_FILE = "data/final_dataset.csv"

rows = []

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"[SKIP] Line {line_num}: {e}")
            continue

        rows.append({
            "talk_page": obj.get("talk_page", ""),
            "section": obj.get("section", ""),
            "conversation_text": obj.get("conversation_text", ""),
            "label": obj.get("label", ""),
            "num_comments": len(obj.get("comments", []))
        })

with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["talk_page", "section", "conversation_text", "label", "num_comments"],
        quoting=csv.QUOTE_ALL  
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"Done. Wrote {len(rows)} rows to {OUTPUT_FILE}")
