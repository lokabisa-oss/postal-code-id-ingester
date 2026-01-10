from pathlib import Path
import json


def load_seen_village_codes(output_path: str) -> set[str]:
    path = Path(output_path)
    seen: set[str] = set()

    if not path.exists():
        return seen

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                vc = obj.get("village_code")
                if vc:
                    seen.add(vc)
            except json.JSONDecodeError:
                # skip corrupted line safely
                continue

    return seen
