import json
from pathlib import Path
from dataclasses import asdict

from postal_code_id_ingester.model.augmented import AugmentedPostalCode


def write_jsonl(
    path: str | Path,
    records: list[AugmentedPostalCode],
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")
