import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class OverrideRule:
    level: str           # village | district
    code: str
    canonical_name: str
    postal_alias: str
    match_mode: str      # district_only | district_village


def load_override_rules(path: str | None):
    if not path:
        return {}

    rules = {}

    with open(Path(path), newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rule = OverrideRule(
                level=row["level"].strip(),
                code=row["code"].strip(),
                canonical_name=row["canonical_name"].strip(),
                postal_alias=row["postal_alias"].strip(),
                match_mode=row["match_mode"].strip(),
            )
            rules[(rule.level, rule.code)] = rule

    return rules
