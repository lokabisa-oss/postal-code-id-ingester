import csv
from pathlib import Path

from postal_code_id_ingester.model.village import VillageInput


def load_villages_from_region_id(
    csv_path: str | Path,
) -> list[VillageInput]:
    villages: list[VillageInput] = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            village_code = (row.get("village_code") or "").strip()
            village = (row.get("village_name") or "").strip()
            district_code = (row.get("district_code") or "").strip()
            district = (row.get("district_name") or "").strip()
            city = (row.get("regency_name") or "").strip()
            province = (row.get("province_name") or "").strip()

            # filter by presence of village_code (leaf node)
            if not village_code:
                continue

            if not all([village, district, city, province]):
                continue

            villages.append(
                VillageInput(
                    village_code=village_code,
                    village=village,
                    district_code=district_code,
                    district=district,
                    city=city,
                    province=province,
                )
            )

    return villages
