import csv
from postal_code_id_ingester.model.village import VillageInput


def load_failed_villages(path: str) -> list[VillageInput]:
    villages = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            villages.append(
                VillageInput(
                    village_code=row["village_code"],
                    village=row["village"],
                    district=row["district"],
                    city=row["city"],
                    province=row["province"],
                )
            )

    return villages
