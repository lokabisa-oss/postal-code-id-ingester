from typing import Optional
from fuzzy_core import similarity

from postal_code_id_ingester.model.village import VillageInput


def match_postal_candidate(
    village: VillageInput,
    candidate: dict,
    *,
    mode: str = "village",
    threshold: float = 0.8,
) -> Optional[float]:
    """
    Return confidence score if candidate matches the village,
    otherwise return None.

    Modes:
    - village (default): village + district + province
    - city: district + city only (LAST RESORT)
    """

    if mode == "city":
        district_score = similarity(
            village.district,
            candidate.get("district", ""),
        )

        city_score = similarity(
            village.city,
            candidate.get("city", ""),
        )

        score = (
            district_score * 0.6
            + city_score * 0.4
        )

        if score >= 0.6:
            return round(score, 3)
        return None

    # ---- default village-level matching ----
    village_score = similarity(
        village.village,
        candidate.get("village", ""),
    )

    district_score = similarity(
        village.district,
        candidate.get("district", ""),
    )

    province_score = similarity(
        village.province,
        candidate.get("province", ""),
    )

    score = (
        village_score * 0.5
        + district_score * 0.3
        + province_score * 0.2
    )

    if score >= threshold:
        return round(score, 3)

    return None
