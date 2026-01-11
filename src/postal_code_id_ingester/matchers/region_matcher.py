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
    - village: village + district + province (default)
    - city: last-resort city-level matching
    """
    # ----------------------------
    # CITY / DISTRICT ONLY MODES
    # ----------------------------
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

    # ----------------------------
    # DEFAULT VILLAGE MODE
    # ----------------------------
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



def match_postal_candidate_override(
    village: VillageInput,
    candidate: dict,
    *,
    mode: str = "village",
    postal_alias: str | None = None,
    threshold: float = 0.8,
) -> Optional[float]:
    """
    Return confidence score if candidate matches the village,
    otherwise return None.

    Modes:
    - district_only: district + city ONLY (no village)
    - village_only: village ONLY (no district or city)
    """
    # ----------------------------
    # CITY / DISTRICT ONLY MODES
    # ----------------------------
    if mode == "district_only":
        district_score = similarity(
            postal_alias,
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
    
    # ----------------------------
    # DISTRICT VILLAGE MODES
    # ----------------------------
    if mode == "district_village":
        district_score = similarity(
            postal_alias,
            candidate.get("district", ""),
        )

        village_score = similarity(
            village.village,
            candidate.get("village", ""),
        )

        score = (
            district_score * 0.6
            + village_score * 0.4
        )

        if score >= 0.6:
            return round(score, 3)
        return None

    # ----------------------------
    # DISTRICT + VILLAGE (TOLERANT)
    # ----------------------------
    if mode == "village_only":
        score = similarity(
            postal_alias,
            candidate.get("village", ""),
        )

        if score >= 0.65:
            return round(score, 3)
        return None

    # ----------------------------
    # DEFAULT VILLAGE MODE
    # ----------------------------
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
