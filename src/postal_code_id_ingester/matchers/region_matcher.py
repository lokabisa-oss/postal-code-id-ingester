from typing import Optional
from fuzzy_core import similarity

from postal_code_id_ingester.model.village import VillageInput


def match_postal_candidate(
    village: VillageInput,
    candidate: dict,
    threshold: float = 0.8,
) -> Optional[float]:
    """
    Return confidence score if candidate matches the village,
    otherwise return None.
    """

    village_score = similarity(
        village.village,
        candidate["village"],
    )

    district_score = similarity(
        village.district,
        candidate["district"],
    )

    province_score = similarity(
        village.province,
        candidate["province"],
    )

    # weighted score
    score = (
        village_score * 0.5
        + district_score * 0.3
        + province_score * 0.2
    )

    if score >= threshold:
        return score

    return None
