from dataclasses import dataclass

@dataclass(frozen=True)
class VillageInput:
    village_code: str
    village: str
    district_code: str
    district: str
    city: str
    province: str
