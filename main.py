import os
from typing import Optional, Tuple

from ps2_census.enums import Zone

from characters import generate_outfit_characters_data

SERVICE_ID: Optional[str] = os.environ.get("CENSUS_SERVICE_ID")

RVNX: str = "RvnX"
YLBT: str = "YLBT"
RAVE: str = "RAVE"
TCFB: str = "TCFB"

if not SERVICE_ID:
    raise ValueError("CENSUS_SERVICE_ID envvar not found")

DAY_1: Tuple[int, int] = (1588914000, 1588942800)
DAY_2: Tuple[int, int] = (1589000400, 1589029200)
DAY_3: Tuple[int, int] = (1589086800, 1589115600)
WAR: Tuple[int, int] = (1589629800, 1589631900)


def desolation_filter(event: dict) -> bool:
    return int(event["zone_id"]) not in {
        Zone.AMERISH.value,
        Zone.ESAMIR.value,
        Zone.HOSSIN.value,
        Zone.INDAR.value,
        Zone.VR_NC.value,
        Zone.VR_TR.value,
        Zone.VR_VS.value,
    }


generate_outfit_characters_data(
    service_id=SERVICE_ID,
    outfit_tag=TCFB,
    time_frames=(WAR,),
    custom_filter=desolation_filter,
)
