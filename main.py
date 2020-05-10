import os
from typing import Optional, Tuple

from characters import generate_outfit_characters_data

SERVICE_ID: Optional[str] = os.environ.get("CENSUS_SERVICE_ID")

OUTFIT_NAME: str = "The Last Ravens"

if not SERVICE_ID:
    raise ValueError("CENSUS_SERVICE_ID envvar not found")

DAY_1: Tuple[int, int] = (1588914000, 1588942800)
DAY_2: Tuple[int, int] = (1589000400, 1589029200)
DAY_3: Tuple[int, int] = (1589086800, 1589115600)

generate_outfit_characters_data(
    service_id=SERVICE_ID, outfit_name=OUTFIT_NAME, time_frames=(DAY_1, DAY_2, DAY_3)
)
