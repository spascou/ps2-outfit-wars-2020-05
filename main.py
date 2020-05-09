import os
from typing import Optional

from characters import generate_outfit_characters_data

SERVICE_ID: Optional[str] = os.environ.get("CENSUS_SERVICE_ID")

OUTFIT_NAME: str = "The Last Ravens"

if not SERVICE_ID:
    raise ValueError("CENSUS_SERVICE_ID envvar not found")

START: int = 1588914000
END: int = 1588942800

generate_outfit_characters_data(
    service_id=SERVICE_ID, outfit_name=OUTFIT_NAME, from_ts=START, to_ts=END
)
