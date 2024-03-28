from datetime import datetime
import re
from typing import Dict
import yaml
from pathlib import Path


def parse_date(date_str: str) -> datetime:
    # Extract the date part (assuming the format is consistent)
    date_part = date_str.split("on ")[1]
    # Remove ordinal suffix from day
    cleaned_date = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_part)

    return datetime.strptime(cleaned_date, "%d %b %Y")


def get_yaml_config() -> Dict:
    path = str(Path(__file__).parent.parent / "config.yaml")
    return yaml.load(open(path, "r"), Loader=yaml.SafeLoader)
