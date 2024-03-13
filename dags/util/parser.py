from datetime import datetime
import re
from typing import Dict
import yaml


def parse_date(date_str: str) -> datetime:
    # Extract the date part (assuming the format is consistent)
    date_part = date_str.split("on ")[1]
    # Remove ordinal suffix from day
    cleaned_date = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_part)

    return datetime.strptime(cleaned_date, "%d %b %Y")


def get_yaml_config() -> Dict:
    return yaml.load(open("config.yaml", "r"), Loader=yaml.SafeLoader)
