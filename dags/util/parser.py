from datetime import datetime
import re
import configparser


def parse_date(date_str: str) -> datetime:
    # Extract the date part (assuming the format is consistent)
    date_part = date_str.split("on ")[1]
    # Remove ordinal suffix from day
    cleaned_date = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_part)

    return datetime.strptime(cleaned_date, "%d %b %Y")


def get_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config
