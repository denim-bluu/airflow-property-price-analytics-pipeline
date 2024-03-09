import json
import re
from typing import List, Dict, Any

from bs4 import BeautifulSoup
from bs4.element import Tag
from selenium import webdriver
from scraper.data_model import Property
from util import const
from resources.s3 import export_to_s3
import errors
from util.parser import parse_date
from selenium.webdriver import FirefoxOptions
import urllib.parse

opts = FirefoxOptions()
opts.add_argument("--headless")


def find_element_by_attribute(page_source: str, attribute: str, value: str) -> Tag:
    """
    Find an HTML element by its attribute and value.

    Args:
        page_source (str): The HTML source code of the page.
        attribute (str): The attribute of the HTML element.
        value (str): The value of the attribute.

    Returns:
        Tag: The found HTML element.

    Raises:
        errors.ElementNotFoundError: If no element is found with the given attribute and value.
    """

    soup = BeautifulSoup(page_source, "html.parser")
    element = soup.find(attribute, {"data-testid": value})
    if not element or not isinstance(element, Tag):
        raise errors.ElementNotFoundError(f"No element found with {attribute}={value}")
    return element


def find_max_pages(page_source: str) -> int:
    """
    Find the maximum number of pages in the pagination.

    Args:
        page_source (str): The HTML source code of the page.

    Returns:
        int: The maximum number of pages.
    """

    pagination = find_element_by_attribute(page_source, "div", "pagination")
    return len(pagination.find_all("li", {"class": "_14xj7k74"}))


def get_listing_html(page_source: str) -> Tag:
    """
    Get the HTML of the listings.

    Args:
        page_source (str): The HTML source code of the page.

    Returns:
        Tag: The HTML of the listings.
    """
    return find_element_by_attribute(page_source, "div", "regular-listings")


def scrape_page(url: str) -> List[Property]:
    """
    Scrape a page for property listings.

    Args:
        url (str): The URL of the page to scrape.

    Returns:
        List[Property]: A list of property listings.
    """
    #TODO: Need to iterate through tha pages via Next button instead of shown page number.
    driver = webdriver.Firefox(options=opts)
    driver.get(url)
    page_source = driver.page_source
    driver.close()

    n_pages = find_max_pages(page_source)
    all_properties = []

    for i in range(n_pages):
        print(f"Scraping page {i+1} of {n_pages}")
        driver = webdriver.Firefox(options=opts)
        driver.get(url + f"&pn={i+1}")
        listing_soup = get_listing_html(driver.page_source)

        for pr in listing_soup.find_all("div", {"class": "dkr2t82"}):
            property_data = {}

            property_data["id"] = pr.attrs["id"]
            address = pr.find("address", {"class": "m6hnz62 _194zg6t9"})
            property_data["address"] = address.get_text() if address else ""

            listed_date = pr.find("li", {"class": "jlg7241"})
            property_data["listed_date"] = (
                parse_date(listed_date.get_text()).strftime("%Y-%m-%d")
                if listed_date
                else ""
            )

            description = pr.find("p", {"class": "m6hnz63 _194zg6t9"})
            property_data["description"] = description.get_text() if description else ""

            listing_title = pr.find("h2", {"data-testid": "listing-title"})
            property_data["listing_title"] = (
                listing_title.get_text() if listing_title else ""
            )

            price = pr.find("p", {"data-testid": "listing-price"})
            property_data["price"] = (
                int(price.get_text().replace("£", "").replace(",", "")) if price else 0
            )

            terms = pr.find_all("div", {"class": "jc64990 jc64994 _194zg6tb"})
            property_data["terms"] = [term.get_text() for term in terms]

            room_details = pr.find_all("li", {"class": "_1wickv1"})
            for detail in room_details:
                txt = detail.get_text().lower()
                nums = re.findall(r"\d+", txt)
                if len(nums) != 1:
                    raise ValueError("Invalid number of digits found")
                nums = int(nums[0])

                if "bed" in txt:
                    property_data["bedrooms"] = nums
                elif "bath" in txt:
                    property_data["bathrooms"] = nums
                elif "living" in txt:
                    property_data["livingrooms"] = nums

            all_properties.append(Property(**property_data))

        driver.close()

    return all_properties


def run_zoopla_scraper(run_date: str, location: str):
    """
    Run the Zoopla scraper.

    Args:
        run_date (str): The date to run the scraper.
    """
    params = {"location": location}
    url = f"{const.ZOOPLA_BASE_URL} / {urllib.parse.urlencode(params)}"
    print("#" * 50)
    print(f"Scraping Zoopla for {location} on {run_date}")
    print("#" * 50)

    try:
        all_properties = scrape_page(url)
    except Exception as e:
        print(f"Error occurred: {e}")
        return

    data: List[Dict[str, Any]] = [property.model_dump() for property in all_properties]

    export_to_s3(
        json.dumps(data),
        const.AWS_S3_BUCKET,
        f"{const.JSON_DATA_DIR.format(run_date=run_date, location=location)}",
    )
