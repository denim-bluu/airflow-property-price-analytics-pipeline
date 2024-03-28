import re
from typing import List

import logging

from .. import errors
from bs4 import BeautifulSoup
from bs4.element import Tag
from ..scraper.data_model import Property
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from ..util.parser import parse_date
import urllib.parse
from .. import constants


def example_url_constructor(minimum_beds: int, location: str) -> str:
    params = {
        "beds_min": minimum_beds,
        "is_retirement_home": "false",
        "is_shared_ownership": "false",
    }
    return f"{constants.ZOOPLA_SEARCH_URL}/{location}/?{urllib.parse.urlencode(params)}"


class ZooplaSCraper:
    def __init__(self):
        self.opts = FirefoxOptions()
        self.opts.add_argument("--headless")
        self.opts.add_argument("--no-sandbox")

    def _find_element_by_attribute(
        self, page_source: str, attribute: str, value: str
    ) -> Tag:
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
        logging.info(f"Finding element by {attribute}={value}")
        soup = BeautifulSoup(page_source, "html.parser")
        element = soup.find(attribute, {"data-testid": value})
        if not element or not isinstance(element, Tag):
            raise errors.ElementNotFoundError(
                f"No element found with {attribute}={value}"
            )
        return element

    def _find_max_pages(self, page_source: str) -> int:
        """
        Find the maximum number of pages in the pagination.

        Args:
            page_source (str): The HTML source code of the page.

        Returns:
            int: The maximum number of pages.
        """
        logging.info("Finding max pages")
        pagination = self._find_element_by_attribute(page_source, "div", "pagination")
        return len(pagination.find_all("li", {"class": "_14xj7k74"}))

    def _next_page_exists(self, page_source: str) -> bool:
        """
        Checks if the next page exists in the given page source.

        Args:
            page_source (str): The HTML source code of the page.

        Returns:
            bool: True if the next page exists, False otherwise.
        """
        pagination = self._find_element_by_attribute(page_source, "div", "pagination")
        next_page = pagination.find("div", {"class": "_14xj7k72"})
        item = next_page.find("a", {"class": "qimhss0 qimhss3 qimhss9 _194zg6t8"})  # type: ignore
        if item.get("href"):  # type: ignore
            return True
        else:
            return False

    def _get_listing_html(self, page_source: str) -> Tag:
        """
        Get the HTML of the listings.

        Args:
            page_source (str): The HTML source code of the page.

        Returns:
            Tag: The HTML of the listings.
        """
        logging.info("Getting listing HTML")
        return self._find_element_by_attribute(page_source, "div", "regular-listings")

    def scrape_url(self, url: str) -> List[Property]:
        """
        Scrape a page for property listings.

        Args:
            url (str): The URL of the page to scrape.

        Returns:
            List[Property]: A list of property listings.
        """
        go_to_next = True
        page = 0
        all_properties = []
        while go_to_next:
            page += 1
            driver = webdriver.Firefox(options=self.opts)
            driver.get(url + f"&pn={page}")
            logging.info(f"Scraping: {url}&pn={page}")
            page_source = driver.page_source
            listing_soup = self._get_listing_html(page_source)
            go_to_next = self._next_page_exists(page_source)
            driver.close()

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
                property_data["description"] = (
                    description.get_text() if description else ""
                )

                listing_title = pr.find("h2", {"data-testid": "listing-title"})
                property_data["listing_title"] = (
                    listing_title.get_text() if listing_title else ""
                )

                price = pr.find("p", {"data-testid": "listing-price"})
                property_data["price"] = (
                    int(price.get_text().replace("£", "").replace(",", ""))
                    if price
                    else 0
                )

                terms = pr.find_all("div", {"class": "jc64990 jc64994 _194zg6tb"})
                property_data["terms"] = str([term.get_text() for term in terms])

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
        return all_properties
