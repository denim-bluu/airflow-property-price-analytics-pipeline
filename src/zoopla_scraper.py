import json
import re
from typing import Any

from bs4 import BeautifulSoup
from playwright.sync_api import Page, sync_playwright

from src.data_struct import Property
from util.browser_support import go_to_page_wrapper, open_browser
from util.parser import parse_date


def find_max_pages(page: Page) -> int:
    buttons = page.get_by_test_id("pagination").inner_html()
    soup = BeautifulSoup(buttons, "html.parser")
    return len(soup.find_all("li", {"class": "_14xj7k74"}))


def run_zoopla_scraper(run_date: str):
    """Orchestrates scraping, validation, and Delta Lake interaction."""
    playwright = sync_playwright().start()

    url = "https://www.zoopla.co.uk/for-sale/property/welwyn-garden-city/?beds_max=3&beds_min=2&q=Welwyn%20Garden%20City%2C%20Hertfordshire&results_sort=newest_listings&search_source=for-sale"
    browser, page = open_browser(playwright)
    go_to_page_wrapper(page, url)

    n_pages = find_max_pages(page)
    browser.close()

    all_properties = []
    for i in range(n_pages):
        print(f"Scraping page {i+1} of {n_pages}")
        browser, page = open_browser(playwright)
        go_to_page_wrapper(page, url + f"&pn={i+1}")

        listing_html = page.get_by_test_id("regular-listings").inner_html()
        data_soup = BeautifulSoup(listing_html, "html.parser")

        # Class name for each listing
        regular_listings = data_soup.find_all("div", {"class": "dkr2t82"})
        if not regular_listings:
            raise ValueError("No listings found")

        meta_data = {}
        for pr in regular_listings:
            meta_data["id"] = pr.attrs["id"]

            address = pr.find_all("address", {"class": "m6hnz62 _194zg6t9"})
            if len(address) != 1:
                raise ValueError("Invalid number of addresses found")
            meta_data["address"] = address[0].get_text()

            listed_date = pr.find_all("li", {"class": "jlg7241"})
            if len(listed_date) != 1:
                raise ValueError("Invalid number of dates found")
            meta_data["listed_date"] = parse_date(listed_date[0].get_text()).strftime(
                "%Y-%m-%d"
            )

            description = pr.find_all("p", {"class": "m6hnz63 _194zg6t9"})
            if len(description) != 1:
                raise ValueError("Invalid number of descriptions found")
            meta_data["description"] = description[0].get_text()

            listing_title = pr.find_all("h2", {"data-testid": "listing-title"})
            if len(listing_title) != 1:
                raise ValueError("Invalid number of descriptions found")
            meta_data["listing_title"] = listing_title[0].get_text()

            # Class name for each room details
            price = pr.find_all("p", {"data-testid": "listing-price"})
            if len(price) != 1:
                raise ValueError("Invalid number of prices found")
            meta_data["price"] = int(
                price[0].get_text().replace("£", "").replace(",", "")
            )

            terms = pr.find_all("div", {"class": "jc64990 jc64994 _194zg6tb"})
            meta_data["terms"] = [i.get_text() for i in terms]

            # Class name for each room details
            room_details = pr.find_all("li", {"class": "_1wickv1"})
            for detail in room_details:
                txt = detail.get_text().lower()
                nums = re.findall(r"\d+", txt)
                if len(nums) != 1:
                    raise ValueError("Invalid number of digits found")
                nums = int(nums[0])

                if "bed" in txt:
                    meta_data["bedrooms"] = nums
                if "bath" in txt:
                    meta_data["bathrooms"] = nums
                if "living" in txt:
                    meta_data["livingrooms"] = nums

            all_properties.append(Property(**meta_data))
        browser.close()
    playwright.stop()

    data: Any = [model.model_dump() for model in all_properties]

    with open(f"data/scraped_output_{run_date}.json", "w") as f:
        json.dump(data, f)
