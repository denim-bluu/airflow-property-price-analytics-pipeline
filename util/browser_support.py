import random
from playwright.sync_api import Playwright, Browser, Page


def simulate_mimic_human_behaviour(page: Page) -> None:
    # Add a random delay of 1 to 5 seconds to simulate human behavior
    page.wait_for_timeout(1000 * (1 + random.random() * 4))
    # Scroll the page to load additional content
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    # Add another random delay of 1 to 5 seconds
    page.wait_for_timeout(1000 * (1 + random.random() * 4))


def go_to_page_wrapper(page: Page, url: str) -> None:
    simulate_mimic_human_behaviour(page)
    page.goto(url)


def open_browser(playwright: Playwright) -> tuple[Browser, Page]:
    browser = playwright.firefox.launch(headless=False)
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    return browser, context.new_page()
