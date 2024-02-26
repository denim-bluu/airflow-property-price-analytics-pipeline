from src.zoopla_scraper import run_zoopla_scraper
from playwright.sync_api import sync_playwright
import json

if __name__ == "__main__":

    def main():
        with sync_playwright() as playwright:
            all_properties = run_zoopla_scraper(playwright)
        return all_properties

    all_properties = main()
    all_properties[0].model_dump_json()

    with open("./data/output.json", "w") as f:
        json.dump([model.model_dump() for model in all_properties], f)

    with open("./data/output.json") as f:
        data = json.load(f)

    breakpoint()
