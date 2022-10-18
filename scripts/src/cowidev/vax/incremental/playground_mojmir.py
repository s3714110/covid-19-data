import time

import pandas as pd
from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_driver
from cowidev.vax.utils.incremental import enrich_data, increment
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChroOpt
from selenium.webdriver.firefox.options import Options as FireOpt


def sel_options(headless: bool = True, firefox: bool = False):
    if firefox:
        op = FireOpt()
    else:
        op = ChroOpt()
        op.add_experimental_option(
            "prefs",
            {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
    op.add_argument('--no-sandbox')
    op.add_argument("--disable-notifications")
    op.add_argument("--disable-dev-shm-usage")
    if headless:
        op.add_argument("--headless")
    return op


def read(source: str) -> pd.Series:

    options = sel_options(headless=True, firefox=False)

    driver = webdriver.Chrome('/snap/bin/chromium.chromedriver', options=options)

    with get_driver(options=options) as driver:
        driver.get(source)
        # time.sleep(10)
        return

        for block in driver.find_elements_by_class_name("kpimetric"):
            if "1ste dosis" in block.text and "%" not in block.text:
                people_partly_vaccinated = clean_count(block.find_element_by_class_name("valueLabel").text)
            elif "2de dosis" in block.text and "%" not in block.text:
                people_fully_vaccinated = clean_count(block.find_element_by_class_name("valueLabel").text)
            elif "3de dosis" in block.text and "%" not in block.text:
                total_boosters = clean_count(block.find_element_by_class_name("valueLabel").text)

    people_vaccinated = people_partly_vaccinated + people_fully_vaccinated

    return pd.Series(
        data={
            "total_vaccinations": people_vaccinated + people_fully_vaccinated,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
            "date": localdate("America/Paramaribo"),
        }
    )



def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Suriname")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://laatjevaccineren.sr/")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://datastudio.google.com/u/0/reporting/d316df2b-49e0-4c3e-aa51-0900828d8cf5/page/igSUC"
    data = read(source)
    print(data)


if __name__ == '__main__':
    main()
