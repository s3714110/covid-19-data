import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cowidev.utils.clean import clean_count
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.utils.clean.dates import clean_date


def read(source: str) -> pd.Series:
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(source)
        time.sleep(3)
        metric_names = [e.text for e in driver.find_elements_by_class_name("jss73")]
        metric_values = [e.text for e in driver.find_elements_by_class_name("jss74")]
        for name, value in zip(metric_names, metric_values):
            if "Primera dosis" == name:
                people_vaccinated = clean_count(value)
            elif "Cantidad total de dosis administradas" == name:
                total_vaccinations = clean_count(value)
            elif "Población completamente vacunada" in name:
                people_fully_vaccinated = clean_count(value)
            elif "Dosis de refuerzo" in name:
                total_boosters = clean_count(value)

        dt_candidates = [e.text for e in driver.find_elements_by_tag_name("h3")]
        for dt_candidate in dt_candidates:
            if "Estadísticas nacionales | Acumulados" in dt_candidate:
                date = clean_date(dt_candidate, "Estadísticas nacionales | Acumulados al %d de %B de %Y", lang="es")
    data = {
        "date": date,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_vaccinations": total_vaccinations,
        "total_boosters": total_boosters,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Dominican Republic")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main():
    source = "https://vacunate.gob.do/"
    data = read(source).pipe(pipeline, source)
    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )
