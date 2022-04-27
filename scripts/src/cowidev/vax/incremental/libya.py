import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils import get_soup, clean_count
from cowidev.utils.clean import extract_clean_date
from cowidev.vax.utils.incremental import increment, enrich_data


class Libya:
    location = "Libya"
    source_url = "https://ncdc.org.ly/Ar"
    regex = {
        "date": r"\d{1,2} \/ \d{1,2} \/ 20\d{2}م",
        "total_vaccinations": r"إجمالي عدد المطعمين",
        "people_vaccinated": r"الجرعة الأولى",
        "people_fully_vaccinated": r"الجرعة الثانية",
        "total_boosters": r"الجرعة التعزيزية",
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """Get data from the source page."""
        # Etract date from soup
        date = self._parse_date_from_soup(soup)
        # Extract metrics from soup
        metrics = self._parse_metrics_from_soup(soup)

        record = {
            "date": date,
            "total_vaccinations": metrics[0],
            "people_vaccinated": metrics[1],
            "people_fully_vaccinated": metrics[2],
            "total_boosters": metrics[3],
        }
        return record

    def _parse_date_from_soup(self, soup: BeautifulSoup) -> str:
        """Get date from soup."""
        date = soup.find(text=re.compile(self.regex["date"]))
        return extract_clean_date(date, r".* (\d{1,2} \/ \d{1,2} \/ 20\d{2})م",  "%d / %m / %Y")

    def _parse_metrics_from_soup(self, soup: BeautifulSoup) -> tuple:
        """Get metrics from soup."""
        metrics = [
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
        ]
        count = [
            clean_count(
                soup.find(text=re.compile(self.regex[metric]))
                .find_parent("div", class_="wptb-text-container")
                .find_next_sibling("div", class_="wptb-text-container")
                .text
            )
            for metric in metrics
        ]
        return count

    def pipe_source(self, data_series: pd.Series) -> pd.Series:
        """Pipes source url."""
        return enrich_data(data_series, "source_url", self.source_url)

    def pipe_location(self, data_series: pd.Series) -> pd.Series:
        """Pipes location."""
        return enrich_data(data_series, "location", self.location)

    def pipe_vaccine(self, data_series: pd.Series) -> pd.Series:
        """Pipes vaccine names."""
        return enrich_data(data_series, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac, Sputnik V")

    def pipeline(self, data_series: pd.Series) -> pd.Series:
        """Pipeline for data."""
        return data_series.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

    def export(self):
        """Exports data to csv."""
        data = self.read().pipe(self.pipeline)
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


def main():
    Libya().export()
