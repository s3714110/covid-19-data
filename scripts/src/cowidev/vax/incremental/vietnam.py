import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import increment, enrich_data


class Vietnam:
    location = "Vietnam"
    base_url = "https://covid19.gov.vn"
    source_url = "https://covid19.gov.vn/ban-tin-covid-19.htm"
    regex = {
        "title": r"Ngày",
        "date": r"(\d{2}/\d{2}/\d{4})",
        # "metrics": {
        #     "total": r"tổng số liều (vắc xin|vaccine) đã được tiêm là ([\d\.]+) liều, ",
        #     "adult": r"tuổi trở lên là \d+ liều: Mũi 1 là ([\d\.]+) liều; Mũi 2 là ([\d\.]+) liều; Mũi 3 là ([\d\.]+) liều; Mũi bổ sung là ([\d\.]+) liều; Mũi nhắc lại là ([\d\.]+) liều",
        #     "adolescent": r"tuổi là \d+ liều: Mũi 1 là ([\d\.]+) liều; Mũi 2 là ([\d\.]+) liều.",
        # },
        "metrics": {
            "adult": (
                r"tuổi trở lên là\s?(\d+) liều, trong đó Mũi 1: ([\d\.]+) liều; Mũi 2: ([\d\.]+) liều; Mũi bổ sung:"
                r" ([\d\.]+) liều và Mũi 3: ([\d\.]+) liều;"
            ),
            "adolescent": r"12\-17 tuổi là (\d+) liều, gồm Mũi 1: (\d+) liều; Mũi 2: (\d+) liều;",
        },
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> dict:
        """Get data from the source page."""
        # Get relevant link
        url = self._get_relevant_link(soup)
        # Extract text from url
        # print(url)
        text = self._get_text_from_url(url)
        # Extract date from text
        date = self._parse_date_from_text(text)
        # Extract metrics from text
        metrics = self._parse_metrics(text)
        record = {"source_url": url, "date": date, **metrics}
        return record

    def _get_relevant_link(self, soup: BeautifulSoup) -> str:
        """Get the relevant URL from the source page."""
        elem_list = soup.find_all("a", title=re.compile(self.regex["title"]))
        if not elem_list:
            raise ValueError("No relevant links found, please update the regex")
        href = elem_list[0]["href"]
        url = f"{self.base_url}{href}"
        return url

    def _get_text_from_url(self, url: str) -> str:
        """Extract text from URL."""
        soup = get_soup(url)
        text = soup.get_text()
        text = re.sub(r"(\d)\.(\d)", r"\1\2", text)
        text = re.sub(r"\s+", " ", text)
        return text

    def _parse_date_from_text(self, text: str) -> str:
        """Get date from text."""
        date = re.search(self.regex["date"], text).group(1)
        date = clean_date(date, "%d/%m/%Y", as_datetime=True) - pd.Timedelta(days=1)
        return date.strftime("%Y-%m-%d")

    def _parse_metrics(self, text: str) -> tuple:
        """Get metrics from text."""
        adults = [clean_count(num) for num in re.search(self.regex["metrics"]["adult"], text).group(1, 2, 3, 4, 5)]
        adolescents = [clean_count(num) for num in re.search(self.regex["metrics"]["adolescent"], text).group(1, 2)]
        metrics = {
            # "total_vaccinations": clean_count(re.search(self.regex["metrics"]["total"], text).group(2)),
            "total_vaccinations": adults[0] + adolescents[0],
            "people_vaccinated": adults[1] + adolescents[1],
            # "people_fully_vaccinated": adults[2] + adults[2] + adolescents[1],
            # "total_boosters": adults[3] + adults[4],
        }
        return metrics

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        """Pipe location."""
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        """Pipe vaccine name."""
        return enrich_data(
            ds,
            "vaccine",
            "Abdala, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for data."""
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine)

    def export(self):
        """Export data to CSV."""
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            date=data["date"],
            vaccine=data["vaccine"],
            source_url=data["source_url"],
            people_vaccinated=data["people_vaccinated"],
            # people_fully_vaccinated=data["people_fully_vaccinated"],
            # total_boosters=data["total_boosters"],
        )


def main():
    Vietnam().export()
