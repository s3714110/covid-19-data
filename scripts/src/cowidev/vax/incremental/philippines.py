import json
from bs4 import BeautifulSoup

import pandas as pd

from cowidev.utils import clean_date, clean_count, get_soup
from cowidev.vax.utils.incremental import increment, enrich_data
from cowidev.vax.utils.utils import add_latest_who_values
from cowidev.vax.utils.base import CountryVaxBase


class Philippines(CountryVaxBase):
    location: str = "Philippines"
    source_url: str = "https://e.infogram.com/_/yFVE69R1WlSdqY3aCsBF"
    source_url_ref: str = (
        "https://news.abs-cbn.com/spotlight/multimedia/infographic/03/23/21/philippines-covid-19-vaccine-tracker"
    )
    metric_entities: dict = {
        "total_vaccinations": "4b9e949e-2990-4349-aa85-5aff8501068a",
        # "people_vaccinated": "25d75a0a-cb56-4824-aed4-4410f395577a",
        "people_fully_vaccinated": "1d6e2083-6212-429f-8599-109454eaef84a586833a-32b3-43c9-ac61-4d3703c816e8",
        "total_boosters": "2f9ba834-59ea-4898-b916-0da3c7394d38fe0b539e-b42c-4f0f-a390-e36c62750b86",
    }
    date_entity: str = "01ff1d02-e027-4eee-9de1-5e19f7fdd5e8"

    def read(self) -> pd.Series:
        """Reada data from source"""
        soup = get_soup(self.source_url)
        json_data = self._get_json_data(soup)
        data = self._parse_data(json_data)
        return pd.Series(data)

    def _print_entitiy_ids(self, json_data):
        entities = json_data["elements"]["content"]["content"]["entities"]
        for k, v in entities.items():
            vv = v["props"]
            if "content" in vv:
                print(k, vv["content"]["blocks"][0]["text"])

    def _parse_data(self, json_data: dict) -> dict:
        """Parses data from JSON"""
        data = {**self._parse_metrics(json_data), "date": self._parse_date(json_data)}
        return data

    def _get_json_data(self, soup: BeautifulSoup) -> dict:
        """Gets JSON from Soup"""
        for script in soup.find_all("script"):
            if "infographicData" in str(script):
                json_data = str(script).replace("<script>window.infographicData=", "").replace(";</script>", "")
                json_data = json.loads(json_data)
                break
        return json_data

    def _parse_metrics(self, json_data: dict) -> dict:
        """Parses metrics from JSON"""
        data = {}
        for metric, entity in self.metric_entities.items():
            value = json_data["elements"]["content"]["content"]["entities"][entity]["props"]["content"]["blocks"][0][
                "text"
            ]
            value = clean_count(value)
            data[metric] = value
        return data

    def _parse_date(self, json_data: dict) -> str:
        """Parses date from JSON"""
        value = json_data["elements"]["content"]["content"]["entities"][self.date_entity]["props"]["content"][
            "blocks"
        ][0]["text"]
        date = clean_date(value.lower(), "as of %B %d, %Y")
        return date

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        """Pipes location"""
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        """Pipes vaccine names"""
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac, Sputnik Light,"
            " Sputnik V",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        """Pipes source url"""
        return enrich_data(
            ds,
            "source_url",
            self.source_url_ref,
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for data"""
        df = ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)
        df = add_latest_who_values(df, "Philippines", ["people_vaccinated"])
        return df

    def export(self):
        """Exports data to CSV"""
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, attach=True)


def main():
    Philippines().export()


if __name__ == "__main__":
    main()
