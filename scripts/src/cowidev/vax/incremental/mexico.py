import tempfile
import re

import pandas as pd
from pdfminer.high_level import extract_text

from cowidev.utils import clean_count, clean_date, get_soup
from cowidev.utils.web.download import download_file_from_url
from cowidev.vax.utils.incremental import enrich_data
from cowidev.vax.utils.base import CountryVaxBase


class Mexico(CountryVaxBase):
    location: str = "Mexico"
    source_page: str = "https://www.gob.mx/salud/documentos/presentaciones-2022"
    regex = {
        "date": r"(\d{1,2} \w+\, 20\d{2})",
        "total_vaccinations": r"(?:COVID-19 )?(\d+) Total de dosis aplicadas reportadas",
        "people_vaccinated": r"(?:Nuevos esquemas )?(\d+) Personas vacunadas reportadas",
        "people_fully_vaccinated": r"(\d+) Personas vacunadas con esq\. completo",
        "total_boosters": r"(\d+) Total de refuerzos aplicados",
    }

    def read(self):
        """Read the data from the source"""
        soup = get_soup(self.source_page)
        link = self._parse_link_pdf(soup)
        return self._parse_data(link)

    def _parse_link_pdf(self, soup) -> list:
        """Parse the link to the pdf"""
        link = soup.find(class_="list-unstyled").find("a")["href"]
        link = "http://www.gob.mx" + link
        self.source_url = link
        # print(link)
        return link

    def _get_text_from_pdf(self, url: str) -> str:
        """Get the text from the pdf url"""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url, tmp.name)
            with open(tmp.name, "rb") as f:
                text = extract_text(f)
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"(\d)\,(\d)", r"\1\2", text)
        return text

    def _parse_data(self, url: str) -> pd.Series:
        """Parse the data from the pdf url"""
        text = self._get_text_from_pdf(url)
        data = {
            "total_vaccinations": clean_count(re.search(self.regex["total_vaccinations"], text).group(1)),
            "people_vaccinated": clean_count(re.search(self.regex["people_vaccinated"], text).group(1)),
            "people_fully_vaccinated": (
                clean_count(match.group(1))
                if (match := re.search(self.regex["people_fully_vaccinated"], text))
                else None
            ),
            "total_boosters": clean_count(re.search(self.regex["total_boosters"], text).group(1)),
            "date": clean_date(re.search(self.regex["date"], text).group(1), "%d %B, %Y", lang="es"),
        }
        self._check_data(data)
        return pd.Series(data)

    def _check_data(self, data):
        assert data["total_vaccinations"] >= 94300526
        assert data["people_vaccinated"] >= 61616895
        assert data["people_vaccinated"] <= data["total_vaccinations"]
        if data["people_fully_vaccinated"] is not None:
            assert data["people_fully_vaccinated"] >= 41115211
            assert data["people_fully_vaccinated"] >= 0.5 * data["people_vaccinated"]

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        """Pipe location to the data"""
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        """Pipe vaccine to the data"""
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Oxford/AstraZeneca, Moderna, Pfizer/BioNTech, Sinovac, Sputnik V, CanSino",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        """Pipe source url to the data"""
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        """Pipeline for the data"""
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        """Export the data to a csv"""
        data = self.read().pipe(self.pipeline)
        self.export_datafile(data, attach=True)


def main():
    Mexico().export()
