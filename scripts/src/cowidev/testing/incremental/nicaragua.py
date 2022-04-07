import re

from bs4 import BeautifulSoup
import pandas as pd
import requests
import tempfile
from pdfminer.high_level import extract_text
from typing import Iterator
from epiweeks import Week


from cowidev.utils import get_soup, clean_count, clean_date
from cowidev.utils.clean import extract_clean_date
from cowidev.utils.utils import download_file_from_url
from cowidev.testing import CountryTestBase


class Nicaragua(CountryTestBase):
    location: str = "Nicaragua"
    units: str = "tests performed"
    source_label: str = "Ministry of Health"
    source_url: str = "http://www.minsa.gob.ni/index.php/repository/Descargas-MINSA/COVID-19/Boletines-Epidemiol%C3%B3gico/Boletines-2022/"
    _base_url: str = "http://www.minsa.gob.ni/index.php/repository/func-download"
    source_url_ref: str = None
    regex: dict = {
        "title": r"Boletín Epidemiológico de la Semana No. ",
    }

    def read(self) -> pd.DataFrame:
        """Read data from source"""
        soup = get_soup(self.source_url)
        df = self._parse_data(soup)
        return df

    def _parse_data(self, soup: BeautifulSoup) -> pd.DataFrame:
        """Parse data from soup"""
        # Get the download URL
        link = self._get_download_url(soup)
        # Parse count from pdf
        count = self._extract_text_from_url(link)
        # Get the date from week num
        date = self._parse_date(link)

    def _get_download_url(self, soup: BeautifulSoup) -> str:
        source_url_ref = soup.find("a", text=re.compile(self.regex["title"]))["href"]
        if not source_url_ref:
            raise ValueError("Article not found, please update the script")
        response = requests.get(self.source_url_ref, allow_redirects=True)
        text = response.content.decode("utf-8")
        result = re.search("func-download(.*)'}", text).group(1)
        link = f"{self._base_url}{result}"
        return link

    def _extract_text_from_url(self) -> str:
        """Extracts text from pdf."""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(self.link, tmp.name)
            with open(tmp.name, "rb") as f:
                text = extract_text(f).replace("\n", " ")
        count = clean_count(re.search("• Acumulado: (.*)  Recuperados", text).group(1))
        if not count:
            raise ValueError("Count not found, please update the script")
        return count

    def _parse_date(self, week_num) -> Iterator:
        """parses the date from the week number."""
        week_num = int(
            re.search(
                "Boletín Epidemiológico de la Semana No. (.*)",
                soup.find("a", text=re.compile(self.regex["title"])).text,
            ).group(1)
        )
        date = Week(2022, week_num, system="iso").enddate()
        return clean_date(date)
