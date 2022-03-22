import re
import tempfile

from bs4 import BeautifulSoup, element
import pandas as pd
from pdfminer.high_level import extract_text

from cowidev.utils.web import get_soup
from cowidev.utils.web.download import download_file_from_url
from cowidev.utils.clean import clean_count, clean_date
from cowidev.testing.utils.incremental import increment


class Romania:
    location = "Romania"
    units = "tests performed"
    source_label = "Ministry of Internal Affairs"
    source_url = "https://gov.ro/ro/comunicate&page="
    _num_max_pages = 3
    regex = {
        "title": r"BULETIN DE PRESĂ",
        "date": r"(\d{1,2} \w+ \d{4})",
        "pcr": r"au fost prelucrate (\d+)",
        "art": r"teste RT-PCR și (\d+)",
    }

    def read(self) -> pd.Series:
        """Read data from source."""
        data = []
        for cnt in range(1, self._num_max_pages + 1):
            url = f"{self.source_url}{cnt}"
            soup = get_soup(url)
            data, proceed = self._parse_data(soup)
            if not proceed:
                break

        return pd.Series(data)

    def _parse_data(self, soup: BeautifulSoup) -> tuple:
        """Get data from the source page."""
        # Get relevant element
        elem = self._get_relevant_element(soup)
        if not elem:
            return None, True
        # Extract url and date from element
        url, date = self._get_link_and_date_from_element(elem)
        # Extract pdf link from url
        pdf_url = self._get_pdf_link_from_url(url)
        # Get text from pdf link
        text = self._parse_pdf_link(pdf_url)
        # Get metrics from text
        count = self._parse_metrics(text)
        record = {
            "source_url": pdf_url,
            "date": date,
            "count": count,
        }
        return record, False

    def _get_relevant_element(self, soup: BeautifulSoup) -> element.Tag:
        """Get the relevant element in news feed."""
        news_list = soup.find_all(text=re.compile(self.regex["title"]))
        if not news_list:
            return None
        return news_list[0].parent

    def _get_link_and_date_from_element(self, elem: element.Tag) -> tuple:
        """Extract link and date from relevant element."""
        link = self._parse_link_from_element(elem)
        date = self._parse_date_from_element(elem)
        return link, date

    def _parse_date_from_element(self, elem: element.Tag) -> str:
        """Get date from relevant element."""
        date = re.search(self.regex["date"], elem.text).group(1)
        return clean_date(date, "%d %B %Y", lang="ro")

    def _parse_link_from_element(self, elem: element.Tag) -> str:
        """Get link from relevant element."""
        link = elem["href"]
        return link

    def _get_pdf_link_from_url(self, url: str) -> str:
        """Extract text from the url."""
        soup = get_soup(url)
        url = soup.find("p", text=re.compile(self.regex["title"]))
        url = url.a["href"]
        return url

    def _parse_pdf_link(self, url: str) -> str:
        """Get text from the pdf link."""
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(url, tmp.name)
            with open(tmp.name, "rb") as f:
                text = extract_text(f)
        text = re.sub(r"(\d)\.(\d)", r"\1\2", text)
        return text

    def _parse_metrics(self, text: str) -> int:
        """Get metrics from news text."""
        pcr = int(re.search(self.regex["pcr"], text).group(1))
        art = int(re.search(self.regex["art"], text).group(1))
        return clean_count(pcr + art)

    def export(self):
        """Export data to csv."""
        data = self.read()
        increment(
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=data["source_url"],
            source_label=self.source_label,
            count=data["count"],
        )


def main():
    Romania().export()
