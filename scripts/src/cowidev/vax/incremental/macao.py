import time

import tabula

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web import get_driver
from cowidev.vax.utils.incremental import increment


class Macao:
    source_url = "https://www.ssm.gov.mo/apps1/covid19vaccine/en.aspx"
    location = "Macao"

    def read(self):
        """Create data."""
        with get_driver() as driver:
            driver.get(self.source_url)
            time.sleep(5)
            # Get element
            elem = driver.find_element_by_partial_link_text("Daily Bulletin on COVID-19")
            # Build data
            data = self._parse_data(elem)
            return data

    def _parse_pdf_table(self, url):
        """Extract table"""
        dfs = tabula.read_pdf(url)
        df = dfs[0]
        # Checks data
        cols = ["Unnamed: 0", "Unnamed: 1", "滅活疫苗", "Unnamed: 2", "其他種類疫苗", "Unnamed: 3", "Unnamed: 4"]
        if df.shape[1] != 7:
            raise ValueError("New columns added!")
        if not (df.columns == cols).all():
            raise ValueError("Source data columns changed!")
        df = df.set_index("Unnamed: 0")
        return df

    def _parse_date(self, element):
        """Get data from report file title."""
        r = r".* \(Last updated: (\d\d\/\d\d\/20\d\d) .*\)"
        return extract_clean_date(element.text, r, "%d/%m/%Y")

    def _parse_data(self, element):
        # Obtain pdf url
        url = element.get_property("href")
        # Obtain date from element
        date = self._parse_date(element)
        # Extract table data
        df = self._parse_pdf_table(url)
        # try:
        total_vaccinations = clean_count(df.loc["Total de doses administradas", "Unnamed: 4"])
        people_vaccinated = clean_count(df.loc["N o Pessoas inoculadas com pelo menos", "Unnamed: 4"])
        people_only_2_doses = clean_count(df.loc["N.o de pessoas vacinadas com a 2a dose", "Unnamed: 4"])
        people_only_3_doses = clean_count(df.loc["N.o de pessoas vacinadas com a 3a dose", "Unnamed: 4"])
        # except Exception as e:
        #     print(e)
        #     print(df.index)

        data = {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_only_2_doses + people_only_3_doses,
            "total_boosters": people_only_3_doses,
            "source_url": url,
            "date": date,
        }
        return data

    def export(self):
        data = self.read()
        increment(
            location=self.location,
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine="Pfizer/BioNTech, Sinopharm/Beijing",
        )


def main():
    Macao().export()
