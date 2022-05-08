from datetime import datetime, timedelta

import pandas as pd

from cowidev.utils.clean.dates import DATE_FORMAT
from cowidev.utils.utils import check_known_columns
from cowidev.utils.web import request_json
from cowidev.utils.web.download import read_csv_from_url
from cowidev.vax.utils.base import CountryVaxBase
from cowidev.vax.utils.utils import build_vaccine_timeline


class Canada(CountryVaxBase):
    location: str = "Canada"
    source_name: str = "Public Health Agency of Canada"
    source_url: str = "https://api.covid19tracker.ca/reports"
    source_url_a: str = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-coverage-byAgeAndSex-overTimeDownload.csv"
    source_url_m: str = "https://health-infobase.canada.ca/src/data/covidLive/vaccination-administration-bydosenumber2.csv"
    source_url_ref: str = "https://covid19tracker.ca/vaccinationtracker.html"
    source_url_age: str = "https://health-infobase.canada.ca/covid-19/vaccination-coverage/"
    source_url_man: str = "https://health-infobase.canada.ca/covid-19/vaccine-administration/"
    age_pattern: str = r"0?(\d{1,2})(?:–(\d{1,2})|\+)"
    vaccine_mapping: dict = {
        "AstraZeneca Vaxzevria/COVISHIELD": "Oxford/AstraZeneca",
        "Janssen": "Johnson&Johnson",
        "Moderna Spikevax": "Moderna",
        "Novavax": "Novavax",
        "Pfizer-BioNTech Comirnaty": "Pfizer/BioNTech",
        "Pfizer-BioNTech Comirnaty pediatric 5-11 years": "Pfizer/BioNTech",
    }

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        df = pd.DataFrame.from_records(data["data"])
        check_known_columns(
            df,
            [
                "date",
                "change_cases",
                "change_fatalities",
                "change_tests",
                "change_hospitalizations",
                "change_criticals",
                "change_recoveries",
                "change_vaccinations",
                "change_vaccinated",
                "change_boosters_1",
                "change_boosters_2",
                "change_vaccines_distributed",
                "total_cases",
                "total_fatalities",
                "total_tests",
                "total_hospitalizations",
                "total_criticals",
                "total_recoveries",
                "total_vaccinations",
                "total_vaccinated",
                "total_boosters_1",
                "total_boosters_2",
                "total_vaccines_distributed",
            ],
        )
        return df[["date", "total_vaccinations", "total_vaccinated", "total_boosters_1", "total_boosters_2"]]

    def read_age(self) -> pd.DataFrame:
        df = read_csv_from_url(
            self.source_url_a,
            verify=False,
            usecols=[
                "pruid",
                "week_end",
                "sex",
                "age",
                "numtotal_atleast1dose",
                "numtotal_fully",
                "numtotal_additional",
            ],
        )
        df = df[(df.pruid == 1) & (df.sex == "All sexes") & df.age.str.match(self.age_pattern)]
        metrics = ["people_vaccinated", "people_fully_vaccinated", "people_with_booster"]
        df = df.rename(
            columns={
                "week_end": "date",
                "numtotal_atleast1dose": metrics[0],
                "numtotal_fully": metrics[1],
                "numtotal_additional": metrics[2],
            }
        )
        df[metrics] = df[metrics].astype("float").fillna(0)
        # Parse age groups
        df[["age_group_min", "age_group_max"]] = df.age.str.extract(self.age_pattern).fillna("")
        df = df.drop(columns=["pruid", "sex", "age"])
        return df.pipe(self.pipe_age_per_capita).assign(location=self.location)

    def read_man(self) -> pd.DataFrame:
        df = read_csv_from_url(
            self.source_url_m,
            verify=False,
            usecols=[
                "week_end",
                "pruid",
                "product_name",
                "numtotal_dose1_admin",
                "numtotal_dose2_admin",
                "numtotal_dose3_admin",
                "numtotal_dose4+_admin",
                "numtotal_doseNotReported_admin",
            ],
        )
        df = df[df.pruid == 1].fillna(0)
        df = df.rename(columns={"week_end": "date", "product_name": "vaccine"})
        df["total_vaccinations"] = df[df.filter(like="numtotal_dose").columns].sum(axis=1)
        df = df[["date", "vaccine", "total_vaccinations"]]
        # Map vaccine names
        df = df[df.vaccine.isin(self.vaccine_mapping.keys()) & (df.total_vaccinations > 0)]
        assert set(df["vaccine"].unique()) == set(self.vaccine_mapping.keys())
        df = df.replace(self.vaccine_mapping).groupby(["date", "vaccine"], as_index=False).sum()
        # Create vaccine timeline
        self.vaccine_timeline = df[["date", "vaccine"]].groupby("vaccine").min().to_dict()["date"]
        self.vaccine_timeline["Pfizer/BioNTech"] = "2020-12-14" # Vaccination start date
        return df.assign(location=self.location)

    def pipe_filter_rows(self, df: pd.DataFrame):
        # Only records since vaccination campaign started
        return df[df.total_vaccinations > 0]

    def pipe_rename_columns(self, df: pd.DataFrame):
        return df.rename(
            columns={
                "total_vaccinated": "people_fully_vaccinated",
            }
        )

    def pipe_metrics(self, df: pd.DataFrame):
        total_boosters = df.total_boosters_1 + df.total_boosters_2.fillna(0)
        df = df.assign(
            people_vaccinated=(df.total_vaccinations - df.people_fully_vaccinated - total_boosters.fillna(0)),
            total_boosters=total_boosters,
        )
        # Booster data was not recorded for these dates, hence estimations on people vaccinated will not be accurate
        # df.loc[(df.date >= "2021-10-04") & (df.date <= "2021-10-09"), "people_vaccinated"] = pd.NA
        return df

    def pipe_filter_lastdates(self, df: pd.DataFrame):
        # date = "2022-03-18"
        last_date = datetime.strptime(df.date.max(), DATE_FORMAT)
        margin_days = 1
        remove_dates = [(last_date - timedelta(days=d)).strftime(DATE_FORMAT) for d in range(margin_days + 1)]
        df = df[~(df.date.isin(remove_dates))]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_metrics)
            .pipe(build_vaccine_timeline, self.vaccine_timeline)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_filter_lastdates)
            .pipe(self.make_monotonic)
            .sort_values("date")[
                [
                    "location",
                    "date",
                    "vaccine",
                    "source_url",
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                    "total_boosters",
                ]
            ]
        )
        return df

    def export(self):
        df = self.read()
        df_age = self.read_age()
        df_man = self.read_man()
        self.export_datafile(
            df.pipe(self.pipeline),
            df_age=df_age,
            df_manufacturer=df_man,
            meta_age={"source_name": self.source_name, "source_url": self.source_url_age},
            meta_manufacturer={"source_name": self.source_name, "source_url": self.source_url_man},
        )


def main():
    Canada().export()
