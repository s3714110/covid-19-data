import pandas as pd

from cowidev.utils import clean_date, clean_date_series
from cowidev.utils.utils import check_known_columns
from cowidev.utils.web.download import read_csv_from_url
from cowidev.vax.utils.base import CountryVaxBase
from cowidev.vax.utils.utils import build_vaccine_timeline


class Australia(CountryVaxBase):
    source_url = {
        "main": "https://covidbaseau.com/people-vaccinated.csv",
        "age_1d": "https://covidbaseau.com/historical/Vaccinations%20By%20Age%20Group%20and%20State%20First.csv",
        "age_2d": "https://covidbaseau.com/historical/Vaccinations%20By%20Age%20Group%20and%20State%20Second.csv",
    }
    source_url_ref = "https://covidbaseau.com/"
    source_file = "https://covidbaseau.com/people-vaccinated.csv"
    location = "Australia"
    columns_rename = {
        "dose_1": "people_vaccinated",
        "dose_2": "people_fully_vaccinated",
        "dose_3": "total_boosters",
    }
    vaccine_timeline = {
        "Pfizer/BioNTech": "2021-01-01",
        "Moderna": "2021-03-06",
        "Oxford/AstraZeneca": "2021-03-06",
        "Novavax": "2022-02-17",
    }

    def read(self) -> pd.DataFrame:
        df = read_csv_from_url(self.source_url["main"])
        check_known_columns(df, ["date", "dose_1", "dose_2", "dose_3"])
        return df

    def read_age(self) -> pd.DataFrame:
        df_1 = read_csv_from_url(self.source_url["age_1d"], header=1).dropna(axis=1, how="all")
        df_1 = df_1.melt("Date", var_name="age_group", value_name="people_vaccinated_per_hundred")
        df_2 = read_csv_from_url(self.source_url["age_2d"], header=1).dropna(axis=1, how="all")
        df_2 = df_2.melt("Date", var_name="age_group", value_name="people_fully_vaccinated_per_hundred")
        df = df_1.merge(df_2, on=["Date", "age_group"], how="left")
        return df

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.dose_1 + df.dose_2 + df.dose_3)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(date=df.date.apply(clean_date, fmt="%Y-%m-%d", minus_days=1))
        return df

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(build_vaccine_timeline, self.vaccine_timeline)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_filter_dp, ["2022-10-06", "2022-10-13", "2022-10-20"])
            .pipe(self.make_monotonic)
            .sort_values("date")
        )

    def pipe_age_groups(self, df):
        regex = r"(\d{1,2})+?(?:-(\d{1,2}))?"
        df[["age_group_min", "age_group_max"]] = df.age_group.str.extract(regex)
        return df

    def pipe_age_numeric(self, df):
        regex = r"([\d\.]+).*"
        metrics = ["people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred"]
        for metric in metrics:
            df.loc[:, metric] = df[metric].str.extract(regex, expand=False).astype(float)
        return df

    def pipe_age_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            date=clean_date_series(df.Date),
            location=self.location,
        )

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_age_groups)
            .pipe(self.pipe_age_numeric)
            .pipe(self.pipe_age_metadata)
            # .dropna(subset=["people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred"], how="all")
            .drop_duplicates(subset=["people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred"])
            .sort_values(["date", "age_group_min"])[
                [
                    "location",
                    "date",
                    "age_group_min",
                    "age_group_max",
                    "people_vaccinated_per_hundred",
                    "people_fully_vaccinated_per_hundred",
                ]
            ]
        )

    def export(self):
        # Main
        df = self.read().pipe(self.pipeline)
        # Age
        df_age = self.read_age().pipe(self.pipeline_age)
        self.export_datafile(
            df=df,
            df_age=df_age,
            meta_age={"source_name": "Ministry of Health via covidbaseau.com", "source_url": self.source_url_ref},
        )


def main():
    Australia().export()
