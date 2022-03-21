import requests

import pandas as pd

from cowidev.utils import clean_date
from cowidev.utils.clean.dates import clean_date_series, localdate
from cowidev.vax.utils.base import CountryVaxBase
from cowidev.vax.utils.incremental import enrich_data


class Argentina(CountryVaxBase):
    location = "Argentina"
    source_url_ref = "https://www.argentina.gob.ar/coronavirus/vacuna/aplicadas"
    source_url = "https://coronavirus.msal.gov.ar/vacunas/d/8wdHBOsMk/seguimiento-vacunacion-covid/api/datasources/proxy/1/query"
    source_url_age = "https://covidstats.com.ar/ws/vacunadosargentina?porgrupoetario=1"
    source_url_ref_2 = "https://covidstats.com.ar/ws/vacunadosargentina"
    age_group_valid = {
        "30-39",
        "80-89",
        "18-29",
        "90-99",
        "50-59",
        "70-79",
        "60-69",
        ">=100",
        "40-49",
        "<12",
        "12-17",
    }

    def read(self) -> pd.DataFrame:

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:94.0) Gecko/20100101 Firefox/94.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://coronavirus.msal.gov.ar/vacunas/d/8wdHBOsMk/seguimiento-vacunacion-covid/d/8wdHBOsMk/seguimiento-vacunacion-covid?orgId=1&refresh=15m%3F",
            "content-type": "application/json",
            "x-grafana-org-id": "1",
            "Origin": "https://coronavirus.msal.gov.ar",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        query = (
            '{"app":"dashboard","requestId":"Q101","timezone":"","panelId":4,"dashboardId":3,"range":{"raw":{"from":"2020-12-29T03:00:00.000Z","to":"now"}},"timeInfo":"","interval":"6h","intervalMs":21600000,"targets":[{"data":null,"target":"distribucion_aplicacion_utilidad_provincia_tabla_publico","refId":"A","hide":false,"type":"table"}],"maxDataPoints":9999,"scopedVars":{"__from":{"text":"1609210800000","value":"1609210800000"},"__dashboard":{"value":{"name":"Seguimiento'
            " vacunaci\xf3n"
            ' Covid","uid":"8wdHBOsMk"}},"__org":{"value":{"name":"minsal","id":0}},"__interval":{"text":"6h","value":"6h"},"__interval_ms":{"text":"21600000","value":21600000}},"startTime":1637056461969,"rangeRaw":{"from":"2020-12-29T03:00:00.000Z","to":"now"},"adhocFilters":[]}'
        )

        json_data = requests.post(self.source_url, headers=headers, data=query).json()
        for row in json_data[0]["rows"]:
            if row[0] == "Totales":
                data = row
                break

        data = pd.Series(
            {
                "people_vaccinated": data[2],
                "people_fully_vaccinated": data[3],
                "total_vaccinations": data[7],
                "total_boosters": data[5] + data[6],
            }
        )
        assert data.total_vaccinations >= data.people_vaccinated >= data.people_fully_vaccinated
        assert data.total_vaccinations >= data.total_boosters

        return data

    def read_age(self):
        data = requests.get(self.source_url_age).json()
        data = list(data.values())[:-1]
        self._check_data_age(data)
        data = self._parse_data_age(data)
        return data

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "date", localdate("America/Argentina/Buenos_Aires"))

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipe_vaccines(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds, "vaccine", "CanSino, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V"
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccines)

    def _check_data_age(self, data):
        ages = {d["denominacion"] for d in data}
        age_wrong = ages.difference(self.age_group_valid | {"Otros (sin especificar)"})
        if age_wrong:
            raise ValueError(f"Unknown age group {age_wrong}")

    def _parse_data_age(self, data):
        # Merge
        dfs = [self._build_df_age_group(d) for d in data if d["denominacion"] in self.age_group_valid]
        df = pd.concat(dfs, ignore_index=True).assign(location=self.location)
        df[["age_group_min", "age_group_max"]] = df[["age_group_min", "age_group_max"]].astype(str)
        return df

    def _build_df_age_group(self, data):
        # Get metrics
        dose_3 = data["adicional"]
        booster = data["refuerzo"]
        total_boosters = [d + b for d, b in zip(dose_3, booster)]
        # Get dates
        n_days = len(dose_3)
        dt = clean_date(data["fecha_inicial"], "%Y-%m-%dT%H:%M:%S%z", as_datetime=False)
        dates = pd.date_range(dt, periods=n_days, freq="D")
        # Build df
        df = pd.DataFrame(
            {
                "date": dates,
                "people_vaccinated": data["dosis1"],
                "people_fully_vaccinated": data["esquemacompleto"],
                "people_with_booster": total_boosters,
            }
        ).assign(
            **{
                "age_group_min": data["desdeedad"],
                "age_group_max": data["hastaedad"] if data["hastaedad"] is not None else "",
                "age_group": data["denominacion"],
            }
        )
        return df

    def pipe_age_cumsum(self, df):
        # cumsum
        cols = ["people_vaccinated", "people_fully_vaccinated", "people_with_booster"]
        df[cols] = df.sort_values("date").groupby("age_group")[cols].cumsum()
        return df

    def pipe_age_date(self, df):
        return df.assign(date=clean_date_series(df.date))

    def pipeline_age(self, df):
        return df.pipe(self.pipe_age_cumsum).pipe(self.pipe_age_date).pipe(self.pipe_age_per_capita)

    def export(self):
        # Main data
        data = self.read().pipe(self.pipeline)
        # Age data
        df_age = self.read_age().pipe(self.pipeline_age)
        # Export
        self.export_datafile(
            data,
            df_age=df_age,
            meta_age={
                "source_name": "Ministry of Health via https://covidstats.com.ar",
                "source_url": self.source_url_ref_2,
            },
            attach=True,
        )


def main():
    Argentina().export()
