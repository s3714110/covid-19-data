from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from cowidev.utils.clean import clean_date_series, clean_df_columns_multiindex
from cowidev.utils.web.utils import to_proxy_url
from cowidev.utils.web.scraping import get_response
from cowidev.vax.utils.base import CountryVaxBase
from cowidev.vax.utils.checks import validate_vaccines
from cowidev.vax.utils.utils import build_vaccine_timeline


class Japan(CountryVaxBase):
    location: str = "Japan"
    source_name: str = "Prime Minister's Office"
    source_url_early: str = "https://www.mhlw.go.jp/stf/seisakunitsuite/bunya/vaccine_sesshujisseki.html"
    source_url: str = "https://www.kantei.go.jp/jp/content/vaccination_data5.xlsx"
    source_url_bst: str = "https://www.kantei.go.jp/jp/content/booster_data.xlsx"
    source_url_bst2: str = "https://www.kantei.go.jp/jp/content/booster2nd_data.xlsx"
    source_url_ref: str = "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"
    cols_early: dict = {
        "日付": "date",
        "内１回目": "dose1",
        "内２回目": "dose2",
    }
    age_groups: dict = {"all": ["すべて"], "65-": ["うち高齢者"], "5-11": ["うち小児接種"]}
    age_groups_bst: dict = {"all": ["すべて"], "65-": ["うち高齢者"], "5-11": ["うち小児接種"]}
    age_group_remain: str = "12-64"
    sheets: dict = {
        "総接種回数": None,
        "初回接種_一般接種": {"name": "general", "header": [2, 3, 4], "date": "接種日", "ind": age_groups},
        "初回接種_医療従事者等": {"name": "healthcare", "header": [2, 3], "date": "集計日", "ind": []},
        "初回接種_職域接種": {"name": "workplace", "header": [2, 3], "date": "集計日", "ind": []},
        "初回接種_重複": {"name": "overlap", "header": [2, 3], "date": "公表日", "ind": []},
    }
    sheets_bst: dict = {
        "総接種回数": None,
        "一般接種": {"name": "general", "header": [1, 2], "date": "接種日", "ind": age_groups_bst},
        "職域接種": {"name": "workplace", "header": [2, 3], "date": "集計日", "ind": ["接種回数"]},
        "重複": {"name": "overlap", "header": [2, 3], "date": "公表日", "ind": ["接種回数"]},
    }
    sheets_bst2: dict = {
        "総接種回数": None,
        "一般接種": {
            "name": "general",
            "header": [1, 2, 3, 4],
            "date": "接種日",
            "ind": ["曜日", "すべて", ""],
        },
    }
    metrics: dict = {"dose1": ["内1回目"], "dose2": ["内2回目"]}
    metrics_bst: dict = {"dose3": []}
    metrics_bst2: dict = {"dose4": []}
    metrics_age: dict = {
        "dose1": "people_vaccinated",
        "dose2": "people_fully_vaccinated",
        "dose3": "people_with_booster",
    }
    vaccine_mapping: dict = {
        "ファイザー社": "Pfizer/BioNTech",
        "モデルナ社": "Moderna",
        "アストラゼネカ社": "Oxford/AstraZeneca",
        "武田社(ノババックス)": "Novavax",
        "武田社\n(ノババックス)": "Novavax",
        "接種回数(合計)": None,
    }

    def read_early(self) -> pd.DataFrame:
        # Use get_response().content since get_response().text may get incorrect encoding
        soup = BeautifulSoup(get_response(self.source_url_early).content, "lxml")
        dfs = pd.read_html(str(soup), header=0)
        assert len(dfs) == 1, f"Only one table should be present. {len(dfs)} tables detected."
        return dfs[0]

    def pipe_early(self, df: pd.DataFrame) -> pd.DataFrame:
        # Filter columns & rows
        df = df[self.cols_early.keys()].rename(columns=self.cols_early)
        df = df[df.date != "合計"]
        return df.assign(
            date=clean_date_series(df.date),
            age_group="all",
            vaccine="Pfizer/BioNTech",
            source_url=self.source_url_early,
        ).sort_values("date")

    def read_latest(self) -> pd.DataFrame:
        dfs = []
        dfs.append(self._read_xlsx(self.source_url, self.sheets, self.metrics))
        dfs.append(self._read_xlsx(self.source_url_bst, self.sheets_bst, self.metrics_bst))
        dfs.append(self._read_xlsx(self.source_url_bst2, self.sheets_bst2, self.metrics_bst2))
        return pd.concat([df for dfs_ in dfs for name, df in dfs_.items() if name != "overlap"]).reset_index(drop=True)

    def _read_xlsx(self, url: str, sheets: dict, metrics: dict) -> dict:
        # Download and check Excel sheets
        # url = to_proxy_url(url)
        # print(url, url_proxy)
        xlsx = pd.ExcelFile(url)
        sheets_unknown = set(xlsx.sheet_names) - set(sheets)
        if sheets_unknown:
            raise ValueError(f"Unknown sheets: {sheets_unknown}")
        dfs = {}
        for sheet, sets in sheets.items():
            if sets:
                # Parse Excel sheets with predefined settings
                df = xlsx.parse(sheet_name=sheet, header=sets["header"])
                if isinstance(sets["ind"], dict):
                    # Parse Excel sheets with age groups
                    dfs_ = []
                    for age_group, ind in sets["ind"].items():
                        df_ = self._parse_df(df, sets["date"], ind, metrics)
                        dfs_.append(df_.assign(age_group=age_group))
                    dfs[sets["name"]] = pd.concat(dfs_).reset_index(drop=True)
                else:
                    dfs[sets["name"]] = self._parse_df(df, sets["date"], sets["ind"], metrics)
                    dfs[sets["name"]]["age_group"] = "all"
        if isinstance(dfs.get("workplace"), pd.DataFrame):
            # Estimate daily metrics for workplace data based on the latest overlap data
            for metric in metrics.keys():
                workplace_latest = dfs["workplace"][metric].iat[-1]
                overlap_latest = dfs["overlap"][metric].iat[-1]
                dfs["workplace"][metric] -= dfs["workplace"][metric].shift(1, fill_value=0)
                dfs["workplace"][metric] *= 1 - overlap_latest / workplace_latest
        return dfs

    def _parse_df(self, df: pd.DataFrame, date_col: str, ind: list, metrics: dict) -> pd.DataFrame:
        # Clean columns
        df = clean_df_columns_multiindex(df.dropna(axis=1, how="all"))
        df = df.loc[:, ~df.columns.duplicated()]
        # Filter and clean metric rows
        df = df[df[date_col].apply(isinstance, args=(datetime,)) & df[date_col].notna()]
        df[date_col] = clean_date_series(df[date_col])
        df = df.set_index(date_col).sort_index()
        dfs = []
        for metric, index in metrics.items():
            # Get, check, convert and rename metric columns
            df_ = df[tuple(ind + index)].stack().reset_index()
            cols_unknown = set(df_.columns) - {date_col, "level_1", 0}
            if cols_unknown:
                raise ValueError(f"Unknown columns: {cols_unknown}")
            df_[0] = pd.to_numeric(df_[0], errors="coerce")
            dfs.append(df_.rename(columns={date_col: "date", "level_1": "vaccine", 0: metric}))
        while len(dfs) > 1:
            dfs[0] = dfs[0].merge(dfs.pop(1), on=["date", "vaccine"])
        return dfs[0]

    def pipe_latest(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check and map vaccine names
        validate_vaccines(df, self.vaccine_mapping)
        df = df.fillna(0).replace(self.vaccine_mapping).dropna()
        # Aggregate metrics by the same date & age group & vaccine
        df = df.groupby(["date", "age_group", "vaccine"], as_index=False).sum()
        return df.assign(source_url=self.source_url_ref)

    def read(self) -> pd.DataFrame:
        df_early = self.read_early().pipe(self.pipe_early)
        df_latest = self.read_latest().pipe(self.pipe_latest)
        return pd.concat([df_early, df_latest]).reset_index(drop=True)

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get cumulative metrics
        metrics = df.filter(like="dose").columns
        df[metrics] = df.fillna(0).groupby(["age_group", "vaccine"])[metrics].cumsum().round()
        df["total_vaccinations"] = df[metrics].sum(axis=1)
        return df[df.total_vaccinations > 0].assign(location=self.location)

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        metrics = list(self.metrics_age.keys())
        df = df.groupby(["date", "age_group", "location"], as_index=False)[metrics].sum()
        # Get the remaining age group
        df_all = df[df.age_group == "all"].set_index("date")
        df_groups = df[df.age_group != "all"]
        df_groups_sum = df_groups.groupby("date").sum()
        df_all[metrics] = df_all[metrics].sub(df_groups_sum[metrics], fill_value=0)
        df_remain = df_all.reset_index().assign(age_group=self.age_group_remain)
        df = pd.concat([df_groups, df_remain]).reset_index(drop=True)
        # Parse age groups, rename metric columns and calculate per capita metrics
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split("-", expand=True)
        return df.rename(columns=self.metrics_age).pipe(self.pipe_age_per_capita)

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df.age_group == "all"][["location", "date", "vaccine", "total_vaccinations"]]

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df["total_boosters"] = df.dose3 + df.dose4
        return df.rename(columns={"dose1": "people_vaccinated", "dose2": "people_fully_vaccinated"})

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df.age_group == "all"]
        vaccine_timeline = df[["date", "vaccine"]].groupby("vaccine").min().date.to_dict()
        df = df.groupby(["date", "location", "source_url"], as_index=False).agg(
            {
                "total_vaccinations": "sum",
                "people_vaccinated": "sum",
                "people_fully_vaccinated": "sum",
                "total_boosters": "sum",
            }
        )
        return df.pipe(build_vaccine_timeline, vaccine_timeline)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_metrics)
            .pipe(self.pipe_aggregate)
            .pipe(self.make_monotonic)[
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

    def export(self):
        # Read and preprocess
        df = self.read().pipe(self.pipeline_base)
        # Transform
        df_age = df.pipe(self.pipeline_age)
        df_man = df.pipe(self.pipeline_manufacturer)
        df = df.pipe(self.pipeline)
        # Export
        self.export_datafile(
            df=df,
            df_age=df_age,
            df_manufacturer=df_man,
            meta_age={"source_name": self.source_name, "source_url": self.source_url_ref},
            meta_manufacturer={"source_name": self.source_name, "source_url": self.source_url_ref},
        )


def main():
    Japan().export()
