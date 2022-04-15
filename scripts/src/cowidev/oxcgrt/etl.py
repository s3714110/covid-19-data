import pandas as pd


class OxCGRTETL:
    def __init__(self) -> None:
        self.source_url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"
        self.source_url_diff = "https://raw.githubusercontent.com/OxCGRT/covid-policy-scratchpad/master/differentiated_vaccination_policies/OxCGRT_differentiated.csv"

    def extract(self):
        df = pd.read_csv(self.source_url, low_memory=False)
        df_diff = pd.read_csv(self.source_url_diff, low_memory=False)
        return df, df_diff

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def load(self, df: pd.DataFrame, output_path: str):
        df.to_csv(output_path, index=False)

    def run(self, output_path: str, output_path_diff: str):
        df, df_diff = self.extract()
        self.load(df, output_path)
        self.load(df_diff, output_path_diff)


def run_etl(output_path: str, output_path_diff: str):
    etl = OxCGRTETL()
    etl.run(output_path, output_path_diff)
