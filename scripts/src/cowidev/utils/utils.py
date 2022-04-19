import traceback
from datetime import datetime, timedelta
import json
import ntpath
import os
import pytz
import tempfile
import warnings

from xlsx2csv import Xlsx2csv
import pandas as pd

from cowidev import PATHS
from cowidev.utils.web.download import download_file_from_url


def make_monotonic(
    df: pd.DataFrame, column_date: str, column_metrics: list, max_removed_rows=10, strict=False
) -> pd.DataFrame:
    # Forces vaccination time series to become monotonic.
    # The algorithm assumes that the most recent values are the correct ones,
    # and therefore removes previous higher values.
    n_rows_before = len(df)
    dates_before = set(df.date)
    df_before = df.copy()

    df = df.sort_values(column_date)
    for metric in column_metrics:
        while not df[metric].ffill().fillna(0).is_monotonic:
            diff = df[metric].ffill().shift(-1) - df[metric].ffill()
            if strict:
                df = df[(diff > 0) | (diff.isna())]
            else:
                df = df[(diff >= 0) | (diff.isna())]
    dates_now = set(df.date)

    if max_removed_rows is not None:
        num_removed_rows = n_rows_before - len(df)
        if num_removed_rows > max_removed_rows:
            dates_wrong = dates_before.difference(dates_now)
            df_wrong = df_before[df_before.date.isin(dates_wrong)]
            # pd.set_option("expand_frame_repr", False)
            df_wrong = df_wrong[["date"] + column_metrics]
            # df_wrong = df_before[["date"] + column_metrics]
            raise Exception(
                f"{num_removed_rows} rowse have been removed. That is more than maximum allowed ({max_removed_rows})"
                f" by make_monotonic() - check the data. Check \n{df_wrong}"  # {', '.join(sorted(dates_wrong))}"
            )

    return df


def series_monotonic(ds):
    diff = ds.ffill().shift(-1) - ds.ffill()
    return ds[(diff >= 0) | (diff.isna())]


def export_timestamp(timestamp_filename: str, force_directory: str = None):
    if force_directory:
        timestamp_filename = os.path.join(force_directory, timestamp_filename)
    else:
        timestamp_filename = os.path.join(PATHS.DATA_TIMESTAMP_DIR, timestamp_filename)
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())


def time_str_grapher():
    return (datetime.now() - timedelta(minutes=10)).astimezone(pytz.timezone("Europe/London")).strftime("%-d %B %Y")


def get_filename(filepath: str, remove_extension: bool = True):
    filename = ntpath.basename(filepath)
    if remove_extension:
        return filename.split(".")[0]
    return filename


def xlsx2csv(filename_xlsx: str, filename_csv: str):
    if filename_xlsx.startswith("https://") or filename_xlsx.startswith("http://"):
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(filename_xlsx, tmp.name)
            Xlsx2csv(tmp.name, outputencoding="utf-8").convert(filename_csv)
    else:
        Xlsx2csv(filename_xlsx, outputencoding="utf-8").convert(filename_csv)


def pd_series_diff_values(a, b):
    common = set(a) & set(b)
    return {*set(a[-a.isin(common)]), *set(b[-b.isin(common)])}


def dict_to_compact_json(d: dict):
    """
    Encodes a Python dict into valid, minified JSON.
    """
    return json.dumps(
        d,
        # Use separators without any trailing whitespace to minimize file size.
        # The defaults (", ", ": ") contain a trailing space.
        separators=(",", ":"),
        # The json library by default encodes NaNs in JSON, but this is invalid JSON.
        # By having this False, an error will be thrown if a NaN exists in the data.
        allow_nan=False,
    )


def check_known_columns(df: pd.DataFrame, known_cols: list) -> None:
    unknown_cols = set(df.columns).difference(set(known_cols))
    if len(unknown_cols) > 0:
        raise Exception(f"Unknown column(s) found: {unknown_cols}")


def get_traceback(e):
    lines = traceback.format_exception(type(e), e, e.__traceback__)
    return "".join(lines)
